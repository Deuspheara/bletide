@TestOn('browser')
library;

import 'dart:async';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';

import 'package:bletide/bletide.dart';
import 'package:bletide/src/web/bluetooth.dart';
import 'package:bletide/src/web/web_backend.dart';
import 'package:test/test.dart';
import 'package:web/web.dart' as web;

extension type FakeBrowser._(JSObject _) implements JSObject {
  external factory FakeBrowser({JSFunction requestDevice});
}

extension type FakeDevice._(JSObject _) implements JSObject {
  external factory FakeDevice({String id, String? name});
}

TypeMatcher<BleException> error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);
void main() {
  late Completer<BrowserDevice> selection;
  late WebBleBackend backend;
  late Ble ble;
  late BrowserDeviceOptions requested;
  late int calls;
  setUp(() {
    selection = Completer<BrowserDevice>();
    calls = 0;
    final browser = FakeBrowser(
      requestDevice: ((BrowserDeviceOptions options) {
        calls++;
        requested = options;
        return selection.future.toJS;
      }).toJS,
    );
    backend = WebBleBackend(
      bluetooth: BrowserBluetooth(browser),
      secureContext: true,
    );
    ble = Ble(backend: backend);
  });
  tearDown(() async {
    await ble.close();
    if (!selection.isCompleted) {
      selection.complete(BrowserDevice(FakeDevice(id: 'late')));
    }
  });
  test('request invokes JS synchronously; accept-all does not serialize null filters', () async {
    final result = ble.requestDevice();
    expect(calls, 1);
    expect(requested.hasProperty('filters'.toJS).toDart, isFalse);
    expect(
      requested.getProperty<JSBoolean>('acceptAllDevices'.toJS).toDart,
      isTrue,
    );
    selection.complete(
      BrowserDevice(FakeDevice(id: 'origin-id', name: 'Test')),
    );
    final device = await result;
    expect(device.deviceId.value, 'origin-id');
    expect(device.name, 'Test');
    expect(device.rssi, isNull);
    expect(device.serviceUuids, isEmpty);
    expect(ble.capabilities.continuousScan, isFalse);
  });
  test(
    'service filters are OR clauses and optional permissions are independent',
    () async {
      final result = ble.requestDevice(
        options: BleDeviceRequest(
          serviceUuids: [BleUuid('180f'), BleUuid('180d')],
          optionalServices: [BleUuid('1234')],
        ),
      );
      expect(requested.hasProperty('acceptAllDevices'.toJS).toDart, isFalse);
      final filters = requested
          .getProperty<JSArray<JSObject>>('filters'.toJS)
          .toDart;
      expect(filters, hasLength(2));
      expect(filters.first.hasProperty('namePrefix'.toJS).toDart, isFalse);
      expect(
        filters.first
            .getProperty<JSArray<JSString>>('services'.toJS)
            .toDart
            .single
            .toDart,
        BleUuid('180f').value,
      );
      expect(
        requested
            .getProperty<JSArray<JSString>>('optionalServices'.toJS)
            .toDart
            .single
            .toDart,
        BleUuid('1234').value,
      );
      selection.complete(BrowserDevice(FakeDevice(id: 'selected')));
      await result;
    },
  );
  test(
    'cancel preserves physical chooser reservation and discards late selection',
    () async {
      final cancel = BleCancellation();
      final result = ble.requestDevice(cancellation: cancel);
      final assertion = expectLater(
        result,
        throwsA(error(BleErrorCode.cancelled)),
      );
      cancel.cancel();
      await assertion;
      await expectLater(
        ble.requestDevice(),
        throwsA(error(BleErrorCode.invalidState)),
      );
      expect(calls, 1);
      selection.complete(BrowserDevice(FakeDevice(id: 'late')));
      await selection.future;
    },
  );
  test(
    'browser denial preserves portable permission error and native evidence',
    () async {
      final deniedBrowser = FakeBrowser(
        requestDevice:
            ((BrowserDeviceOptions options) => JSPromise<BrowserDevice>(
              ((JSFunction resolve, JSFunction reject) {
                reject.callAsFunction(
                  null,
                  web.DOMException('Gesture required', 'SecurityError'),
                );
              }).toJS,
            )).toJS,
      );
      final denied = Ble(
        backend: WebBleBackend(
          bluetooth: BrowserBluetooth(deniedBrowser),
          secureContext: true,
        ),
      );
      addTearDown(denied.close);
      final result = denied.requestDevice();
      final assertion = expectLater(
        result,
        throwsA(
          error(
            BleErrorCode.permissionDenied,
          ).having((e) => e.context.nativeCode, 'native code', 'SecurityError'),
        ),
      );
      await assertion;
    },
  );
  test('settled chooser can be reused immediately without releasing a newer reservation', () async {
    final first = ble.requestDevice();
    selection.complete(BrowserDevice(FakeDevice(id: 'selected')));
    await first;
    final second = await ble.requestDevice();
    expect(second.deviceId.value, 'selected');
    expect(calls, 2);
  });
  test('insecure context gates chooser and scan remains unsupported', () async {
    final insecure = Ble(backend: WebBleBackend(secureContext: false));
    expect(insecure.capabilities.requestDevice, isFalse);
    await expectLater(
      insecure.requestDevice(),
      throwsA(error(BleErrorCode.notSupported)),
    );
    await insecure.close();
    await expectLater(ble.scan(), emitsError(error(BleErrorCode.notSupported)));
  });
  test(
    'close completes selection and ignores browser-owned late success',
    () async {
      final result = ble.requestDevice();
      final assertion = expectLater(
        result,
        throwsA(error(BleErrorCode.disposed)),
      );
      await ble.close();
      await assertion;
      selection.complete(BrowserDevice(FakeDevice(id: 'late')));
      await expectLater(
        ble.requestDevice(),
        throwsA(error(BleErrorCode.disposed)),
      );
    },
  );
}
