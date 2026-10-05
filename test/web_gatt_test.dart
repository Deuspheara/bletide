@TestOn('browser')
library;

import 'dart:async';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:bletide/src/backend.dart';
import 'package:bletide/src/web/bluetooth.dart';
import 'package:bletide/src/web/web_backend.dart';
import 'package:bletide/src/web/web_connection.dart';
import 'package:test/test.dart';
import 'package:web/web.dart' as web;

import 'support/gatt_contract.dart';

void property(JSObject target, String name, JSAny? value) =>
    target.setProperty(name.toJS, value);
TypeMatcher<BleException> error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);

final class Fixture {
  Fixture({this.connectBarrier, this.readBarrier, this.availabilityBarrier}) {
    if (availabilityBarrier != null) {
      property(
        browser,
        'getAvailability',
        (() => availabilityBarrier!.future.toJS).toJS,
      );
    }
    property(server, 'connected', false.toJS);
    property(
      server,
      'connect',
      (() {
        calls.add('connect');
        return (connectBarrier?.future ?? Future.value(BrowserGatt(server)))
            .then((value) {
              property(server, 'connected', true.toJS);
              return value;
            })
            .toJS;
      }).toJS,
    );
    property(
      server,
      'disconnect',
      (() {
        calls.add('disconnect');
        property(server, 'connected', false.toJS);
      }).toJS,
    );
    property(device, 'id', 'web-fixture'.toJS);
    property(device, 'name', 'Test peripheral'.toJS);
    property(device, 'gatt', server);
    property(characteristic, 'uuid', BleUuid('2a19').value.toJS);
    final props = JSObject();
    for (final name in [
      'read',
      'write',
      'writeWithoutResponse',
      'notify',
      'indicate',
    ]) {
      property(props, name, true.toJS);
    }
    property(characteristic, 'properties', props);
    property(
      characteristic,
      'readValue',
      (() {
        calls.add('read');
        if (!readStarted.isCompleted) readStarted.complete();
        return (readBarrier?.future ??
                Future.value(
                  ByteData.sublistView(
                    Uint8List.fromList([90, 0, 255, 128, 91]),
                    1,
                    4,
                  ).toJS,
                ))
            .toJS;
      }).toJS,
    );
    for (final name in [
      'writeValueWithResponse',
      'writeValueWithoutResponse',
    ]) {
      property(
        characteristic,
        name,
        ((JSUint8Array bytes) {
          calls.add(name);
          writes.add(Uint8List.fromList(bytes.toDart));
          return Future<JSAny?>.value(null).toJS;
        }).toJS,
      );
    }
    property(descriptor, 'uuid', BleUuid('2901').value.toJS);
    property(
      descriptor,
      'readValue',
      (() => Future.value(
        ByteData.sublistView(Uint8List.fromList([65])).toJS,
      ).toJS).toJS,
    );
    property(
      descriptor,
      'writeValue',
      ((JSUint8Array value) {
        writes.add(Uint8List.fromList(value.toDart));
        return Future<JSAny?>.value(null).toJS;
      }).toJS,
    );
    property(
      characteristic,
      'getDescriptors',
      (() => Future.value([BrowserDescriptor(descriptor)].toJS).toJS).toJS,
    );
    property(
      characteristic,
      'startNotifications',
      (() {
        calls.add('subscribe');
        return Future.value(BrowserCharacteristic(characteristic)).toJS;
      }).toJS,
    );
    property(
      characteristic,
      'stopNotifications',
      (() {
        calls.add('unsubscribe');
        return Future.value(BrowserCharacteristic(characteristic)).toJS;
      }).toJS,
    );
    property(service, 'uuid', BleUuid('180f').value.toJS);
    property(service, 'isPrimary', true.toJS);
    property(
      service,
      'getCharacteristics',
      (() => Future.value(
        [BrowserCharacteristic(characteristic)].toJS,
      ).toJS).toJS,
    );
    property(
      server,
      'getPrimaryServices',
      (() {
        calls.add('discover');
        return Future.value([BrowserService(service)].toJS).toJS;
      }).toJS,
    );
    property(
      browser,
      'requestDevice',
      ((BrowserDeviceOptions _) => Future.value(
        BrowserDevice(device),
      ).toJS).toJS,
    );
    backend = WebBleBackend(
      bluetooth: BrowserBluetooth(browser),
      secureContext: true,
    );
    ble = Ble(backend: backend);
  }
  final Completer<BrowserGatt>? connectBarrier;
  final Completer<JSDataView>? readBarrier;
  final Completer<JSBoolean>? availabilityBarrier;
  final server = JSObject(), service = JSObject(), descriptor = JSObject();
  final browser = web.EventTarget();
  final device = web.EventTarget(), characteristic = web.EventTarget();
  final readStarted = Completer<void>();
  final calls = <String>[];
  final writes = <Uint8List>[];
  late final WebBleBackend backend;
  late final Ble ble;
  Future<BleConnection> connect() async {
    final selected = await ble.requestDevice();
    return ble.connect(selected.deviceId);
  }

  void notify(List<int> value) {
    property(
      characteristic,
      'value',
      ByteData.sublistView(Uint8List.fromList(value)).toJS,
    );
    characteristic.dispatchEvent(web.Event('characteristicvaluechanged'));
  }

  void available(bool value) {
    final event = web.Event('availabilitychanged');
    property(event, 'available', value.toJS);
    browser.dispatchEvent(event);
  }
}

void main() {
  test('nonstandard CCCD policy is unsupported before browser setup', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final connection = await f.connect();
    final char =
        (await connection.discoverServices()).single.characteristics.single;
    await expectLater(
      connection.enableNotifications(
        char,
        setupMode: BleNotificationSetupMode.compat,
      ),
      throwsA(error(BleErrorCode.notSupported)),
    );
    expect(f.calls, isNot(contains('subscribe')));
    expect(f.calls, isNot(contains('unsubscribe')));
    final owner = await connection.enableNotifications(char);
    await owner.cancel();
  });

  test('replacement readiness preserves browser notification stop failure and permits reconnect', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final connection = await f.connect();
    final char =
        (await connection.discoverServices()).single.characteristics.single;
    final owner = await connection.enableNotifications(char);
    JSFunction? rejecting;
    final started = Completer<void>();
    property(
      f.characteristic,
      'stopNotifications',
      (() => JSPromise<BrowserCharacteristic>(
        ((JSFunction resolve, JSFunction reject) {
          rejecting = reject;
          started.complete();
        }).toJS,
      )).toJS,
    );
    Object? releaseError;
    final releasing = owner.cancel().then<void>(
      (_) {
        fail('Stop should fail');
      },
      onError: (Object error) {
        releaseError = error;
      },
    );
    await started.future;
    Object? readinessError;
    final replacement = connection
        .enableNotifications(char)
        .then<void>(
          (_) {
            fail('Replacement should fail');
          },
          onError: (Object error) {
            readinessError = error;
          },
        );
    rejecting!.callAsFunction(
      null,
      web.DOMException('CCCD disable rejected', 'OperationError'),
    );
    await replacement;
    await releasing;
    expect(readinessError, error(BleErrorCode.gattFailure));
    final cause = readinessError as BleException;
    expect(cause.context.operation, 'unsubscribe');
    expect(cause.context.platform, 'web');
    expect(cause.context.nativeCode, 'OperationError');
    expect(cause.context.nativeMessage, 'CCCD disable rejected');
    expect(cause.context.characteristicUuid, char.uuid);
    expect(cause.context.connectionGeneration, connection.generation);
    expect(releaseError, error(BleErrorCode.gattFailure));
    expect(connection.state, BleConnectionState.disconnected);
    expect(f.backend.debugResources.connections, 0);
    final next = await f.ble.connect(connection.deviceId);
    expect(next.generation, greaterThan(connection.generation));
  });

  test(
    'awaitable notification setup waits for browser promise and owns teardown',
    () async {
      final f = Fixture();
      addTearDown(f.ble.close);
      final c = await f.connect();
      final char = (await c.discoverServices()).single.characteristics.single;
      final started = Completer<void>();
      final enabled = Completer<BrowserCharacteristic>();
      property(
        f.characteristic,
        'startNotifications',
        (() {
          started.complete();
          return enabled.future.toJS;
        }).toJS,
      );
      var ready = false;
      final setup = c.enableNotifications(char).then((owner) {
        ready = true;
        return owner;
      });
      await started.future;
      expect(ready, isFalse);
      enabled.complete(BrowserCharacteristic(f.characteristic));
      final owner = await setup;
      final value = Completer<Uint8List>();
      final listener = owner.values.listen(value.complete);
      f.notify([0, 255]);
      expect(await value.future, [0, 255]);
      await listener.cancel();
      await owner.cancel();
      expect(f.calls.where((e) => e == 'unsubscribe'), hasLength(1));
      await expectLater(
        c.getWritePayloadLimit(),
        throwsA(error(BleErrorCode.notSupported)),
      );
      await expectLater(
        c.requestConnectionPriority(BleConnectionPriority.high),
        throwsA(error(BleErrorCode.notSupported)),
      );
    },
  );
  test('cancelling queued browser retry cannot create another connection', () async {
    final barrier = Completer<BrowserGatt>();
    final f = Fixture(connectBarrier: barrier);
    addTearDown(f.ble.close);
    final selected = await f.ble.requestDevice();
    final first = f.backend.connect(selected.deviceId);
    final failed = expectLater(
      first.result,
      throwsA(error(BleErrorCode.cancelled)),
    );
    first.cancel();
    await failed;
    final retry = f.backend.connect(selected.deviceId);
    final cancelled = expectLater(
      retry.result,
      throwsA(error(BleErrorCode.cancelled)),
    );
    retry.cancel();
    await cancelled;
    barrier.complete(BrowserGatt(f.server));
    // A subsequent retry waits for the same retirement and then connects once.
    final next = await f.backend.connect(selected.deviceId).result;
    expect(f.calls.where((e) => e == 'connect'), hasLength(2));
    await next.disconnect().result;
  });

  registerGattContracts('Web Bluetooth / JS', () async {
    final f = Fixture();
    var value = Uint8List.fromList([0, 255, 128]);
    var descriptor = Uint8List(0);
    final issued = Completer<void>();
    final release = Completer<JSAny?>();
    property(
      f.server,
      'connect',
      (() {
        value = Uint8List.fromList([0, 255, 128]);
        descriptor = Uint8List(0);
        property(f.server, 'connected', true.toJS);
        return Future.value(BrowserGatt(f.server)).toJS;
      }).toJS,
    );
    property(
      f.characteristic,
      'readValue',
      (() => Future.value(
        ByteData.sublistView(Uint8List.fromList(value)).toJS,
      ).toJS).toJS,
    );
    for (final name in [
      'writeValueWithResponse',
      'writeValueWithoutResponse',
    ]) {
      property(
        f.characteristic,
        name,
        ((JSUint8Array input) {
          final bytes = Uint8List.fromList(input.toDart);
          if (bytes.length == 1 && bytes[0] == 0xff) {
            issued.complete();
            return release.future.toJS;
          }
          if (bytes.length == 1 && bytes[0] == 0xee) {
            return JSPromise<JSAny?>(
              ((JSFunction resolve, JSFunction reject) {
                reject.callAsFunction(
                  null,
                  web.DOMException(
                    'Controlled peripheral write failure',
                    'OperationError',
                  ),
                );
              }).toJS,
            );
          }
          value = bytes;
          f.notify(value);
          return Future<JSAny?>.value(null).toJS;
        }).toJS,
      );
    }
    property(
      f.descriptor,
      'readValue',
      (() => Future.value(
        ByteData.sublistView(Uint8List.fromList(descriptor)).toJS,
      ).toJS).toJS,
    );
    property(
      f.descriptor,
      'writeValue',
      ((JSUint8Array input) {
        descriptor = Uint8List.fromList(input.toDart);
        return Future<JSAny?>.value(null).toJS;
      }).toJS,
    );
    return GattContractFixture(
      f.ble,
      f.connect,
      writeIssued: () => issued.future,
      cleanup: () async {
        if (!release.isCompleted) release.complete(null);
      },
    );
  });
  test(
    'availability loss aborts connecting and cleans late browser server',
    () async {
      final query = Completer<JSBoolean>()..complete(true.toJS);
      final connect = Completer<BrowserGatt>();
      final f = Fixture(availabilityBarrier: query, connectBarrier: connect);
      addTearDown(f.ble.close);
      await f.ble.ready;
      final selected = await f.ble.requestDevice();
      final request = f.backend.connect(selected.deviceId);
      final failed = expectLater(
        request.result,
        throwsA(error(BleErrorCode.adapterUnavailable)),
      );
      f.available(false);
      await failed;
      connect.complete(BrowserGatt(f.server));
      final deadline = DateTime.now().add(const Duration(seconds: 2));
      while (f.calls.where((call) => call == 'disconnect').length < 2 &&
          DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(Duration.zero);
      }
      expect(f.calls.where((call) => call == 'disconnect'), hasLength(2));
      expect(BrowserGatt(f.server).connected, isFalse);
    },
  );

  test('availability event wins over older query and gates connect', () async {
    final query = Completer<JSBoolean>();
    final f = Fixture(availabilityBarrier: query);
    addTearDown(f.ble.close);
    expect(f.backend.capabilities.adapterState, isTrue);
    f.available(false);
    query.complete(true.toJS);
    await f.ble.ready;
    expect(f.backend.currentAdapterState, BleAdapterState.unavailable);
    final selected = await f.ble.requestDevice();
    await expectLater(
      f.ble.connect(selected.deviceId),
      throwsA(error(BleErrorCode.adapterUnavailable)),
    );
    f.available(true);
    final connection = await f.ble.connect(selected.deviceId);
    await connection.disconnect();
  });

  test(
    'close while availability pending ignores late query and events',
    () async {
      final query = Completer<JSBoolean>();
      final f = Fixture(availabilityBarrier: query);
      final ready = expectLater(
        f.ble.ready,
        throwsA(error(BleErrorCode.disposed)),
      );
      await f.ble.close();
      await ready;
      query.complete(true.toJS);
      await query.future;
      f.available(true);
      expect(f.backend.currentAdapterState, BleAdapterState.unsupported);
    },
  );

  test(
    'availability loss interrupts running GATT and rejects late data',
    () async {
      final query = Completer<JSBoolean>()..complete(true.toJS);
      final read = Completer<JSDataView>();
      final f = Fixture(availabilityBarrier: query, readBarrier: read);
      addTearDown(f.ble.close);
      await f.ble.ready;
      final selected = await f.ble.requestDevice();
      final connection = await f.backend.connect(selected.deviceId).result;
      final services = await connection.discoverServices().result;
      final characteristic = services.single.characteristics.single;
      final result = expectLater(
        connection.read(characteristic).result,
        throwsA(error(BleErrorCode.adapterUnavailable)),
      );
      await f.readStarted.future;
      f.available(false);
      await result;
      expect(f.calls.where((call) => call == 'disconnect'), hasLength(1));
      read.complete(ByteData(1).toJS);
      await read.future;
      expect(f.backend.currentAdapterState, BleAdapterState.unavailable);
    },
  );

  test(
    'disconnect clears queued callbacks before the browser call settles',
    () async {
      final barrier = Completer<JSDataView>();
      final f = Fixture(readBarrier: barrier);
      addTearDown(f.ble.close);
      final selected = await f.ble.requestDevice();
      final c =
          await f.backend.connect(selected.deviceId).result as WebBleConnection;
      final char =
          (await c.discoverServices().result).single.characteristics.single;
      await c.debugActiveCompletion;
      final read = c.read(char);
      final readError = expectLater(
        read.result,
        throwsA(error(BleErrorCode.disconnected)),
      );
      await f.readStarted.future;
      final queued = <Future<void>>[];
      for (var index = 0; index < 100; index++) {
        final request = c.write(char, Uint8List.fromList([index]), true);
        queued.add(
          expectLater(
            request.result,
            throwsA(error(BleErrorCode.disconnected)),
          ),
        );
      }
      expect(c.debugResources.queued, 100);
      expect(c.debugResources.pending, 101);
      await c.disconnect().result;
      await readError;
      await Future.wait(queued);
      expect(c.debugResources, (
        pending: 0,
        queued: 0,
        active: 1,
        listeners: 0,
        characters: 0,
        descriptors: 0,
      ));
      expect(f.backend.debugResources.connections, 0);
      expect(f.writes, isEmpty);
      // A browser-owned operation still exists; settling it releases that final job.
      barrier.complete(ByteData.sublistView(Uint8List.fromList([99])).toJS);
      await c.debugActiveCompletion;
      expect(c.debugResources.active, 0);
      expect(f.writes, isEmpty);
      await f.ble.close();
      expect(f.backend.debugResources, (
        devices: 0,
        connections: 0,
        browserConnects: 0,
        chooser: 0,
        availabilityListeners: 0,
      ));
    },
  );

  test('late discovery cannot start further browser attribute calls after cancellation', () async {
    for (final stage in ['services', 'characteristics']) {
      final f = Fixture();
      addTearDown(f.ble.close);
      final services = Completer<JSArray<BrowserService>>();
      final characters = Completer<JSArray<BrowserCharacteristic>>();
      final started = Completer<void>();
      var nextCalls = 0;
      if (stage == 'services') {
        property(
          f.server,
          'getPrimaryServices',
          (() {
            started.complete();
            return services.future.toJS;
          }).toJS,
        );
        property(
          f.service,
          'getCharacteristics',
          (() {
            nextCalls++;
            return Future.value([BrowserCharacteristic(f.characteristic)].toJS)
                .toJS;
          }).toJS,
        );
      } else {
        property(
          f.service,
          'getCharacteristics',
          (() {
            started.complete();
            return characters.future.toJS;
          }).toJS,
        );
        property(
          f.characteristic,
          'getDescriptors',
          (() {
            nextCalls++;
            return Future.value([BrowserDescriptor(f.descriptor)].toJS).toJS;
          }).toJS,
        );
      }
      final selected = await f.ble.requestDevice();
      final c =
          await f.backend.connect(selected.deviceId).result as WebBleConnection;
      final request = c.discoverServices();
      final cancelled = expectLater(
        request.result,
        throwsA(error(BleErrorCode.cancelled)),
      );
      await started.future;
      request.cancel();
      await cancelled;
      if (stage == 'services') {
        services.complete([BrowserService(f.service)].toJS);
      } else {
        characters.complete([BrowserCharacteristic(f.characteristic)].toJS);
      }
      await c.debugActiveCompletion;
      expect(nextCalls, 0, reason: stage);
      expect(c.debugResources, (
        pending: 0,
        queued: 0,
        active: 0,
        listeners: 0,
        characters: 0,
        descriptors: 0,
      ));
    }
  });

  test('facade cancellation reports browser stop failure and disconnects generation', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final connection = await f.connect();
    final char =
        (await connection.discoverServices()).single.characteristics.single;
    final received = <Uint8List>[];
    final listener = connection.subscribe(char).listen(received.add);
    // The read runs behind setup in the connection FIFO.
    await connection.read(char);
    expect(f.calls, contains('subscribe'));
    property(
      f.characteristic,
      'stopNotifications',
      (() => JSPromise<BrowserCharacteristic>(
        ((JSFunction resolve, JSFunction reject) {
          reject.callAsFunction(
            null,
            web.DOMException('Stop failed', 'OperationError'),
          );
        }).toJS,
      )).toJS,
    );
    await expectLater(
      listener.cancel(),
      throwsA(error(BleErrorCode.gattFailure)),
    );
    expect(f.calls, contains('disconnect'));
    f.notify([7]);
    expect(received, isEmpty);
    await expectLater(
      connection.read(char),
      throwsA(error(BleErrorCode.disconnected)),
    );
    final next = await f.ble.connect(connection.deviceId);
    expect(next.generation, greaterThan(connection.generation));
    final nextChar =
        (await next.discoverServices()).single.characteristics.single;
    expect(await next.read(nextChar), [0, 255, 128]);
  });

  test('failed notification stop retains ownership and a retry removes the listener', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final selected = await f.ble.requestDevice();
    final c =
        await f.backend.connect(selected.deviceId).result as WebBleConnection;
    final char =
        (await c.discoverServices().result).single.characteristics.single;
    await c.subscribe(char).result;
    property(
      f.characteristic,
      'stopNotifications',
      (() => JSPromise<BrowserCharacteristic>(
        ((JSFunction resolve, JSFunction reject) {
          reject.callAsFunction(
            null,
            web.DOMException('Stop failed', 'OperationError'),
          );
        }).toJS,
      )).toJS,
    );
    await expectLater(
      c.unsubscribe(char).result,
      throwsA(error(BleErrorCode.gattFailure)),
    );
    await c.debugActiveCompletion;
    expect(c.debugResources.listeners, 2);
    final incoming = <BackendNotification>[];
    final listener = c.notifications.listen(incoming.add);
    f.notify([7]);
    expect(incoming, hasLength(1));
    property(
      f.characteristic,
      'stopNotifications',
      (() => Future.value(BrowserCharacteristic(f.characteristic)).toJS).toJS,
    );
    await c.unsubscribe(char).result;
    await c.debugActiveCompletion;
    expect(c.debugResources.listeners, 1);
    f.notify([8]);
    expect(incoming, hasLength(1));
    await listener.cancel();
    await c.disconnect().result;
    expect(c.debugResources.listeners, 0);
  });

  test(
    '100 browser connection and notification cycles remove all event delivery',
    () async {
      final f = Fixture();
      addTearDown(f.ble.close);
      final selected = await f.ble.requestDevice();
      var previous = 0;
      for (var index = 0; index < 100; index++) {
        final c = await f.ble.connect(selected.deviceId);
        expect(c.generation, greaterThan(previous));
        previous = c.generation;
        expect(f.backend.debugResources.connections, 1);
        expect(f.backend.debugResources.browserConnects, 0);
        final char = (await c.discoverServices()).single.characteristics.single;
        final incoming = <Uint8List>[];
        final subscription = c.subscribe(char).listen(incoming.add);
        await c.read(char);
        f.notify([index]);
        await c.read(char);
        expect(incoming.single, [index]);
        await subscription.cancel();
        await c.read(char);
        await c.disconnect();
        expect(f.backend.debugResources.connections, 0);
        expect(f.backend.debugResources.browserConnects, 0);
        expect(f.backend.debugResources.devices, 1);
        f.notify([255]);
        expect(incoming, hasLength(1));
        expect(
          f.server.getProperty<JSBoolean>('connected'.toJS).toDart,
          isFalse,
        );
      }
      expect(f.calls.where((call) => call == 'subscribe'), hasLength(100));
      expect(f.calls.where((call) => call == 'unsubscribe'), hasLength(100));
      expect(f.calls.where((call) => call == 'disconnect'), hasLength(100));
    },
  );

  test(
    'running read cancellation closes generation and skips queued write',
    () async {
      final barrier = Completer<JSDataView>();
      final f = Fixture(readBarrier: barrier);
      addTearDown(f.ble.close);
      final selected = await f.ble.requestDevice();
      final c = await f.backend.connect(selected.deviceId).result;
      final char =
          (await c.discoverServices().result).single.characteristics.single;
      final read = c.read(char);
      await f.readStarted.future;
      final queued = c.write(char, Uint8List.fromList([7]), true);
      final first = expectLater(
        read.result,
        throwsA(error(BleErrorCode.cancelled)),
      );
      final second = expectLater(
        queued.result,
        throwsA(error(BleErrorCode.disconnected)),
      );
      read.cancel();
      await first;
      await second;
      expect(f.writes, isEmpty);
      expect(f.server.getProperty<JSBoolean>('connected'.toJS).toDart, isFalse);
      barrier.complete(ByteData.sublistView(Uint8List.fromList([99])).toJS);
      final next = await f.backend.connect(selected.deviceId).result;
      expect(next.generation, greaterThan(c.generation));
      await next.disconnect().result;
    },
  );
  test('missing characteristic context is scoped and ordinary error keeps FIFO usable', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final c = await f.connect();
    final char = (await c.discoverServices()).single.characteristics.single;
    final missing = BleCharacteristic(
      serviceUuid: char.serviceUuid,
      uuid: BleUuid('1234'),
      properties: const BleCharacteristicProperties(2),
    );
    await expectLater(
      c.read(missing),
      throwsA(
        error(BleErrorCode.characteristicNotFound)
            .having(
              (e) => e.context.characteristicUuid,
              'characteristic',
              missing.uuid,
            )
            .having(
              (e) => e.context.connectionGeneration,
              'generation',
              c.generation,
            ),
      ),
    );
    expect(await c.read(char), [0, 255, 128]);
  });
  test(
    'disconnect cleanup error is reported and engine close preserves it',
    () async {
      final f = Fixture();
      final c = await f.connect();
      property(
        f.server,
        'disconnect',
        ((() {
          throw StateError('OS disconnect failed');
        }) as void Function()).toJS,
      );
      await expectLater(
        c.disconnect(),
        throwsA(error(BleErrorCode.gattFailure)),
      );
      await expectLater(
        f.ble.close(),
        throwsA(error(BleErrorCode.gattFailure)),
      );
      expect(c.state, BleConnectionState.disconnected);
    },
  );

  test('browser discovery, copied reads, both writes and descriptors use actual JS calls', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final c = await f.connect();
    final services = await c.discoverServices();
    final char = services.single.characteristics.single;
    expect(char.properties.bits, 62);
    expect(await c.read(char), [0, 255, 128]);
    final bytes = Uint8List.fromList([1, 2]);
    final write = c.write(char, bytes);
    bytes[0] = 99;
    await write;
    await c.write(char, Uint8List.fromList([3]), withResponse: false);
    expect(f.writes, [
      [1, 2],
      [3],
    ]);
    final desc = char.descriptors.single;
    expect(await c.readDescriptor(desc), [65]);
    await c.writeDescriptor(desc, Uint8List.fromList([66]));
    expect(f.writes.last, [66]);
    await expectLater(c.readRssi(), throwsA(error(BleErrorCode.notSupported)));
    await c.disconnect();
    await c.disconnect();
    expect(f.calls.where((call) => call == 'disconnect'), hasLength(1));
  });
  test(
    'direct backend FIFO waits for read; queued cancellation skips JS write',
    () async {
      final barrier = Completer<JSDataView>();
      final f = Fixture(readBarrier: barrier);
      addTearDown(f.ble.close);
      final selected = await f.ble.requestDevice();
      final c = await f.backend.connect(selected.deviceId).result;
      final char =
          (await c.discoverServices().result).single.characteristics.single;
      final read = c.read(char);
      final write = c.write(char, Uint8List.fromList([7]), true);
      final cancelled = expectLater(
        write.result,
        throwsA(error(BleErrorCode.cancelled)),
      );
      write.cancel();
      await cancelled;
      barrier.complete(ByteData.sublistView(Uint8List.fromList([8])).toJS);
      expect(await read.result, [8]);
      expect(f.writes, isEmpty);
      await c.write(char, Uint8List.fromList([9]), true).result;
      expect(f.writes.single, [9]);
    },
  );
  test('notifications share setup; reconnect rejects old generation and old connection', () async {
    final f = Fixture();
    addTearDown(f.ble.close);
    final c = await f.connect();
    final char = (await c.discoverServices()).single.characteristics.single;
    final incoming = <Uint8List>[];
    final first = c.subscribe(char).listen(incoming.add);
    final second = c.subscribe(char).listen(incoming.add);
    // A queued read is the explicit subscription setup/delivery barrier.
    await c.read(char);
    expect(f.calls.where((call) => call == 'subscribe'), hasLength(1));
    f.notify([4, 5]);
    await c.read(char);
    expect(incoming, [
      [4, 5],
      [4, 5],
    ]);
    await first.cancel();
    expect(f.calls.where((call) => call == 'unsubscribe'), isEmpty);
    await c.disconnect();
    await second.cancel();
    final next = await f.ble.connect(c.deviceId);
    expect(next.generation, greaterThan(c.generation));
    f.notify([99]);
    expect(incoming, hasLength(2));
    await expectLater(c.read(char), throwsA(error(BleErrorCode.disconnected)));
  });
  test('cancelled connect retains lease until late browser connect is disconnected', () async {
    final barrier = Completer<BrowserGatt>();
    final f = Fixture(connectBarrier: barrier);
    addTearDown(f.ble.close);
    final selected = await f.ble.requestDevice();
    final request = f.backend.connect(selected.deviceId);
    final assertion = expectLater(
      request.result,
      throwsA(error(BleErrorCode.cancelled)),
    );
    request.cancel();
    await assertion;
    final retry = f.backend.connect(selected.deviceId);
    expect(f.calls.where((call) => call == 'connect'), hasLength(1));
    barrier.complete(BrowserGatt(f.server));
    // Poll observed JS cleanup, not a delay interpreted as successful cleanup.
    final deadline = Stopwatch()..start();
    while (f.calls.where((call) => call == 'disconnect').length < 2 &&
        deadline.elapsed < const Duration(seconds: 2)) {
      await Future<void>.delayed(Duration.zero);
    }
    expect(f.calls.where((call) => call == 'disconnect'), hasLength(2));
    final next = await retry.result;
    expect(f.calls.where((call) => call == 'connect'), hasLength(2));
    expect(f.server.getProperty<JSBoolean>('connected'.toJS).toDart, isTrue);
    await next.disconnect().result;
  });
  test(
    'remote loss cancels running read and late result cannot revive generation',
    () async {
      final barrier = Completer<JSDataView>();
      final f = Fixture(readBarrier: barrier);
      addTearDown(f.ble.close);
      final c = await f.connect();
      final char = (await c.discoverServices()).single.characteristics.single;
      final read = c.read(char);
      final assertion = expectLater(
        read,
        throwsA(error(BleErrorCode.disconnected)),
      );
      f.device.dispatchEvent(web.Event('gattserverdisconnected'));
      await assertion;
      barrier.complete(ByteData.sublistView(Uint8List.fromList([99])).toJS);
      final next = await f.ble.connect(c.deviceId);
      expect(next.generation, greaterThan(c.generation));
      expect(c.state, BleConnectionState.disconnected);
    },
  );
}
