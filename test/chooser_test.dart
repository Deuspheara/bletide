import 'package:fake_async/fake_async.dart';
import 'package:bletide/bletide.dart';
import 'package:bletide/testing.dart';
import 'package:test/test.dart';

TypeMatcher<BleException> error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);
void main() {
  test(
    'chooser starts synchronously and returns an opaque selected identity',
    () async {
      final backend = FakeBleBackend(
        capabilities: const BleCapabilities(requestDevice: true),
      );
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final selected = ble.requestDevice();
      expect(backend.history, ['requestDevice']);
      final ad = BleAdvertisement(
        deviceId: const BleDeviceId('browser-origin-id'),
        name: 'Test',
      );
      backend.next<BleAdvertisement>('requestDevice').complete(ad);
      expect(await selected, same(ad));
    },
  );
  test(
    'concurrent chooser is rejected and engine close cancels selection',
    () async {
      final backend = FakeBleBackend(
        capabilities: const BleCapabilities(requestDevice: true),
      );
      final ble = Ble(backend: backend);
      final selected = ble.requestDevice();
      final assertion = expectLater(
        selected,
        throwsA(error(BleErrorCode.disposed)),
      );
      await expectLater(
        ble.requestDevice(),
        throwsA(error(BleErrorCode.invalidState)),
      );
      await ble.close();
      await assertion;
      expect(backend.pending, isEmpty);
      await expectLater(
        ble.requestDevice(),
        throwsA(error(BleErrorCode.disposed)),
      );
    },
  );
  test('pre-cancelled selection never opens chooser', () async {
    final backend = FakeBleBackend(
      capabilities: const BleCapabilities(requestDevice: true),
    );
    final ble = Ble(backend: backend);
    addTearDown(ble.close);
    final cancel = BleCancellation()..cancel();
    await expectLater(
      ble.requestDevice(cancellation: cancel),
      throwsA(error(BleErrorCode.cancelled)),
    );
    expect(backend.history, isEmpty);
  });
  test('chooser timeout cancels backend through virtual clock', () {
    fakeAsync((clock) {
      final backend = FakeBleBackend(
        capabilities: const BleCapabilities(requestDevice: true),
      );
      final ble = Ble(backend: backend);
      Object? failure;
      ble
          .requestDevice(timeout: const Duration(seconds: 2))
          .then(
            (_) {},
            onError: (Object error) {
              failure = error;
            },
          );
      clock.elapse(const Duration(seconds: 2));
      clock.flushMicrotasks();
      expect(failure, error(BleErrorCode.timeout));
      expect(backend.pending, isEmpty);
      ble.close();
      clock.flushMicrotasks();
      expect(clock.nonPeriodicTimerCount, 0);
    });
  });
  test('chooser options snapshot UUID grants and reject empty prefix', () {
    final services = [BleUuid('180f')];
    final options = BleDeviceRequest(
      serviceUuids: services,
      optionalServices: services,
    );
    services.clear();
    expect(options.serviceUuids, [BleUuid('180f')]);
    expect(options.optionalServices, [BleUuid('180f')]);
    expect(() => options.serviceUuids.clear(), throwsUnsupportedError);
    expect(() => BleDeviceRequest(namePrefix: ''), throwsFormatException);
  });
}
