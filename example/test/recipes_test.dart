import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:bletide/testing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../lib/recipes.dart';

void main() {
  const device = BleDeviceId('recipe-fixture');
  final characteristic = BleCharacteristic(
    serviceUuid: BleUuid('180f'),
    uuid: BleUuid('2a19'),
    properties: const BleCharacteristicProperties(0x3e),
  );
  for (final response in [true, false]) {
    test(
      'complete scan/GATT recipe with response=$response releases ownership',
      () async {
        final backend = FakeBleBackend();
        final ble = Ble(backend: backend);
        addTearDown(ble.close);
        final selection = findFirst(ble);
        final started = ble.diagnostics.firstWhere(
          (event) => event.name == 'scan.started',
        );
        (await backend.waitFor<void>('scan.start')).complete(null);
        await started;
        backend.advertise(BleAdvertisement(deviceId: device));
        (await backend.waitFor<void>('scan.stop')).complete(null);
        final session = runGattSession(
          ble,
          selection,
          serviceUuid: characteristic.serviceUuid,
          readUuid: characteristic.uuid,
          writeUuid: characteristic.uuid,
          notifyUuid: characteristic.uuid,
          payload: Uint8List.fromList([1]),
          withResponse: response,
        );
        await backend.waitFor<BackendConnection>('connect');
        final physical = backend.completeConnect(device);
        (await backend.waitFor<List<BleService>>('discover')).complete([
          BleService(
            uuid: characteristic.serviceUuid,
            primary: true,
            characteristics: [characteristic],
          ),
        ]);
        (await backend.waitFor<Uint8List>('read'))
            .complete(Uint8List.fromList([2]));
        (await backend.waitFor<void>('subscribe')).complete(null);
        final write = await backend.waitFor<void>(
          response ? 'write' : 'writeWithoutResponse',
        );
        expect(write.value, [1]);
        physical.emitNotification(characteristic, Uint8List.fromList([3]));
        write.complete(null);
        (await backend.waitFor<void>('unsubscribe')).complete(null);
        (await backend.waitFor<void>('disconnect')).complete(null);
        final result = await session;
        expect(result.read, [2]);
        expect(result.notification, [3]);
        expect(backend.history, [
          'scan.start',
          'scan.stop',
          'connect',
          'discover',
          'read',
          'subscribe',
          response ? 'write' : 'writeWithoutResponse',
          'unsubscribe',
          'disconnect',
        ]);
        expect(backend.pending, isEmpty);
        expect(backend.liveConnections, 0);
        expect(backend.subscriptions, 0);
        expect(backend.scanning, isFalse);
      },
    );
  }
  test(
    'scan recipe timeout awaits stop instead of leaving a lease alive',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final selection = findFirst(
        ble,
        timeout: const Duration(milliseconds: 20),
      );
      final failed = expectLater(selection, throwsA(isA<Exception>()));
      (await backend.waitFor<void>('scan.start')).complete(null);
      final stop = await backend.waitFor<void>('scan.stop');
      expect(backend.scanning, isTrue);
      stop.complete(null);
      await failed;
      expect(backend.scanning, isFalse);
      expect(backend.pending, isEmpty);
    },
  );
}
