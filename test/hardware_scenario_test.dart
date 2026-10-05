import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:bletide/src/backend.dart';
import 'package:bletide/src/fake_backend.dart';
import 'package:test/test.dart';

import '../integration_test/hardware_config.dart';
import '../integration_test/hardware_scenario.dart';
import 'hardware_config_test.dart' show configuration;

void main() {
  test(
    'configured unsupported radio requests are recorded without platform calls',
    () async {
      final backend = FakeBleBackend(
        capabilities: const BleCapabilities(connect: true, gatt: true),
      );
      final checks = <String>[];
      const device = BleDeviceId('unsupported-radio-fixture');
      final run = runHardwareScenario(
        Ble(backend: backend),
        HardwareConfig.fromMap(
          configuration()
            ..['BLE_REQUEST_MTU'] = '247'
            ..['BLE_CONNECTION_PRIORITY'] = 'balanced',
        ),
        selected: Future.value(
          BleAdvertisement(deviceId: device, name: 'Test'),
        ),
        record: (check, _) => checks.add(check),
      );
      final failed = expectLater(run, throwsA(isA<BleException>()));
      await backend.waitFor<BackendConnection>('connect');
      backend.completeConnect(device);
      (await backend.waitFor<List<BleService>>('discover')).fail(
        const BleException(
          BleErrorCode.gattFailure,
          'Stop fixture after capability checks',
        ),
      );
      await failed;
      expect(
        checks,
        containsAllInOrder([
          'requestMtuUnsupported',
          'requestConnectionPriorityUnsupported',
        ]),
      );
      expect(backend.history, isNot(contains('requestMtu')));
      expect(backend.history, isNot(contains('connection.priority')));
      expect(backend.pending, isEmpty);
      expect(backend.liveConnections, 0);
    },
  );

  test(
    'configured compat mode propagates unsupported and closes before writes',
    () async {
      final backend = FakeBleBackend();
      final checks = <String>[];
      final config = HardwareConfig.fromMap(
        configuration()..['BLE_NOTIFY_SETUP_MODE'] = 'compat',
      );
      const device = BleDeviceId('unsupported-notification-fixture');
      final run = runHardwareScenario(
        Ble(backend: backend),
        config,
        selected: Future.value(
          BleAdvertisement(deviceId: device, name: 'Test'),
        ),
        record: (check, _) => checks.add(check),
      );
      final failed = expectLater(
        run,
        throwsA(
          isA<BleException>().having(
            (e) => e.code,
            'code',
            BleErrorCode.notSupported,
          ),
        ),
      );
      await backend.waitFor<BackendConnection>('connect');
      backend.completeConnect(device);
      (await backend.waitFor<List<BleService>>('discover')).complete([
        BleService(
          uuid: config.service,
          primary: true,
          characteristics: [
            BleCharacteristic(
              serviceUuid: config.service,
              uuid: config.read,
              properties: const BleCharacteristicProperties(0x3e),
            ),
          ],
        ),
      ]);
      (await backend.waitFor<Uint8List>('read')).complete(config.expectedRead);
      // Observe the next event turn before awaiting the scenario: a discarded
      // mode must fail the no-setup assertion instead of hanging on a fake ACK.
      await Future<void>(() {});
      expect(backend.history, isNot(contains('subscribe')));
      await failed;
      expect(checks, isNot(contains('subscribed')));
      expect(backend.history, isNot(contains('write')));
      expect(backend.history, isNot(contains('writeWithoutResponse')));
      expect(backend.pending, isEmpty);
      expect(backend.subscriptions, 0);
      expect(backend.liveConnections, 0);
    },
  );

  test('hardware notification setup failure cleans up without claiming readiness or writing', () async {
    final backend = FakeBleBackend();
    final checks = <String>[];
    final config = HardwareConfig.fromMap(configuration());
    const device = BleDeviceId('failed-notification-fixture');
    final run = runHardwareScenario(
      Ble(backend: backend),
      config,
      selected: Future.value(BleAdvertisement(deviceId: device, name: 'Test')),
      record: (check, _) => checks.add(check),
    );
    final failed = expectLater(
      run,
      throwsA(
        isA<BleException>().having(
          (e) => e.code,
          'code',
          BleErrorCode.gattFailure,
        ),
      ),
    );
    await backend.waitFor<BackendConnection>('connect');
    backend.completeConnect(device);
    (await backend.waitFor<List<BleService>>('discover')).complete([
      BleService(
        uuid: config.service,
        primary: true,
        characteristics: [
          BleCharacteristic(
            serviceUuid: config.service,
            uuid: config.read,
            properties: const BleCharacteristicProperties(0x3e),
          ),
        ],
      ),
    ]);
    (await backend.waitFor<Uint8List>('read')).complete(config.expectedRead);
    (await backend.waitFor<void>('subscribe')).fail(
      const BleException(BleErrorCode.gattFailure, 'CCCD enable rejected'),
    );
    await failed;
    expect(checks, isNot(contains('subscribed')));
    expect(backend.history, isNot(contains('write')));
    expect(backend.pending, isEmpty);
    expect(backend.subscriptions, 0);
    expect(backend.liveConnections, 0);
  });

  test(
    'full hardware scenario joins scan, subscriptions and reconnect cleanup',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      final started = ble.diagnostics.firstWhere(
        (event) => event.name == 'scan.started',
      );
      final checks = <String>[];
      final evidence = <String, Map<String, Object?>>{};
      final config = HardwareConfig.fromMap(
        configuration()
          ..['BLE_REQUEST_MTU'] = '247'
          ..['BLE_CONNECTION_PRIORITY'] = 'high',
      );
      final run = runHardwareScenario(
        ble,
        config,
        record: (check, fields) {
          checks.add(check);
          evidence[check] = fields;
        },
      );
      final completed = expectLater(run, completes);
      (await backend.waitFor<void>('scan.start')).complete(null);
      await started;
      const device = BleDeviceId('hardware-fixture');
      backend.advertise(
        BleAdvertisement(deviceId: device, name: 'Test fixture'),
      );
      (await backend.waitFor<void>('scan.stop')).complete(null);
      await backend.waitFor<BackendConnection>('connect');
      final first = backend.completeConnect(device);
      (await backend.waitFor<int>('requestMtu')).complete(185);
      final priority = await backend.waitFor<void>('connection.priority');
      expect(priority.value, [1]);
      priority.complete(null);
      final characteristic = BleCharacteristic(
        serviceUuid: config.service,
        uuid: config.read,
        properties: const BleCharacteristicProperties(0x3e),
      );
      final services = [
        BleService(
          uuid: config.service,
          primary: true,
          characteristics: [characteristic],
        ),
      ];
      (await backend.waitFor<List<BleService>>('discover')).complete(services);
      (await backend.waitFor<Uint8List>('read')).complete(config.expectedRead);
      final enabling = await backend.waitFor<void>('subscribe');
      expect(checks, isNot(contains('subscribed')));
      expect(backend.history, isNot(contains('write')));
      enabling.complete(null);
      final write = await backend.waitFor<void>('write');
      expect(write.value, config.writePayload);
      write.complete(null);
      final writeNoResponse = await backend.waitFor<void>(
        'writeWithoutResponse',
      );
      expect(writeNoResponse.value, config.writeWithoutResponsePayload);
      first.emitNotification(characteristic, config.expectedNotification);
      writeNoResponse.complete(null);
      (await backend.waitFor<void>('unsubscribe')).complete(null);
      (await backend.waitFor<int>('rssi')).complete(-50);
      (await backend.waitFor<int>('getMtu')).complete(23);
      (await backend.waitFor<int>('getMtu')).complete(23);
      (await backend.waitFor<void>('disconnect')).complete(null);
      await backend.waitFor<BackendConnection>('connect');
      backend.completeConnect(device);
      (await backend.waitFor<List<BleService>>('discover')).complete(services);
      (await backend.waitFor<Uint8List>('read')).complete(config.expectedRead);
      (await backend.waitFor<void>('disconnect')).complete(null);
      await completed;
      expect(backend.pending, isEmpty);
      expect(backend.scanning, isFalse);
      expect(backend.liveConnections, 0);
      expect(backend.subscriptions, 0);
      expect(evidence['subscribed'], {'setupMode': 'standard'});
      expect(evidence['getWritePayloadLimit'], {'bytes': 20});
      expect(evidence['requestMtu'], {'requested': 247, 'effective': 185});
      expect(evidence['requestConnectionPriority'], {'requested': 'high'});
      expect(
        checks,
        containsAllInOrder([
          'read',
          'subscribed',
          'writeWithResponse',
          'writeWithoutResponse',
          'notificationReceived',
          'notificationListenerCancelled',
          'readRssi',
          'getMtu',
          'getWritePayloadLimit',
          'disconnected',
          'reconnected',
          'closed',
        ]),
      );
    },
  );

  test(
    'wrong expected read fails and closes all owned resources before writes',
    () async {
      final backend = FakeBleBackend();
      final checks = <String>[];
      final run = runHardwareScenario(
        Ble(backend: backend),
        HardwareConfig.fromMap(configuration()),
        selected: Future.value(
          BleAdvertisement(
            deviceId: const BleDeviceId('fixture'),
            name: 'Test',
          ),
        ),
        record: (check, _) => checks.add(check),
      );
      final failure = expectLater(run, throwsStateError);
      await backend.waitFor<BackendConnection>('connect');
      backend.completeConnect(const BleDeviceId('fixture'));
      final characteristic = BleCharacteristic(
        serviceUuid: BleUuid('180f'),
        uuid: BleUuid('2a19'),
        properties: const BleCharacteristicProperties(0x3e),
      );
      (await backend.waitFor<List<BleService>>('discover')).complete([
        BleService(
          uuid: BleUuid('180f'),
          primary: true,
          characteristics: [characteristic],
        ),
      ]);
      (await backend.waitFor<Uint8List>('read'))
          .complete(Uint8List.fromList([99]));
      await failure;
      expect(backend.liveConnections, 0);
      expect(backend.pending, isEmpty);
      expect(backend.subscriptions, 0);
      expect(backend.history, isNot(contains('write')));
      expect(checks, contains('discoveredServices'));
      expect(checks, isNot(contains('closed')));
    },
  );
}
