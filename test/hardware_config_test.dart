import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';
import 'package:bletide/bletide.dart';

import '../integration_test/hardware_config.dart';

Map<String, String> configuration() => {
  'BLE_HARDWARE': 'controlled fixture firmware 1',
  'BLE_NAME_PREFIX': 'Test',
  'BLE_SERVICE': '180f',
  'BLE_READ': '2a19',
  'BLE_WRITE': '2a19',
  'BLE_WRITE_NO_RESPONSE': '2a19',
  'BLE_NOTIFY': '2a19',
  'BLE_EXPECTED_READ_HEX': '00 ff 80',
  'BLE_WRITE_HEX': '01',
  'BLE_WRITE_NO_RESPONSE_HEX': '02',
  'BLE_EXPECTED_NOTIFICATION_HEX': '03',
};

void main() {
  test('hardware config canonicalizes UUIDs and freezes explicit bytes', () {
    final config = HardwareConfig.fromMap(configuration());
    expect(config.service.value, '0000180f-0000-1000-8000-00805f9b34fb');
    expect(config.expectedRead, [0, 255, 128]);
    expect(() => config.writePayload[0] = 9, throwsUnsupportedError);
    expect(config.timeout, const Duration(seconds: 30));
    expect(config.requestedMtu, isNull);
    expect(config.connectionPriority, isNull);
    expect(config.notificationSetupMode, BleNotificationSetupMode.standard);
  });
  test('every required field fails before hardware use when missing', () {
    for (final key in configuration().keys) {
      final values = configuration()..remove(key);
      expect(
        () => HardwareConfig.fromMap(values),
        throwsFormatException,
        reason: key,
      );
    }
  });
  test('invalid UUID, payload and deadline reject configuration', () {
    for (final (key, value) in [
      ('BLE_SERVICE', 'invalid'),
      ('BLE_WRITE_HEX', '1'),
      ('BLE_WRITE_HEX', 'gg'),
      ('BLE_WRITE_HEX', ''),
      ('BLE_WRITE_HEX', 'REPLACE_WITH_SAFE_BYTES'),
      ('BLE_TIMEOUT_SECONDS', '0'),
      ('BLE_TIMEOUT_SECONDS', '301'),
      ('BLE_TIMEOUT_SECONDS', 'unknown'),
      ('BLE_REQUEST_MTU', '22'),
      ('BLE_REQUEST_MTU', '518'),
      ('BLE_REQUEST_MTU', 'unknown'),
      ('BLE_CONNECTION_PRIORITY', 'urgent'),
      ('BLE_NOTIFY_SETUP_MODE', 'auto'),
    ]) {
      expect(
        () => HardwareConfig.fromMap(configuration()..[key] = value),
        throwsFormatException,
        reason: '$key=$value',
      );
    }
  });
  test('optional radio requests validate boundary MTUs and priority names', () {
    for (final mtu in [23, 247, 517]) {
      for (final priority in ['balanced', 'high', 'lowPower']) {
        final config = HardwareConfig.fromMap(
          configuration()
            ..['BLE_REQUEST_MTU'] = ' $mtu '
            ..['BLE_CONNECTION_PRIORITY'] = priority,
        );
        expect(config.requestedMtu, mtu);
        expect(config.connectionPriority?.name, priority);
      }
    }
  });
  test('notification setup mode is explicit and defaults to standard', () {
    for (final (value, expected) in [
      ('', BleNotificationSetupMode.standard),
      (' standard ', BleNotificationSetupMode.standard),
      (' compat ', BleNotificationSetupMode.compat),
    ]) {
      final config = HardwareConfig.fromMap(
        configuration()..['BLE_NOTIFY_SETUP_MODE'] = value,
      );
      expect(config.notificationSetupMode, expected);
    }
  });
  test('checked-in hardware template cannot issue default writes', () {
    final values = (jsonDecode(
      File('integration_test/hardware.example.json').readAsStringSync(),
    ) as Map<String, dynamic>).cast<String, String>();
    expect(() => HardwareConfig.fromMap(values), throwsFormatException);
  });
}
