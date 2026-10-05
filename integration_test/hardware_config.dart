import 'dart:typed_data';

import 'package:bletide/bletide.dart';

/// Test-only configuration. Writes require explicit payloads supplied by the
/// operator; there are no device protocols or default write commands here.
final class HardwareConfig {
  HardwareConfig.fromMap(Map<String, String> values)
    : hardware = _required(values, 'BLE_HARDWARE'),
      namePrefix = _required(values, 'BLE_NAME_PREFIX'),
      service = BleUuid(_required(values, 'BLE_SERVICE')),
      read = BleUuid(_required(values, 'BLE_READ')),
      write = BleUuid(_required(values, 'BLE_WRITE')),
      writeWithoutResponse = BleUuid(
        _required(values, 'BLE_WRITE_NO_RESPONSE'),
      ),
      notify = BleUuid(_required(values, 'BLE_NOTIFY')),
      expectedRead = _hex(values, 'BLE_EXPECTED_READ_HEX'),
      writePayload = _hex(values, 'BLE_WRITE_HEX'),
      writeWithoutResponsePayload = _hex(values, 'BLE_WRITE_NO_RESPONSE_HEX'),
      expectedNotification = _hex(values, 'BLE_EXPECTED_NOTIFICATION_HEX'),
      timeout = _timeout(values),
      requestedMtu = _requestedMtu(values),
      connectionPriority = _priority(values),
      notificationSetupMode = _notificationSetupMode(values);

  factory HardwareConfig.fromEnvironment() => HardwareConfig.fromMap({
    'BLE_HARDWARE': const String.fromEnvironment('BLE_HARDWARE'),
    'BLE_NAME_PREFIX': const String.fromEnvironment('BLE_NAME_PREFIX'),
    'BLE_SERVICE': const String.fromEnvironment('BLE_SERVICE'),
    'BLE_READ': const String.fromEnvironment('BLE_READ'),
    'BLE_WRITE': const String.fromEnvironment('BLE_WRITE'),
    'BLE_WRITE_NO_RESPONSE': const String.fromEnvironment(
      'BLE_WRITE_NO_RESPONSE',
    ),
    'BLE_NOTIFY': const String.fromEnvironment('BLE_NOTIFY'),
    'BLE_EXPECTED_READ_HEX': const String.fromEnvironment(
      'BLE_EXPECTED_READ_HEX',
    ),
    'BLE_WRITE_HEX': const String.fromEnvironment('BLE_WRITE_HEX'),
    'BLE_WRITE_NO_RESPONSE_HEX': const String.fromEnvironment(
      'BLE_WRITE_NO_RESPONSE_HEX',
    ),
    'BLE_EXPECTED_NOTIFICATION_HEX': const String.fromEnvironment(
      'BLE_EXPECTED_NOTIFICATION_HEX',
    ),
    'BLE_TIMEOUT_SECONDS': const String.fromEnvironment('BLE_TIMEOUT_SECONDS'),
    'BLE_REQUEST_MTU': const String.fromEnvironment('BLE_REQUEST_MTU'),
    'BLE_NOTIFY_SETUP_MODE': const String.fromEnvironment(
      'BLE_NOTIFY_SETUP_MODE',
    ),
    'BLE_CONNECTION_PRIORITY': const String.fromEnvironment(
      'BLE_CONNECTION_PRIORITY',
    ),
  });

  final String hardware, namePrefix;
  final BleUuid service, read, write, writeWithoutResponse, notify;
  final Uint8List expectedRead,
      writePayload,
      writeWithoutResponsePayload,
      expectedNotification;
  final Duration timeout;
  final int? requestedMtu;
  final BleConnectionPriority? connectionPriority;
  final BleNotificationSetupMode notificationSetupMode;

  static BleNotificationSetupMode _notificationSetupMode(
    Map<String, String> values,
  ) {
    final raw = values['BLE_NOTIFY_SETUP_MODE']?.trim();
    return switch (raw) {
      null || '' || 'standard' => BleNotificationSetupMode.standard,
      'compat' => BleNotificationSetupMode.compat,
      _ => throw const FormatException(
        'BLE_NOTIFY_SETUP_MODE must be standard or compat',
      ),
    };
  }

  static int? _requestedMtu(Map<String, String> values) {
    final raw = values['BLE_REQUEST_MTU']?.trim();
    if (raw == null || raw.isEmpty) return null;
    final mtu = int.tryParse(raw);
    if (mtu == null || mtu < 23 || mtu > 517) {
      throw const FormatException('BLE_REQUEST_MTU must be between 23 and 517');
    }
    return mtu;
  }

  static BleConnectionPriority? _priority(Map<String, String> values) {
    final raw = values['BLE_CONNECTION_PRIORITY']?.trim();
    if (raw == null || raw.isEmpty) return null;
    for (final priority in BleConnectionPriority.values) {
      if (priority.name == raw) return priority;
    }
    throw const FormatException(
      'BLE_CONNECTION_PRIORITY must be balanced, high or lowPower',
    );
  }

  static String _required(Map<String, String> values, String key) {
    final value = values[key]?.trim();
    if (value == null || value.isEmpty || value.startsWith('REPLACE_')) {
      throw FormatException('Hardware configuration requires $key');
    }
    return value;
  }

  static Uint8List _hex(Map<String, String> values, String key) {
    final value = _required(values, key).replaceAll(RegExp(r'\s+'), '');
    if (value.length.isOdd || !RegExp(r'^[0-9a-fA-F]+$').hasMatch(value)) {
      throw FormatException('$key must contain complete hexadecimal bytes');
    }
    return Uint8List.fromList([
      for (var i = 0; i < value.length; i += 2)
        int.parse(value.substring(i, i + 2), radix: 16),
    ]).asUnmodifiableView();
  }

  static Duration _timeout(Map<String, String> values) {
    final raw = values['BLE_TIMEOUT_SECONDS'];
    if (raw == null || raw.isEmpty) return const Duration(seconds: 30);
    final seconds = int.tryParse(raw);
    if (seconds == null || seconds < 1 || seconds > 300) {
      throw const FormatException(
        'BLE_TIMEOUT_SECONDS must be between 1 and 300',
      );
    }
    return Duration(seconds: seconds);
  }
}
