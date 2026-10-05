import 'dart:async';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';

import 'hardware_config.dart';

/// Owns and closes [ble], including on assertion, timeout, or platform failure.
/// [selected] allows a browser harness to invoke its chooser from a user gesture
/// before running this shared scenario. Native tests instead discover by scan.
Future<void> runHardwareScenario(
  Ble ble,
  HardwareConfig config, {
  required void Function(String check, Map<String, Object?> evidence) record,
  Future<BleAdvertisement>? selected,
}) async {
  BleConnection? connection;
  BleNotificationSubscription? notificationOwner;
  StreamSubscription<Uint8List>? notifications;
  try {
    await ble.ready;
    record('initialized', {'adapter': (await ble.adapterState.first).name});
    final advertisement =
        await (selected ??
                ble
                    .scan(filter: BleScanFilter(namePrefix: config.namePrefix))
                    .first)
            .timeout(config.timeout);
    if (!(advertisement.name?.startsWith(config.namePrefix) ?? false)) {
      throw StateError(
        'Selected peripheral does not match configured name prefix',
      );
    }
    record('discovered', {
      'deviceId': advertisement.deviceId.value,
      'name': advertisement.name,
      'hardware': config.hardware,
    });
    connection = await ble.connect(advertisement.deviceId);
    final firstGeneration = connection.generation;
    record('connected', {'generation': firstGeneration});
    if (config.requestedMtu case final requested?) {
      if (ble.capabilities.requestMtu) {
        final effective = await connection.requestMtu(requested);
        if (effective < 23 || effective > 517) {
          throw StateError('Invalid requested ATT MTU result $effective');
        }
        record('requestMtu', {'requested': requested, 'effective': effective});
      } else {
        record('requestMtuUnsupported', {'requested': requested});
      }
    }
    if (config.connectionPriority case final priority?) {
      if (ble.capabilities.requestConnectionPriority) {
        await connection.requestConnectionPriority(priority);
        record('requestConnectionPriority', {'requested': priority.name});
      } else {
        record('requestConnectionPriorityUnsupported', {
          'requested': priority.name,
        });
      }
    }
    final services = await connection.discoverServices();
    final service = services.singleWhere(
      (s) => s.uuid == config.service,
      orElse: () => throw StateError('Configured service was not discovered'),
    );
    BleCharacteristic characteristic(BleUuid uuid) =>
        service.characteristics.singleWhere(
          (c) => c.uuid == uuid,
          orElse: () =>
              throw StateError('Configured characteristic $uuid missing'),
        );
    final read = characteristic(config.read);
    final write = characteristic(config.write);
    final writeNoResponse = characteristic(config.writeWithoutResponse);
    final notify = characteristic(config.notify);
    if (!read.properties.read ||
        !write.properties.write ||
        !writeNoResponse.properties.writeWithoutResponse ||
        !(notify.properties.notify || notify.properties.indicate)) {
      throw StateError(
        'Peripheral lacks required read/write/notification properties',
      );
    }
    record('discoveredServices', {
      'services': services.length,
      'characteristics': service.characteristics.length,
    });
    _equal(await connection.read(read), config.expectedRead, 'initial read');
    record('read', {});

    final received = Completer<Uint8List>();
    received.future.ignore();
    notificationOwner = await connection.enableNotifications(
      notify,
      setupMode: config.notificationSetupMode,
    );
    notifications = notificationOwner.values.listen(
      (bytes) {
        if (!received.isCompleted) received.complete(bytes);
      },
      onError: (Object error, StackTrace stack) {
        if (!received.isCompleted) received.completeError(error, stack);
      },
      onDone: () {
        if (!received.isCompleted) {
          received.completeError(
            StateError('Notification stream closed before a value arrived'),
          );
        }
      },
    );
    record('subscribed', {'setupMode': config.notificationSetupMode.name});
    await connection.write(write, config.writePayload);
    record('writeWithResponse', {});
    await connection.write(
      writeNoResponse,
      config.writeWithoutResponsePayload,
      withResponse: false,
    );
    record('writeWithoutResponse', {});
    _equal(
      await received.future.timeout(config.timeout),
      config.expectedNotification,
      'notification',
    );
    record('notificationReceived', {});
    await notifications.cancel();
    notifications = null;
    await notificationOwner.cancel();
    notificationOwner = null;
    record('notificationListenerCancelled', {});
    if (ble.capabilities.readRssi) {
      record('readRssi', {'rssi': await connection.readRssi()});
    } else {
      record('readRssiUnsupported', {});
    }
    if (ble.capabilities.getMtu) {
      final mtu = await connection.getMtu();
      if (mtu < 23) throw StateError('Invalid ATT MTU snapshot $mtu');
      record('getMtu', {'mtu': mtu});
      record('getWritePayloadLimit', {
        'bytes': await connection.getWritePayloadLimit(),
      });
    } else {
      record('getMtuUnsupported', {});
      record('getWritePayloadLimitUnsupported', {});
    }
    await connection.disconnect();
    await connection.disconnect();
    record('disconnected', {'state': connection.state.name});
    connection = await ble.connect(advertisement.deviceId);
    if (connection.generation <= firstGeneration) {
      throw StateError('Reconnect reused an old generation');
    }
    final nextServices = await connection.discoverServices();
    final nextRead = nextServices
        .singleWhere((s) => s.uuid == config.service)
        .characteristics
        .singleWhere((c) => c.uuid == config.read);
    await connection.read(nextRead);
    record('reconnected', {'generation': connection.generation});
    await connection.disconnect();
    connection = null;
  } finally {
    try {
      await notifications?.cancel();
    } finally {
      try {
        await notificationOwner?.cancel();
      } finally {
        // Engine close owns any still-connected generation and pending operation.
        await ble.close();
      }
    }
  }
  record('closed', {});
}

void _equal(Uint8List actual, Uint8List expected, String operation) {
  if (actual.length != expected.length ||
      Iterable<int>.generate(actual.length)
          .any((i) => actual[i] != expected[i])) {
    // Test reports omit payload contents, even on failed comparisons.
    throw StateError('$operation did not match the configured expected bytes');
  }
}
