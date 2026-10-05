/// Small central-mode recipes; configure UUIDs/payloads for your own peripheral.
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';

/// Owns only its scan lease. Timeout also cancels the lease and awaits stop.
Future<BleAdvertisement> findFirst(
  Ble ble, {
  BleScanFilter filter = const BleScanFilter(),
  Duration timeout = const Duration(seconds: 15),
}) async {
  final scan = StreamIterator(ble.scan(filter: filter));
  try {
    if (!await scan.moveNext().timeout(timeout)) {
      throw StateError('Scan ended without a device');
    }
    return scan.current;
  } finally {
    await scan.cancel();
  }
}

/// Owns and closes [ble], including on failure. Subscribe before the write that
/// triggers a notification. Selection must identify your controlled peripheral;
/// browser callers invoke requestDevice from a user gesture before calling this.
/// Returns the read and first notification without logging private payloads.
Future<({Uint8List read, Uint8List notification})> runGattSession(
  Ble ble,
  Future<BleAdvertisement> selection, {
  required BleUuid serviceUuid,
  required BleUuid readUuid,
  required BleUuid writeUuid,
  required BleUuid notifyUuid,
  required Uint8List payload,
  bool withResponse = true,
  Duration notificationTimeout = const Duration(seconds: 10),
}) async {
  // Selection can fail while initialization is still pending. Observe it now;
  // awaiting it below still propagates its error once the engine is ready.
  selection.ignore();
  try {
    await ble.ready;
    final device = await selection;
    final connection = await ble.connect(device.deviceId);
    try {
      final services = await connection.discoverServices();
      final service = services.singleWhere((s) => s.uuid == serviceUuid);
      BleCharacteristic attribute(BleUuid uuid) =>
          service.characteristics.singleWhere((c) => c.uuid == uuid);
      final read = await connection.read(attribute(readUuid));
      final owner = await connection.enableNotifications(attribute(notifyUuid));
      try {
        final received = Completer<Uint8List>();
        received.future.ignore();
        final listener = owner.values.listen(
          (value) {
            if (!received.isCompleted) received.complete(value);
          },
          onError: (Object error, StackTrace stack) {
            if (!received.isCompleted) received.completeError(error, stack);
          },
          onDone: () {
            if (!received.isCompleted) {
              received.completeError(
                StateError('Disconnected before notification'),
              );
            }
          },
        );
        try {
          await connection.write(
            attribute(writeUuid),
            payload,
            withResponse: withResponse,
          );
          return (
            read: read,
            notification: await received.future.timeout(notificationTimeout),
          );
        } finally {
          await listener.cancel();
        }
      } finally {
        await owner.cancel();
      }
    } finally {
      await connection.disconnect();
    }
  } finally {
    await ble.close();
  }
}
