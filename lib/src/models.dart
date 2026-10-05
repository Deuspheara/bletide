import 'dart:typed_data';

extension type const BleDeviceId(String value) {}

/// Canonical Bluetooth identity, including 16-bit and 32-bit aliases.
final class BleUuid {
  factory BleUuid(String input) {
    var value = input.toLowerCase();
    if (RegExp(r'^[0-9a-f]{4}$').hasMatch(value)) {
      value = '0000$value-0000-1000-8000-00805f9b34fb';
    } else if (RegExp(r'^[0-9a-f]{8}$').hasMatch(value)) {
      value = '$value-0000-1000-8000-00805f9b34fb';
    } else if (!RegExp(
      r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    ).hasMatch(value)) {
      throw FormatException('Malformed Bluetooth UUID', input);
    }
    return BleUuid._(value);
  }
  const BleUuid._(this.value);
  final String value;
  @override
  bool operator ==(Object other) => other is BleUuid && value == other.value;
  @override
  int get hashCode => value.hashCode;
  @override
  String toString() => value;
}

enum BleAdapterState { unsupported, unavailable, disabled, unauthorized, ready }

enum BleConnectionState { connecting, connected, disconnecting, disconnected }

/// Requests are OS hints; success does not confirm a negotiated interval.
enum BleConnectionPriority { balanced, high, lowPower }

final class BleCapabilities {
  const BleCapabilities({
    this.scan = false,
    this.connect = false,
    this.gatt = false,
    this.descriptorAccess = false,
    this.readRssi = false,
    this.getMtu = false,
    this.requestMtu = false,
    this.requestConnectionPriority = false,
    this.manufacturerData = false,
    this.serviceData = false,
    this.continuousScan = false,
    this.adapterState = false,
    this.requestDevice = false,
  });
  final bool scan,
      connect,
      gatt,
      descriptorAccess,
      readRssi,
      getMtu,
      requestMtu,
      requestConnectionPriority,
      manufacturerData,
      serviceData,
      continuousScan,
      adapterState,
      requestDevice;
}

final class BleTimeouts {
  const BleTimeouts({
    this.connect = const Duration(seconds: 20),
    this.discovery = const Duration(seconds: 15),
    this.read = const Duration(seconds: 10),
    this.write = const Duration(seconds: 10),
    this.subscribe = const Duration(seconds: 10),
    this.disconnect = const Duration(seconds: 5),
  });
  final Duration connect, discovery, read, write, subscribe, disconnect;
}

final class BleAdvertisement {
  BleAdvertisement({
    required this.deviceId,
    this.name,
    this.rssi,
    this.connectable,
    Iterable<BleUuid> serviceUuids = const [],
    Map<int, Uint8List> manufacturerData = const {},
    Map<BleUuid, Uint8List> serviceData = const {},
    this.diagnosticAddress,
  }) : serviceUuids = List.unmodifiable(serviceUuids),
       manufacturerData = Map.unmodifiable(
         manufacturerData.map(
           (key, value) =>
               MapEntry(key, Uint8List.fromList(value).asUnmodifiableView()),
         ),
       ),
       serviceData = Map.unmodifiable(
         serviceData.map(
           (key, value) =>
               MapEntry(key, Uint8List.fromList(value).asUnmodifiableView()),
         ),
       );
  final BleDeviceId deviceId;
  final String? name, diagnosticAddress;
  final int? rssi;
  final bool? connectable;
  final List<BleUuid> serviceUuids;
  final Map<int, Uint8List> manufacturerData;
  final Map<BleUuid, Uint8List> serviceData;
}

/// Browser chooser filters and service permissions, separate from scanning.
/// Services match any UUID; optionalServices grant access without filtering.
final class BleDeviceRequest {
  BleDeviceRequest({
    this.namePrefix,
    Iterable<BleUuid> serviceUuids = const [],
    Iterable<BleUuid> optionalServices = const [],
  }) : serviceUuids = List.unmodifiable(serviceUuids.toSet()),
       optionalServices = List.unmodifiable(optionalServices.toSet()) {
    if (namePrefix != null && namePrefix!.isEmpty) {
      throw const FormatException('Device name prefix must not be empty');
    }
  }
  final String? namePrefix;
  final List<BleUuid> serviceUuids, optionalServices;
}

final class BleScanFilter {
  const BleScanFilter({this.namePrefix, this.serviceUuids = const []});
  final String? namePrefix;
  final List<BleUuid> serviceUuids;
  bool matches(BleAdvertisement advertisement) =>
      (namePrefix == null ||
          (advertisement.name?.startsWith(namePrefix!) ?? false)) &&
      (serviceUuids.isEmpty ||
          serviceUuids.any(advertisement.serviceUuids.contains));
}

/// Notification setup mode for a single characteristic subscription.
/// Standard behavior is the default on every platform.
enum BleNotificationSetupMode {
  standard,

  /// For notify-capable peripherals with a nonstandard CCCD.
  /// Android enables local routing without writing the CCCD. Apple still calls
  /// setNotifyValue and tolerates only CBATTErrorDomain code 10 in its callback.
  /// Other platforms return unsupported. Setup success does not prove delivery.
  compat,
}

final class BleCharacteristicProperties {
  const BleCharacteristicProperties(this.bits);
  final int bits;
  bool get read => bits & 0x02 != 0;
  bool get writeWithoutResponse => bits & 0x04 != 0;
  bool get write => bits & 0x08 != 0;
  bool get notify => bits & 0x10 != 0;
  bool get indicate => bits & 0x20 != 0;
}

final class BleDescriptor {
  const BleDescriptor({
    required this.serviceUuid,
    required this.characteristicUuid,
    required this.uuid,
  });
  final BleUuid serviceUuid, characteristicUuid, uuid;
}

final class BleCharacteristic {
  BleCharacteristic({
    required this.serviceUuid,
    required this.uuid,
    required this.properties,
    Iterable<BleDescriptor> descriptors = const [],
  }) : descriptors = List.unmodifiable(descriptors);
  final BleUuid serviceUuid, uuid;
  final BleCharacteristicProperties properties;
  final List<BleDescriptor> descriptors;
  String get key => '$serviceUuid/$uuid';
}

final class BleService {
  BleService({
    required this.uuid,
    required this.primary,
    Iterable<BleCharacteristic> characteristics = const [],
  }) : characteristics = List.unmodifiable(characteristics);
  final BleUuid uuid;
  final bool primary;
  final List<BleCharacteristic> characteristics;
}

final class BleDiagnostic {
  BleDiagnostic(
    this.name, {
    this.deviceId,
    this.generation,
    this.operation,
    this.errorCode,
    this.nativeMessage,
    this.duration,
  }) : timestamp = DateTime.now().toUtc();
  final DateTime timestamp;
  final String name;
  final BleDeviceId? deviceId;
  final int? generation;
  final String? operation, errorCode, nativeMessage;

  /// Time from GATT execution starting to its public terminal result.
  /// Excludes waiting in the queue and any later physical cleanup.
  final Duration? duration;
}
