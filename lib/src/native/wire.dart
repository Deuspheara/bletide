import 'dart:convert';
import 'dart:typed_data';

import '../models.dart';
import '../errors.dart';

/// Bounds-checked decoding of the native binary format. No native pointers.
final class NativeReader {
  NativeReader(this.bytes);
  final Uint8List bytes;
  int _offset = 0;
  int get remaining => bytes.length - _offset;
  Uint8List take(int length) {
    if (length < 0 || length > remaining) {
      throw const FormatException('Truncated native payload');
    }
    final result = Uint8List.sublistView(bytes, _offset, _offset + length);
    _offset += length;
    return result;
  }

  int u8() => take(1)[0];
  int u16() => ByteData.sublistView(take(2)).getUint16(0, Endian.little);
  int i16() => ByteData.sublistView(take(2)).getInt16(0, Endian.little);
  int u32() => ByteData.sublistView(take(4)).getUint32(0, Endian.little);
  Uint8List blob() => take(u32());
  String string() => utf8.decode(blob());
  bool present() => switch (u8()) {
    0 => false,
    1 => true,
    _ => throw const FormatException('Invalid native presence flag'),
  };
  String? optionalString() => present() ? string() : null;
  BleUuid uuid() {
    final hex = take(16).map((n) => n.toRadixString(16).padLeft(2, '0')).join();
    return BleUuid(
      '${hex.substring(0, 8)}-${hex.substring(8, 12)}-'
      '${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}',
    );
  }

  int count(int minimumBytes) {
    final count = u32();
    if (count > remaining ~/ minimumBytes) {
      throw const FormatException('Invalid native collection length');
    }
    return count;
  }

  void finish() {
    if (remaining != 0) throw const FormatException('Trailing native payload');
  }
}

BleAdvertisement decodeAdvertisement(Uint8List payload) {
  final reader = NativeReader(payload);
  final id = reader.string();
  if (id.isEmpty) throw const FormatException('Empty native device identifier');
  final name = reader.optionalString();
  final rssi = reader.present() ? reader.i16() : null;
  final address = reader.optionalString();
  final services = <BleUuid>[];
  final serviceCount = reader.count(16);
  for (var i = 0; i < serviceCount; i++) {
    services.add(reader.uuid());
  }
  final manufacturer = <int, Uint8List>{};
  final manufacturerCount = reader.count(6);
  for (var i = 0; i < manufacturerCount; i++) {
    final id = reader.u16();
    if (manufacturer.containsKey(id)) {
      throw const FormatException('Duplicate manufacturer entry');
    }
    manufacturer[id] = reader.blob();
  }
  final serviceData = <BleUuid, Uint8List>{};
  final dataCount = reader.count(20);
  for (var i = 0; i < dataCount; i++) {
    final uuid = reader.uuid();
    if (serviceData.containsKey(uuid)) {
      throw const FormatException('Duplicate service entry');
    }
    serviceData[uuid] = reader.blob();
  }
  reader.finish();
  return BleAdvertisement(
    deviceId: BleDeviceId(id),
    name: name,
    rssi: rssi,
    diagnosticAddress: address,
    serviceUuids: services,
    manufacturerData: manufacturer,
    serviceData: serviceData,
  );
}

final class NativeWriter {
  final BytesBuilder _bytes = BytesBuilder(copy: false);
  void u64(int value) {
    final bytes = ByteData(8)..setUint64(0, value, Endian.little);
    _bytes.add(bytes.buffer.asUint8List());
  }

  void uuid(BleUuid value) {
    final hex = value.value.replaceAll('-', '');
    _bytes.add(
      Uint8List.fromList(
        List.generate(
          16,
          (i) => int.parse(hex.substring(i * 2, i * 2 + 2), radix: 16),
        ),
      ),
    );
  }

  void bytes(Uint8List value) => _bytes.add(Uint8List.fromList(value));
  Uint8List finish() => _bytes.takeBytes();
}

List<BleService> decodeServices(Uint8List payload) {
  final reader = NativeReader(payload);
  final services = <BleService>[];
  final count = reader.count(21);
  for (var i = 0; i < count; i++) {
    final service = reader.uuid();
    final primary = reader.present();
    final characteristics = <BleCharacteristic>[];
    final characteristicCount = reader.count(21);
    for (var j = 0; j < characteristicCount; j++) {
      final uuid = reader.uuid();
      final properties = BleCharacteristicProperties(reader.u8());
      final descriptors = <BleDescriptor>[];
      final descriptorCount = reader.count(16);
      for (var k = 0; k < descriptorCount; k++) {
        descriptors.add(
          BleDescriptor(
            serviceUuid: service,
            characteristicUuid: uuid,
            uuid: reader.uuid(),
          ),
        );
      }
      characteristics.add(
        BleCharacteristic(
          serviceUuid: service,
          uuid: uuid,
          properties: properties,
          descriptors: descriptors,
        ),
      );
    }
    services.add(
      BleService(
        uuid: service,
        primary: primary,
        characteristics: characteristics,
      ),
    );
  }
  reader.finish();
  return List.unmodifiable(services);
}

/// ABI v2 optional native-code envelope. Unflagged errors retain their v1 layout.
BleException decodeNativeError(
  int encodedCode,
  Uint8List payload, {
  required String platform,
  required String operation,
  int? generation,
}) {
  final hasNativeCode = encodedCode & 0x80000000 != 0;
  final code = encodedCode & 0x7fffffff;
  if (code == 0) {
    throw const FormatException('Missing native error classification');
  }
  String? nativeCode;
  Uint8List messageBytes = payload;
  if (hasNativeCode) {
    final reader = NativeReader(payload);
    nativeCode = reader.string();
    if (nativeCode.isEmpty) {
      throw const FormatException('Empty native error code');
    }
    messageBytes = reader.take(reader.remaining);
  }
  final message = utf8.decode(messageBytes, allowMalformed: true);
  return BleException(
    code <= BleErrorCode.values.length
        ? BleErrorCode.values[code - 1]
        : BleErrorCode.unknown,
    message,
    context: BleErrorContext(
      platform: platform,
      operation: operation,
      connectionGeneration: generation,
      nativeCode: nativeCode ?? '$code',
      nativeMessage: message,
    ),
  );
}
