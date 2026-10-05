@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:bletide/src/native/wire.dart';
import 'package:test/test.dart';

void main() {
  test('shared Rust native error fixture preserves separate platform code', () {
    final bytes = File('test/fixtures/native_error.bin').readAsBytesSync();
    final code = ByteData.sublistView(bytes).getUint32(12, Endian.little);
    final cause = decodeNativeError(
      code,
      Uint8List.sublistView(bytes, 16),
      platform: 'controlled',
      operation: 'read',
      generation: 41,
    );
    expect(cause.code, BleErrorCode.unknown);
    expect(cause.message, 'Controlled native platform failure');
    expect(cause.context.nativeCode, '133');
    expect(cause.context.nativeMessage, cause.message);
    expect(cause.context.operation, 'read');
    expect(cause.context.connectionGeneration, 41);
    for (var length = 0; length < 7; length++) {
      expect(
        () => decodeNativeError(
          code,
          Uint8List.sublistView(bytes, 16, 16 + length),
          platform: 'controlled',
          operation: 'read',
        ),
        throwsFormatException,
      );
    }
    for (final bad in [
      [255, 255, 255, 255],
      [0, 0, 0, 0],
      [1, 0, 0, 0, 255],
    ]) {
      expect(
        () => decodeNativeError(
          code,
          Uint8List.fromList(bad),
          platform: 'controlled',
          operation: 'read',
        ),
        throwsFormatException,
      );
    }
    expect(
      () => decodeNativeError(
        0x80000000,
        Uint8List.sublistView(bytes, 16),
        platform: 'controlled',
        operation: 'read',
      ),
      throwsFormatException,
    );
  });
  final fixture = File('test/fixtures/advertisement.bin').readAsBytesSync();
  test('shared Rust/Dart advertisement fixture preserves every field', () {
    final advertisement = decodeAdvertisement(fixture);
    expect(advertisement.deviceId.value, 'opaque-device');
    expect(advertisement.name, 'Test 🌍');
    expect(advertisement.rssi, -73);
    expect(advertisement.diagnosticAddress, isNull);
    expect(advertisement.connectable, isNull);
    final uuid = BleUuid('12345678-9abc-def0-1234-56789abcdef0');
    expect(advertisement.serviceUuids, [uuid]);
    expect(advertisement.manufacturerData[65535], [0, 255, 128]);
    expect(advertisement.serviceData[uuid], [1, 0, 254]);
    fixture.fillRange(0, fixture.length, 0);
    expect(advertisement.manufacturerData[65535], [0, 255, 128]);
  });
  test('every truncated fixture and trailing bytes are rejected', () {
    final bytes = File('test/fixtures/advertisement.bin').readAsBytesSync();
    for (var length = 0; length < bytes.length; length++) {
      expect(
        () => decodeAdvertisement(Uint8List.sublistView(bytes, 0, length)),
        throwsFormatException,
        reason: 'length=$length',
      );
    }
    expect(
      () => decodeAdvertisement(Uint8List.fromList([...bytes, 0])),
      throwsFormatException,
    );
  });
  test(
    'invalid flags and hostile collection counts fail before allocation',
    () {
      final reader = NativeReader(Uint8List.fromList([2]));
      expect(reader.present, throwsFormatException);
      final lengths = NativeReader(Uint8List.fromList([255, 255, 255, 255]));
      expect(() => lengths.count(16), throwsFormatException);
      expect(
        () => NativeReader(Uint8List.fromList([1, 0, 0, 0, 255])).string(),
        throwsFormatException,
      );
    },
  );
}
