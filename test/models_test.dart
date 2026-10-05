import 'dart:math';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:test/test.dart';

void main() {
  test('UUID aliases normalize and have canonical equality and hash', () {
    for (final input in [
      '180F',
      '0000180F',
      '0000180F-0000-1000-8000-00805F9B34FB',
    ]) {
      expect(BleUuid(input), BleUuid('180f'));
      expect(BleUuid(input).hashCode, BleUuid('180f').hashCode);
    }
  });
  test('generated 16-bit/32-bit UUID aliases preserve canonical identity', () {
    final random = Random(42);
    for (var i = 0; i < 10000; i++) {
      final short = random.nextInt(65536).toRadixString(16).padLeft(4, '0');
      expect(BleUuid(short), BleUuid('0000$short'));
      final long = random
          .nextInt(0x100000000)
          .toRadixString(16)
          .padLeft(8, '0');
      expect(BleUuid(long), BleUuid('$long-0000-1000-8000-00805f9b34fb'));
    }
  });
  test('malformed UUIDs are rejected early', () {
    for (final input in [
      '',
      '180',
      ' 180f',
      '180f ',
      '0x180f',
      'z800',
      '0000180f00001000800000805f9b34fb',
      '0000180f-0000-1000-8000-00805f9b34f',
    ]) {
      expect(() => BleUuid(input), throwsFormatException);
    }
  });
  test(
    'advertisements copy all binary values and expose immutable collections',
    () {
      final value = Uint8List.fromList([0, 255]);
      final ad = BleAdvertisement(
        deviceId: const BleDeviceId('opaque'),
        manufacturerData: {42: value},
        serviceData: {BleUuid('180f'): value},
      );
      value[0] = 9;
      expect(ad.manufacturerData[42], [0, 255]);
      expect(ad.serviceData[BleUuid('180f')], [0, 255]);
      expect(() => ad.manufacturerData[42]![0] = 1, throwsUnsupportedError);
      expect(
        () => ad.serviceUuids.add(BleUuid('180f')),
        throwsUnsupportedError,
      );
    },
  );
  test('portable scan filtering combines name and advertised services', () {
    final filter = BleScanFilter(
      namePrefix: 'Test',
      serviceUuids: [BleUuid('180f')],
    );
    expect(
      filter.matches(
        BleAdvertisement(
          deviceId: const BleDeviceId('a'),
          name: 'Test 1',
          serviceUuids: [BleUuid('180f')],
        ),
      ),
      isTrue,
    );
    expect(
      filter.matches(
        BleAdvertisement(deviceId: const BleDeviceId('b'), name: 'Test 1'),
      ),
      isFalse,
    );
  });
}
