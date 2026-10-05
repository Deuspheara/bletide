import 'dart:async';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:bletide/testing.dart';

import 'gatt_contract.dart';

Future<GattContractFixture> fakeGattFixture() async {
  const device = BleDeviceId('shared-fake');
  final backend = FakeBleBackend();
  FakeBackendConnection? connection;
  var value = Uint8List.fromList([0, 255, 128]);
  var descriptor = Uint8List(0);
  final issued = Completer<void>();
  final char = BleCharacteristic(
    serviceUuid: BleUuid('180f'),
    uuid: BleUuid('2a19'),
    properties: const BleCharacteristicProperties(0x1e),
    descriptors: [
      BleDescriptor(
        serviceUuid: BleUuid('180f'),
        characteristicUuid: BleUuid('2a19'),
        uuid: BleUuid('2902'),
      ),
    ],
  );
  final operations = backend.operations.listen((operation) {
    if (operation.completed) return;
    switch (operation.name) {
      case 'connect':
        value = Uint8List.fromList([0, 255, 128]);
        descriptor = Uint8List(0);
        connection = backend.completeConnect(device);
      case 'discover':
        operation.complete([
          BleService(
            uuid: char.serviceUuid,
            primary: true,
            characteristics: [char],
          ),
        ]);
      case 'read':
        operation.complete(Uint8List.fromList(value));
      case 'write' || 'writeWithoutResponse':
        final bytes = operation.value!;
        if (bytes.length == 1 && bytes[0] == 0xff) {
          issued.complete(); // Leave the explicit fake platform operation unresolved.
        } else if (bytes.length == 1 && bytes[0] == 0xee) {
          operation.fail(
            const BleException(
              BleErrorCode.gattFailure,
              'Controlled peripheral write failure',
            ),
          );
        } else {
          value = Uint8List.fromList(bytes);
          operation.complete(null);
          connection!.emitNotification(char, value);
        }
      case 'readDescriptor':
        operation.complete(Uint8List.fromList(descriptor));
      case 'writeDescriptor':
        descriptor = Uint8List.fromList(operation.value!);
        operation.complete(null);
      case 'subscribe' || 'unsubscribe' || 'disconnect':
        operation.complete(null);
      default:
        operation.fail(
          BleException(
            BleErrorCode.notSupported,
            'Unexpected shared-fixture operation ${operation.name}',
          ),
        );
    }
  });
  final ble = Ble(backend: backend);
  return GattContractFixture(
    ble,
    () => ble.connect(device),
    writeIssued: () => issued.future,
    cleanup: operations.cancel,
  );
}
