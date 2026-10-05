import 'dart:async';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:bletide/src/native/event_bridge.dart';
import 'package:bletide/src/native/native_backend.dart';
import 'package:test/test.dart';

import 'gatt_contract.dart';

@Native<Int64 Function(Int64, Pointer<Void>)>(
  symbol: 'bletide_open_fixture',
  assetId: 'package:bletide/src/native/bindings.dart',
)
external int _openFixture(int port, Pointer<Void> post);

const _device = BleDeviceId('bletide-fixture');
TypeMatcher<BleException> _error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);

void registerNativeFixtureContracts(
  int Function() resourceCounts,
  List<int> Function() workerResources,
) {
  group('Dart → FFI → production Rust workers / injected peripheral', () {
    late NativeEventBridge bridge;
    late NativeBleBackend backend;
    late int baseline;
    late List<int> workers;
    setUp(() async {
      baseline = resourceCounts();
      workers = workerResources();
      bridge = NativeEventBridge(openEngine: _openFixture);
      backend = NativeBleBackend(
        transport: bridge,
        timeouts: const BleTimeouts(
          connect: Duration(seconds: 5),
          discovery: Duration(seconds: 5),
          write: Duration(milliseconds: 500),
        ),
      );
      await backend.ready;
    });
    tearDown(() async {
      await backend.close();
      expect(bridge.pendingCount, 0);
      expect(resourceCounts(), baseline);
      expect(workerResources(), workers);
    });
    registerGattContracts('native FFI', () async {
      final ble = Ble(backend: backend);
      return GattContractFixture(
        ble,
        () => ble.connect(_device),
        writeIssued: () async {
          await bridge.events.firstWhere(
            (event) =>
                ByteData.sublistView(event).getUint32(0, Endian.little) == 8 &&
                event[16] == 0xff,
          );
        },
      );
    });

    Future<void> control(List<int> payload) async {
      await bridge
          .submit(
            60,
            Uint8List.fromList(payload),
            timeout: const Duration(seconds: 5),
          )
          .result;
    }

    Future<Uint8List> started(int operation) => bridge.events.firstWhere(
      (event) =>
          ByteData.sublistView(event).getUint32(0, Endian.little) == 8 &&
          event[16] == operation,
    );

    for (final failure in {
      1: BleErrorCode.adapterUnavailable,
      2: BleErrorCode.adapterDisabled,
      3: BleErrorCode.permissionDenied,
      8: BleErrorCode.disconnected,
    }.entries) {
      test(
        'GATT ${failure.value.name} retires native generation and rejects following requests',
        () async {
          final connection = await backend.connect(_device).result;
          final characteristic = (await connection.discoverServices().result)
              .single
              .characteristics
              .single;
          final disconnected = connection.disconnected.first;
          final write = connection.write(
            characteristic,
            Uint8List.fromList([0xed, failure.key]),
            true,
          );
          final writeFailure = expectLater(
            write.result,
            throwsA(
              _error(failure.value)
                  .having(
                    (error) => error.context.nativeCode,
                    'native code',
                    '${failure.key}',
                  )
                  .having(
                    (error) => error.context.connectionGeneration,
                    'generation',
                    connection.generation,
                  ),
            ),
          );
          final queued = connection.read(characteristic);
          final queuedFailure = expectLater(
            queued.result,
            throwsA(_error(BleErrorCode.disconnected)),
          );
          await writeFailure;
          await queuedFailure;
          await disconnected;
          await expectLater(
            connection.read(characteristic).result,
            throwsA(_error(BleErrorCode.disconnected)),
          );
          await backend.close();
          expect(bridge.pendingCount, 0);
          expect(workerResources(), workers);
          expect(resourceCounts(), baseline);
        },
      );
    }

    test(
      'cancelled issued connect releases ownership before immediate reconnect',
      () async {
        await control([2, 0]);
        final issued = started(30);
        final connecting = backend.connect(_device);
        final cancelled = expectLater(
          connecting.result,
          throwsA(_error(BleErrorCode.cancelled)),
        );
        await issued;
        await expectLater(
          backend.connect(_device).result,
          throwsA(_error(BleErrorCode.connectInProgress)),
        );
        connecting.cancel();
        await cancelled;
        final nextIssued = started(30);
        final next = backend.connect(_device);
        await Future.any([
          nextIssued,
          next.result.then(
            (_) => throw StateError('Connect completed before release'),
          ),
        ]);
        await control([2, 1]);
        final connection = await next.result;
        await connection.disconnect().result;
      },
    );

    test('cancelled discovery discards released old work and fresh generation discovers normally', () async {
      final connection = await backend.connect(_device).result;
      await control([3, 0]);
      final issued = started(40);
      final discovery = connection.discoverServices();
      final cancelled = expectLater(
        discovery.result,
        throwsA(_error(BleErrorCode.cancelled)),
      );
      await issued;
      discovery.cancel();
      await cancelled;
      await connection.disconnect().result;
      await control([3, 1]);
      final next = await backend.connect(_device).result;
      expect(next.generation, greaterThan(connection.generation));
      final char =
          (await next.discoverServices().result).single.characteristics.single;
      expect(await next.read(char).result, [0, 255, 128]);
      await next.disconnect().result;
    });

    test('adapter loss interrupts an issued connect and recovery requires a new attempt', () async {
      await control([2, 0]);
      final issued = started(30);
      final connecting = backend.connect(_device);
      final disconnected = expectLater(
        connecting.result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await issued;
      final disabled = backend.adapterState.firstWhere(
        (state) => state == BleAdapterState.disabled,
      );
      await control([1, 2]);
      await disabled;
      await disconnected;
      await expectLater(
        backend.connect(_device).result,
        throwsA(_error(BleErrorCode.adapterDisabled)),
      );
      final ready = backend.adapterState.firstWhere(
        (state) => state == BleAdapterState.ready,
      );
      await control([1, 4]);
      await ready;
      final nextIssued = started(30);
      final next = backend.connect(_device);
      await Future.any([
        nextIssued,
        next.result.then(
          (_) => throw StateError('Connect completed before release'),
        ),
      ]);
      await control([2, 1]);
      await (await next.result).disconnect().result;
    });

    test('permission loss during discovery rejects queued work and recovery cannot revive old generation', () async {
      final connection = await backend.connect(_device).result;
      final char = (await connection.discoverServices().result)
          .single
          .characteristics
          .single;
      await control([3, 0]);
      final issued = started(40);
      final discovery = connection.discoverServices();
      final disconnected = expectLater(
        discovery.result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await issued;
      final queued = connection.read(char);
      final skipped = expectLater(
        queued.result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      final unauthorized = backend.adapterState.firstWhere(
        (state) => state == BleAdapterState.unauthorized,
      );
      await control([1, 3]);
      await unauthorized;
      await disconnected;
      await skipped;
      await connection.disconnect().result;
      await expectLater(
        backend.connect(_device).result,
        throwsA(_error(BleErrorCode.permissionDenied)),
      );
      await control([3, 1]);
      final ready = backend.adapterState.firstWhere(
        (state) => state == BleAdapterState.ready,
      );
      await control([1, 4]);
      await ready;
      final next = await backend.connect(_device).result;
      expect(next.generation, greaterThan(connection.generation));
      await next.discoverServices().result;
      await expectLater(
        connection.read(char).result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await next.disconnect().result;
    });

    test('notification transport cause reaches listeners, diagnostics and issued GATT', () async {
      final ble = Ble(backend: backend);
      final connection = await ble.connect(_device);
      final char =
          (await connection.discoverServices()).single.characteristics.single;
      final diagnostic = ble.diagnostics.firstWhere(
        (event) => event.name == 'notification.failed',
      );
      final errors = Completer<BleException>();
      final subscription = connection
          .subscribe(char)
          .listen(
            (_) {},
            onError: (Object error) {
              if (!errors.isCompleted) errors.complete(error as BleException);
            },
          );
      // FIFO read completion proves subscribe setup has acknowledged.
      await connection.read(char);
      final issued = started(0xff);
      final running = connection.write(char, Uint8List.fromList([0xff]));
      final failed = expectLater(
        running,
        throwsA(
          _error(BleErrorCode.gattFailure).having(
            (e) => e.context.nativeMessage,
            'native cause',
            'Controlled notification transport failure',
          ),
        ),
      );
      await issued;
      await control([6]);
      await failed;
      final error = await errors.future;
      expect(error.code, BleErrorCode.gattFailure);
      expect(error.context.deviceId, _device);
      expect(error.context.connectionGeneration, connection.generation);
      expect(error.context.operation, 'notification');
      expect(
        error.context.nativeMessage,
        'Controlled notification transport failure',
      );
      final event = await diagnostic;
      expect(event.errorCode, 'gattFailure');
      expect(event.nativeMessage, 'Controlled notification transport failure');
      expect(event.generation, connection.generation);
      await subscription.cancel();
      await ble.close();
    });

    test('notification transport loss interrupts issued GATT and joins its generation', () async {
      final idle = workerResources();
      final connection = await backend.connect(_device).result;
      final char = (await connection.discoverServices().result)
          .single
          .characteristics
          .single;
      final issued = started(0xff);
      final running = connection.write(char, Uint8List.fromList([0xff]), true);
      final failed = expectLater(
        running.result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await issued;
      final queued = expectLater(
        connection.read(char).result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await control([5, 0]);
      await failed;
      await queued;
      await connection.disconnect().result;
      expect(workerResources(), idle);
      await control([5, 1]);
      final next = await backend.connect(_device).result;
      expect(next.generation, greaterThan(connection.generation));
      final fresh =
          (await next.discoverServices().result).single.characteristics.single;
      expect(await next.read(fresh).result, [0, 255, 128]);
      await expectLater(
        connection.read(char).result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await next.disconnect().result;
      expect(workerResources(), idle);
    });

    test('100 rapid adapter loss/recovery cycles invalidate issued work without automatic reconnect', () async {
      final idle = workerResources();
      for (var cycle = 0; cycle < 100; cycle++) {
        final connection = await backend.connect(_device).result;
        final char = (await connection.discoverServices().result)
            .single
            .characteristics
            .single;
        final issued = started(0xff);
        final write = connection.write(char, Uint8List.fromList([0xff]), true);
        final interrupted = expectLater(
          write.result,
          throwsA(_error(BleErrorCode.disconnected)),
        );
        await issued;
        final states = <BleAdapterState>[];
        final recovered = Completer<void>();
        final events = backend.adapterState.listen((state) {
          states.add(state);
          if (states.length == 2) recovered.complete();
        });
        await control([
          4,
          2,
          4,
        ]); // Both transitions queued before the adapter worker polls events.
        await recovered.future;
        await interrupted;
        expect(states, [BleAdapterState.disabled, BleAdapterState.ready]);
        await events.cancel();
        await connection.disconnect().result;
        expect(backend.currentAdapterState, BleAdapterState.ready);
        expect(workerResources(), idle);
        await expectLater(
          connection.read(char).result,
          throwsA(_error(BleErrorCode.disconnected)),
        );
      }
    });

    test('native capability gates preserve RSSI and MTU semantics', () async {
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final connection = await ble.connect(_device);
      expect(await connection.getMtu(), 23);
      if (ble.capabilities.readRssi) {
        expect(await connection.readRssi(), -42);
      } else {
        await expectLater(
          connection.readRssi(),
          throwsA(_error(BleErrorCode.notSupported)),
        );
      }
      expect(ble.capabilities.requestMtu, isFalse);
      await expectLater(
        connection.requestMtu(64),
        throwsA(_error(BleErrorCode.notSupported)),
      );
      await connection.disconnect();
    });

    test(
      'duplicate connect, FIFO, ordinary failure and scoped errors cross FFI',
      () async {
        final connection = await backend.connect(_device).result;
        await expectLater(
          backend.connect(_device).result,
          throwsA(_error(BleErrorCode.alreadyConnected)),
        );
        final characteristic = (await connection.discoverServices().result)
            .single
            .characteristics
            .single;
        final first = connection.write(
          characteristic,
          Uint8List.fromList([3]),
          true,
        );
        final read = connection.read(characteristic);
        final last = connection.write(
          characteristic,
          Uint8List.fromList([4]),
          false,
        );
        await first.result;
        expect(await read.result, [3]);
        await last.result;
        final failed = connection.write(
          characteristic,
          Uint8List.fromList([0xee]),
          true,
        );
        await expectLater(
          failed.result,
          throwsA(
            _error(BleErrorCode.gattFailure)
                .having(
                  (e) => e.context.serviceUuid,
                  'service',
                  characteristic.serviceUuid,
                )
                .having(
                  (e) => e.context.connectionGeneration,
                  'generation',
                  connection.generation,
                ),
          ),
        );
        expect(await connection.read(characteristic).result, [4]);
        final missing = BleCharacteristic(
          serviceUuid: characteristic.serviceUuid,
          uuid: BleUuid('1234'),
          properties: characteristic.properties,
        );
        await expectLater(
          connection.read(missing).result,
          throwsA(_error(BleErrorCode.characteristicNotFound)),
        );
        expect(await connection.read(characteristic).result, [4]);
        await connection.disconnect().result;
      },
    );

    test('running cancellation skips queued operations and reconnect gets a fresh generation', () async {
      final connection = await backend.connect(_device).result;
      final characteristic = (await connection.discoverServices().result)
          .single
          .characteristics
          .single;
      final started = bridge.events.firstWhere(
        (event) => ByteData.sublistView(event).getUint32(0, Endian.little) == 8,
      );
      final stalled = connection.write(
        characteristic,
        Uint8List.fromList([0xff]),
        true,
      );
      final failed = expectLater(
        stalled.result,
        throwsA(_error(BleErrorCode.cancelled)),
      );
      await started;
      final queued = connection.write(
        characteristic,
        Uint8List.fromList([7]),
        true,
      );
      final skipped = expectLater(
        queued.result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      stalled.cancel();
      await failed;
      await skipped;
      await connection
          .disconnect()
          .result; // Acknowledgement joins the old worker.
      final next = await backend.connect(_device).result;
      expect(next.generation, greaterThan(connection.generation));
      final fresh =
          (await next.discoverServices().result).single.characteristics.single;
      expect(await next.read(fresh).result, [0, 255, 128]);
      await expectLater(
        connection.read(characteristic).result,
        throwsA(_error(BleErrorCode.disconnected)),
      );
      await next.disconnect().result;
    });

    test(
      'native GATT timeout closes the issued generation and skips queued work',
      () async {
        final connection = await backend.connect(_device).result;
        final characteristic = (await connection.discoverServices().result)
            .single
            .characteristics
            .single;
        final started = bridge.events.firstWhere(
          (event) =>
              ByteData.sublistView(event).getUint32(0, Endian.little) == 8,
        );
        final stalled = connection.write(
          characteristic,
          Uint8List.fromList([0xff]),
          true,
        );
        final timedOut = expectLater(
          stalled.result,
          throwsA(_error(BleErrorCode.timeout)),
        );
        await started;
        final queued = connection.read(characteristic);
        final skipped = expectLater(
          queued.result,
          throwsA(_error(BleErrorCode.disconnected)),
        );
        await timedOut;
        await skipped;
        await connection.disconnect().result;
        final next = await backend.connect(_device).result;
        expect(next.generation, greaterThan(connection.generation));
        await next.disconnect().result;
      },
    );

    test('engine close interrupts an issued peripheral operation and joins all workers', () async {
      final connection = await backend.connect(_device).result;
      final characteristic = (await connection.discoverServices().result)
          .single
          .characteristics
          .single;
      final started = bridge.events.firstWhere(
        (event) => ByteData.sublistView(event).getUint32(0, Endian.little) == 8,
      );
      final stalled = connection.write(
        characteristic,
        Uint8List.fromList([0xff]),
        true,
      );
      final failed = expectLater(
        stalled.result,
        throwsA(_error(BleErrorCode.disposed)),
      );
      await started;
      await backend.close();
      await failed;
    });

    test('100 native connection and subscription generations return owned resources to baseline', () async {
      final idle = workerResources();
      var previous = 0;
      for (var index = 0; index < 100; index++) {
        final connection = await backend.connect(_device).result;
        expect(connection.generation, greaterThan(previous));
        previous = connection.generation;
        final characteristic = (await connection.discoverServices().result)
            .single
            .characteristics
            .single;
        await connection.subscribe(characteristic).result;
        final active = workerResources();
        expect(active, [idle[0], idle[1] + 1, idle[2] + 1, idle[3] + 1]);
        final notified = connection.notifications.first;
        await connection
            .write(characteristic, Uint8List.fromList([index]), true)
            .result;
        expect((await notified).value, [index]);
        await connection.unsubscribe(characteristic).result;
        await connection.disconnect().result;
        expect(workerResources(), idle);
      }
    });
  });
}
