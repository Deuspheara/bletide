import 'dart:async';
import 'dart:typed_data';

import 'package:bletide/bletide.dart';
import 'package:test/test.dart';

final class GattContractFixture {
  GattContractFixture(
    this.ble,
    this.connect, {
    required this.writeIssued,
    this.cleanup,
  });
  final Ble ble;
  final Future<BleConnection> Function() connect;
  final Future<void> Function()? cleanup;
  final Future<void> Function() writeIssued;
}

/// Portable façade assertions; factories adapt only peripheral transport/setup.
/// Every peripheral starts with [0,255,128], echoes successful writes via notify,
/// rejects [0xee] as an ordinary GATT error and holds [0xff] after signalling
/// writeIssued. Each reconnect starts fresh.
void registerGattContracts(
  String backend,
  Future<GattContractFixture> Function() create,
) {
  group('shared GATT contract / $backend', () {
    late GattContractFixture fixture;
    setUp(() async {
      fixture = await create();
      await fixture.ble.ready;
    });
    tearDown(() async {
      try {
        await fixture.ble.close();
      } finally {
        await fixture.cleanup?.call();
      }
    });
    Future<(BleConnection, BleCharacteristic)> discovered() async {
      final connection = await fixture.connect();
      final char =
          (await connection.discoverServices()).single.characteristics.single;
      expect(char.serviceUuid, BleUuid('180f'));
      expect(char.uuid, BleUuid('2a19'));
      return (connection, char);
    }

    TypeMatcher<BleException> error(BleErrorCode code) =>
        isA<BleException>().having((e) => e.code, 'code', code);

    test(
      'disconnect cancels an issued write and rejects queued work',
      () async {
        final (connection, char) = await discovered();
        final issued = fixture.writeIssued();
        final write = connection.write(char, Uint8List.fromList([0xff]));
        final interrupted = expectLater(
          write,
          throwsA(error(BleErrorCode.disconnected)),
        );
        await issued;
        final read = connection.read(char);
        final skipped = expectLater(
          read,
          throwsA(error(BleErrorCode.disconnected)),
        );
        await connection.disconnect();
        await interrupted;
        await skipped;
        final (next, fresh) = await discovered();
        expect(next.generation, greaterThan(connection.generation));
        expect(await next.read(fresh), [0, 255, 128]);
        await expectLater(
          connection.read(char),
          throwsA(error(BleErrorCode.disconnected)),
        );
        await next.disconnect();
      },
    );

    test(
      'close during an issued write rejects queued work and closes the facade',
      () async {
        final (connection, char) = await discovered();
        final issued = fixture.writeIssued();
        final write = connection.write(char, Uint8List.fromList([0xff]));
        final interrupted = expectLater(
          write,
          throwsA(error(BleErrorCode.disconnected)),
        );
        await issued;
        final read = connection.read(char);
        final skipped = expectLater(
          read,
          throwsA(error(BleErrorCode.disconnected)),
        );
        final closing = fixture.ble.close();
        expect(identical(closing, fixture.ble.close()), isTrue);
        await closing;
        await interrupted;
        await skipped;
        await expectLater(
          fixture.ble.connect(connection.deviceId),
          throwsA(error(BleErrorCode.disposed)),
        );
      },
    );

    test(
      'copied write bytes, both modes, descriptor round trip and FIFO',
      () async {
        final (connection, char) = await discovered();
        expect(await connection.read(char), [0, 255, 128]);
        final bytes = Uint8List.fromList([7, 0, 128, 255]);
        final prior = connection.write(char, Uint8List.fromList([6]));
        final first = connection.write(char, bytes);
        bytes.fillRange(0, bytes.length, 0);
        final middle = connection.read(char);
        final last = connection.write(
          char,
          Uint8List.fromList([9]),
          withResponse: false,
        );
        await prior;
        await first;
        expect(await middle, [7, 0, 128, 255]);
        await last;
        expect(await connection.read(char), [9]);
        final descriptor = char.descriptors.single;
        await connection.writeDescriptor(
          descriptor,
          Uint8List.fromList([1, 0, 255]),
        );
        expect(await connection.readDescriptor(descriptor), [1, 0, 255]);
      },
    );

    test(
      'write diagnostics omit payload bytes and Dart output stays silent',
      () async {
        final (connection, char) = await discovered();
        const marker = 'PAYLOAD_ONLY_42';
        final value = Uint8List.fromList([...marker.codeUnits, 0, 255, 128]);
        final diagnostics = <BleDiagnostic>[];
        final listener = fixture.ble.diagnostics.listen(diagnostics.add);
        addTearDown(listener.cancel);
        final printed = <String>[];
        final completed = fixture.ble.diagnostics.firstWhere(
          (event) => event.name == 'gatt.read.completed',
        );
        await runZoned(
          () async {
            await connection.write(char, value);
            await connection.write(char, value, withResponse: false);
            expect(await connection.read(char), value);
          },
          zoneSpecification: ZoneSpecification(
            print: (self, parent, zone, line) => printed.add(line),
          ),
        );
        await completed;
        expect(
          printed,
          isEmpty,
          reason: 'GATT must not print payloads by default',
        );
        final hex = value
            .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
            .join();
        for (final event in diagnostics) {
          final metadata = [
            event.name,
            event.operation,
            event.errorCode,
            event.nativeMessage,
          ].join(' ');
          for (final encoded in [marker, value.toString(), hex]) {
            expect(
              metadata.contains(encoded),
              isFalse,
              reason: 'Diagnostic contains write payload',
            );
          }
        }
        expect(
          diagnostics.where((event) => event.name == 'gatt.write.completed'),
          hasLength(2),
        );
      },
    );

    test('ordinary failure preserves queued reads and writes', () async {
      final (connection, char) = await discovered();
      final diagnostics = <BleDiagnostic>[];
      final listener = fixture.ble.diagnostics.listen(diagnostics.add);
      addTearDown(listener.cancel);
      final failed = expectLater(
        connection.write(char, Uint8List.fromList([0xee])),
        throwsA(error(BleErrorCode.gattFailure)),
      );
      final read = connection.read(char);
      final write = connection.write(char, Uint8List.fromList([8]));
      await failed;
      expect(await read, [0, 255, 128]);
      await write;
      final finalReadDiagnostic = fixture.ble.diagnostics.firstWhere(
        (event) =>
            event.name == 'gatt.read.completed' &&
            event.generation == connection.generation,
      );
      expect(await connection.read(char), [8]);
      // Public results can precede delivery on the asynchronous diagnostics
      // stream. Wait for its terminal event rather than an arbitrary delay.
      await finalReadDiagnostic;
      final gattEvents = diagnostics.where(
        (event) => event.operation == 'read' || event.operation == 'write',
      );
      final started = gattEvents.where(
        (event) => event.name.endsWith('.started'),
      );
      final terminal = gattEvents.where(
        (event) => !event.name.endsWith('.started'),
      );
      expect(started.map((event) => event.operation), [
        'write',
        'read',
        'write',
        'read',
      ]);
      expect(terminal.map((event) => event.name), [
        'gatt.write.failed',
        'gatt.read.completed',
        'gatt.write.completed',
        'gatt.read.completed',
      ]);
      expect(terminal.first.errorCode, 'gattFailure');
      for (final event in gattEvents) {
        expect(event.deviceId, connection.deviceId);
        expect(event.generation, connection.generation);
      }
      expect(started.every((event) => event.duration == null), isTrue);
      expect(
        terminal.every(
          (event) => event.duration != null && !event.duration!.isNegative,
        ),
        isTrue,
      );
    });

    test('notification owners share setup and one cancellation preserves the other', () async {
      final (connection, char) = await discovered();
      final first = <List<int>>[], second = <List<int>>[];
      final aReady = Completer<void>(),
          bReady = Completer<void>(),
          bNext = Completer<void>();
      final a = connection.subscribe(char).listen((value) {
        first.add(value.toList());
        if (!aReady.isCompleted) aReady.complete();
      });
      final b = connection.subscribe(char).listen((value) {
        second.add(value.toList());
        if (!bReady.isCompleted) bReady.complete();
        if (second.length == 2) bNext.complete();
      });
      await connection.read(char); // FIFO barrier after subscription setup.
      await connection.write(char, Uint8List.fromList([1, 255]));
      await Future.wait([aReady.future, bReady.future]);
      await a.cancel();
      await connection.write(char, Uint8List.fromList([2, 128]));
      await bNext.future;
      expect(first, [
        [1, 255],
      ]);
      expect(second, [
        [1, 255],
        [2, 128],
      ]);
      await b.cancel();
      await connection.read(char); // FIFO barrier after final unsubscribe.
      await connection.write(char, Uint8List.fromList([3]));
      await connection.read(char);
      expect(first, hasLength(1));
      expect(second, hasLength(2));
    });

    test(
      'paused notification listener resumes while generation remains connected',
      () async {
        final (connection, char) = await discovered();
        final received = Completer<Uint8List>();
        final listener = connection.subscribe(char).listen(received.complete);
        listener.pause();
        final witnessed = Completer<void>();
        final active = connection.subscribe(char).listen((_) {
          if (!witnessed.isCompleted) witnessed.complete();
        });
        await connection.read(char);
        await connection.write(char, Uint8List.fromList([0, 128, 255]));
        await witnessed.future;
        expect(received.isCompleted, isFalse);
        listener.resume();
        final value = await received.future;
        expect(value, [0, 128, 255]);
        expect(() => value[0] = 9, throwsUnsupportedError);
        await listener.cancel();
        await active.cancel();
      },
    );

    for (final engineClose in [false, true]) {
      test(
        'buffered notifications are discarded after generation closes; engine=$engineClose',
        () async {
          final (connection, char) = await discovered();
          final values = <Uint8List>[];
          final done = Completer<void>();
          final paused = connection
              .subscribe(char)
              .listen(values.add, onDone: done.complete);
          paused.pause();
          final received = Completer<Uint8List>();
          final active = connection.subscribe(char).listen((value) {
            if (!received.isCompleted) received.complete(value);
          });
          await connection.read(char); // FIFO setup barrier.
          await connection.write(char, Uint8List.fromList([42, 0, 255]));
          expect(await received.future, [42, 0, 255]);
          // The active owner proves delivery while the other owner remains paused.
          if (engineClose) {
            await fixture.ble.close();
          } else {
            await connection.disconnect();
            final (next, fresh) = await discovered();
            expect(next.generation, greaterThan(connection.generation));
            expect(await next.read(fresh), [0, 255, 128]);
            await next.write(fresh, Uint8List.fromList([88]));
          }
          paused.resume();
          await done.future;
          expect(
            values,
            isEmpty,
            reason: 'Retired generation must not deliver buffered data',
          );
          await paused.cancel();
          await active.cancel();
        },
      );
    }

    test(
      'disconnect is idempotent and reconnect creates a fresh generation',
      () async {
        final (connection, char) = await discovered();
        await connection.write(char, Uint8List.fromList([4]));
        await connection.disconnect();
        await connection.disconnect();
        await expectLater(
          connection.read(char),
          throwsA(error(BleErrorCode.disconnected)),
        );
        final (next, fresh) = await discovered();
        expect(next.generation, greaterThan(connection.generation));
        expect(identical(next, connection), isFalse);
        expect(await next.read(fresh), [0, 255, 128]);
        await next.disconnect();
      },
    );
  });
}
