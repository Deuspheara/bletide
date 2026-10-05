@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:bletide/bletide.dart';
import 'package:bletide/src/backend.dart';
import 'package:bletide/src/native/event_bridge.dart';
import 'package:bletide/src/native/native_backend.dart';
import 'package:bletide/src/native/wire.dart';
import 'package:test/test.dart';

final class Pending {
  Pending(this.code, this.payload, this.timeout);
  final int code;
  final Duration timeout;
  final Uint8List payload;
  final result = Completer<Uint8List>();
  bool cancelled = false;
}

final class Transport implements NativeTransport {
  final controller = StreamController<Uint8List>.broadcast(sync: true);
  final pending = <Pending>[];
  bool closed = false;
  Completer<void>? shutdown;
  Object? initializationFailure;
  final closeStarted = Completer<void>();
  @override
  Stream<Uint8List> get events => controller.stream;
  @override
  NativeRequest submit(
    int operation,
    Uint8List payload, {
    required Duration timeout,
    String? operationName,
  }) {
    if (operation == 10) {
      if (initializationFailure != null) {
        return NativeRequest(Future.error(initializationFailure!), () {});
      }
      return NativeRequest(Future.value(Uint8List.fromList([4])), () {});
    }
    final request = Pending(operation, Uint8List.fromList(payload), timeout);
    pending.add(request);
    return NativeRequest(request.result.future, () => request.cancelled = true);
  }

  Pending next(int code) {
    final request = pending.removeAt(0);
    expect(request.code, code);
    return request;
  }

  void emit(int kind, int generation, Uint8List payload, {int code = 0}) {
    final header = ByteData(16)
      ..setUint32(0, kind, Endian.little)
      ..setUint64(4, generation, Endian.little)
      ..setUint32(12, code, Endian.little);
    controller.add(
      Uint8List.fromList([...header.buffer.asUint8List(), ...payload]),
    );
  }

  @override
  Future<void> close() async {
    closed = true;
    await controller.close();
    if (!closeStarted.isCompleted) closeStarted.complete();
    if (shutdown != null) await shutdown!.future;
  }
}

TypeMatcher<BleException> error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);
Uint8List generation(int id) => (NativeWriter()..u64(id)).finish();
Future<BackendConnection> connect(
  NativeBleBackend backend,
  Transport transport,
  int id,
) async {
  final connecting = backend.connect(const BleDeviceId('opaque-device'));
  final call = transport.next(30);
  expect(utf8.decode(call.payload), 'opaque-device');
  call.result.complete(generation(id));
  return connecting.result;
}

void main() {
  late Transport transport;
  late NativeBleBackend backend;
  setUp(() async {
    transport = Transport();
    backend = NativeBleBackend(
      transport: transport,
      timeouts: const BleTimeouts(
        read: Duration(seconds: 31),
        write: Duration(seconds: 32),
      ),
    );
    await backend.ready;
  });
  tearDown(() => backend.close());
  test(
    'scan callback wire errors preserve classification and OS message',
    () async {
      for (final entry in [
        (1, 16, BleErrorCode.invalidState),
        (4, 11, BleErrorCode.notSupported),
        (3, 19, BleErrorCode.unknown),
        (99, 19, BleErrorCode.unknown),
      ]) {
        final cause = 'Android scan failed: code ${entry.$1}';
        final failure = expectLater(
          backend.advertisements.first,
          throwsA(
            isA<BleException>()
                .having((e) => e.code, 'classification', entry.$3)
                .having((e) => e.message, 'message', cause)
                .having((e) => e.context.operation, 'operation', 'scan')
                .having(
                  (e) => e.context.nativeMessage,
                  'native message',
                  cause,
                ),
          ),
        );
        transport.emit(
          4,
          0,
          Uint8List.fromList(utf8.encode(cause)),
          code: entry.$2,
        );
        await failure;
        expect(transport.closed, isFalse);
      }
      final retry = backend.startScan();
      transport.next(20).result.complete(Uint8List(0));
      await retry.result;
      final stopped = backend.stopScan();
      transport.next(21).result.complete(Uint8List(0));
      await stopped.result;
    },
  );

  test(
    'unlistened notification errors are bounded and release their owner',
    () async {
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final connecting = ble.connect(const BleDeviceId('opaque-device'));
      await Future<void>(() {});
      transport.next(30).result.complete(generation(77));
      final connection = await connecting;
      final characteristic = BleCharacteristic(
        serviceUuid: BleUuid('180f'),
        uuid: BleUuid('2a19'),
        properties: const BleCharacteristicProperties(0x10),
      );
      final enabling = connection.enableNotifications(characteristic);
      await Future<void>(() {});
      transport.next(44).result.complete(Uint8List(0));
      final owner = await enabling;
      for (var i = 0; i < 300; i++) {
        transport.emit(
          6,
          77,
          Uint8List.fromList(utf8.encode('native failure $i')),
          code: 15,
        );
      }
      await Future<void>(() {});
      expect(transport.pending.map((request) => request.code), [45]);
      transport.next(45).result.complete(Uint8List(0));
      await owner.cancel();
      final errors = <BleException>[];
      await owner.values.handleError((Object failure) {
        errors.add(failure as BleException);
      }).drain<void>();
      expect(errors, hasLength(257));
      expect(errors.first.message, 'native failure 0');
      expect(errors[255].message, 'native failure 255');
      expect(errors.last.context.operation, 'notification.buffer');
      expect(connection.state, BleConnectionState.connected);
    },
  );

  test('consumed notification errors release buffer capacity', () async {
    final ble = Ble(backend: backend);
    addTearDown(ble.close);
    final connecting = ble.connect(const BleDeviceId('opaque-device'));
    await Future<void>(() {});
    transport.next(30).result.complete(generation(77));
    final connection = await connecting;
    final characteristic = BleCharacteristic(
      serviceUuid: BleUuid('180f'),
      uuid: BleUuid('2a19'),
      properties: const BleCharacteristicProperties(0x10),
    );
    final enabling = connection.enableNotifications(characteristic);
    await Future<void>(() {});
    transport.next(44).result.complete(Uint8List(0));
    final owner = await enabling;
    final errors = <Object>[];
    final listener = owner.values.listen((_) {}, onError: errors.add);
    addTearDown(listener.cancel);
    for (var i = 0; i < 300; i++) {
      transport.emit(
        6,
        77,
        Uint8List.fromList(utf8.encode('native failure $i')),
        code: 15,
      );
      await Future<void>(() {});
    }
    expect(errors, hasLength(300));
    expect(transport.pending, isEmpty);
    final stopping = owner.cancel();
    await Future<void>(() {});
    transport.next(45).result.complete(Uint8List(0));
    await stopping;
  });

  test('explicit notification policy waits for ACK, owns teardown, and rejects mixed owners', () async {
    if (!Platform.isAndroid && !Platform.isIOS && !Platform.isMacOS) return;
    final ble = Ble(backend: backend);
    final connecting = ble.connect(const BleDeviceId('opaque-device'));
    await Future<void>(() {});
    transport.next(30).result.complete(generation(77));
    final connection = await connecting;
    final characteristic = BleCharacteristic(
      serviceUuid: BleUuid('180f'),
      uuid: BleUuid('2a19'),
      properties: const BleCharacteristicProperties(0x10),
    );
    const policy = BleNotificationSetupMode.compat;
    final enabling = connection.enableNotifications(
      characteristic,
      setupMode: policy,
    );
    var enabled = false;
    enabling.then((_) => enabled = true);
    await Future<void>(() {});
    final start = transport.next(44);
    expect(start.payload.sublist(40), [1]);
    expect(enabled, isFalse);
    await expectLater(
      connection.enableNotifications(characteristic),
      throwsA(error(BleErrorCode.invalidState)),
    );
    start.result.complete(Uint8List(0));
    final owner = await enabling;
    final shared = await connection.enableNotifications(
      characteristic,
      setupMode: policy,
    );
    final packet = owner.values.first;
    final value = Uint8List.fromList([7, 8]);
    transport.emit(
      6,
      77,
      (NativeWriter()
            ..uuid(characteristic.serviceUuid)
            ..uuid(characteristic.uuid)
            ..bytes(value))
          .finish(),
    );
    expect(await packet, value);
    // .first cancels only this owner; the shared owner still retains setup.
    await owner.cancel();
    expect(transport.pending, isEmpty);
    final stopping = shared.cancel();
    await Future<void>(() {});
    final stop = transport.next(45);
    expect(stop.payload.sublist(40), [1]);
    stop.result.complete(Uint8List(0));
    await stopping;
    final standard = connection.enableNotifications(characteristic);
    await Future<void>(() {});
    final restart = transport.next(44);
    expect(restart.payload.length, 40, reason: 'Existing API remains strict');
    restart.result.complete(Uint8List(0));
    final fresh = await standard;
    final releasing = fresh.cancel();
    await Future<void>(() {});
    transport.next(45).result.complete(Uint8List(0));
    await releasing;
    await ble.close();
  });

  test(
    'compatibility cancellation keeps its policy through cleanup before retry',
    () async {
      if (!Platform.isAndroid && !Platform.isIOS && !Platform.isMacOS) return;
      final ble = Ble(backend: backend);
      final connecting = ble.connect(const BleDeviceId('opaque-device'));
      await Future<void>(() {});
      transport.next(30).result.complete(generation(79));
      final connection = await connecting;
      final characteristic = BleCharacteristic(
        serviceUuid: BleUuid('180f'),
        uuid: BleUuid('2a19'),
        properties: const BleCharacteristicProperties(0x10),
      );
      final token = BleCancellation();
      final enabling = connection.enableNotifications(
        characteristic,
        setupMode: BleNotificationSetupMode.compat,
        cancellation: token,
      );
      await Future<void>(() {});
      final setup = transport.next(44);
      token.cancel();
      await expectLater(enabling, throwsA(error(BleErrorCode.cancelled)));
      setup.result.complete(Uint8List(0));
      await Future<void>(() {});
      final cleanup = transport.next(45);
      expect(cleanup.payload.sublist(40), [1]);
      cleanup.result.complete(Uint8List(0));
      await Future<void>(() {});
      final retry = connection.enableNotifications(characteristic);
      await Future<void>(() {});
      final strict = transport.next(44);
      expect(strict.payload.length, 40);
      strict.result.complete(Uint8List(0));
      final owner = await retry;
      final releasing = owner.cancel();
      await Future<void>(() {});
      transport.next(45).result.complete(Uint8List(0));
      await releasing;
      await ble.close();
    },
  );

  test('non-notify compatibility fails before native setup', () async {
    final ble = Ble(backend: backend);
    final connecting = ble.connect(const BleDeviceId('opaque-device'));
    await Future<void>(() {});
    transport.next(30).result.complete(generation(78));
    final connection = await connecting;
    final characteristic = BleCharacteristic(
      serviceUuid: BleUuid('180f'),
      uuid: BleUuid('2a19'),
      properties: const BleCharacteristicProperties(0x20),
    );
    await expectLater(
      connection.enableNotifications(
        characteristic,
        setupMode: BleNotificationSetupMode.compat,
      ),
      throwsA(error(BleErrorCode.notSupported)),
    );
    expect(transport.pending, isEmpty);
    await ble.close();
  });

  test(
    'idle native callback error preserves decoded diagnostic cause',
    () async {
      final ble = Ble(backend: backend);
      await ble.ready;
      final diagnostic = ble.diagnostics.firstWhere(
        (event) => event.name == 'backend.error',
      );
      const cause = 'CoreBluetooth callback panicked: injected_native_callback';
      transport.emit(4, 0, Uint8List.fromList(utf8.encode(cause)), code: 18);
      final event = await diagnostic;
      expect(event.errorCode, 'internal');
      expect(event.nativeMessage, cause);
      expect(event.operation, 'scan');
      expect(transport.pending, isEmpty);
      await transport.controller.close();
      await ble.close();
      expect(transport.closed, isTrue);
    },
  );

  test(
    'transport stream error preserves cause and joins automatic shutdown',
    () async {
      transport.shutdown = Completer<void>();
      addTearDown(() {
        if (!transport.shutdown!.isCompleted) transport.shutdown!.complete();
      });
      final connection = await connect(backend, transport, 73);
      const failure = BleException(
        BleErrorCode.internal,
        'Malformed native event',
        context: BleErrorContext(
          operation: 'decodeEvent',
          nativeMessage: 'Malformed native event',
        ),
      );
      final failures = <Object>[];
      final listener = backend.advertisements.listen(
        (_) {},
        onError: failures.add,
      );
      addTearDown(listener.cancel);
      transport.controller.addError(failure, StackTrace.current);
      expect(backend.currentAdapterState, BleAdapterState.unavailable);
      await expectLater(
        backend.connect(const BleDeviceId('another')).result,
        throwsA(error(BleErrorCode.disposed)),
      );
      await expectLater(
        connection.getMtu().result,
        throwsA(error(BleErrorCode.disconnected)),
      );
      await transport.closeStarted.future;
      expect(failures.single, same(failure));
      var joined = false;
      final closing = backend.close().then((_) => joined = true);
      await Future<void>(() {});
      expect(joined, isFalse);
      transport.shutdown!.complete();
      await closing;
      expect(joined, isTrue);
    },
  );
  test(
    'automatic malformed-event cleanup reports and retains shutdown failure',
    () async {
      final transport = Transport()..shutdown = Completer<void>();
      final backend = NativeBleBackend(transport: transport);
      final errors = <Object>[];
      final done = Completer<void>();
      backend.advertisements.listen(
        (_) {},
        onError: errors.add,
        onDone: done.complete,
      );
      await backend.ready;
      transport.controller.add(Uint8List(0));
      await transport.closeStarted.future;
      const failure = BleException(
        BleErrorCode.internal,
        'Native shutdown failed',
      );
      transport.shutdown!.completeError(failure);
      await done.future;
      // Drain this event-loop turn before installing a late explicit close
      // listener, so that listener cannot hide an unowned shutdown error.
      await Future<void>(() {});
      expect(errors.first, error(BleErrorCode.internal));
      expect(errors.last, same(failure));
      await expectLater(backend.close(), throwsA(same(failure)));
      await expectLater(backend.close(), throwsA(same(failure)));
    },
  );

  test('failed initialization owns cleanup failure without replacing the ready error', () async {
    const initial = BleException(
      BleErrorCode.permissionDenied,
      'Permission denied during initialization',
    );
    const failure = BleException(
      BleErrorCode.internal,
      'Native shutdown failed',
    );
    final transport = Transport()
      ..shutdown = Completer<void>()
      ..initializationFailure = initial;
    final backend = NativeBleBackend(transport: transport);
    final errors = <Object>[];
    final done = Completer<void>();
    backend.advertisements.listen(
      (_) {},
      onError: errors.add,
      onDone: done.complete,
    );
    final ready = expectLater(backend.ready, throwsA(same(initial)));
    await transport.closeStarted.future;
    transport.shutdown!.completeError(failure);
    await ready;
    await done.future;
    await Future<void>(() {});
    expect(errors.single, same(failure));
    await expectLater(backend.close(), throwsA(same(failure)));
  });

  for (final length in [0, 1, 3, 4, 8, 12, 15]) {
    test(
      'truncated native event header $length reports a typed error and closes',
      () async {
        final incoming = <Object>[];
        final listener = backend.advertisements.listen(
          (_) {},
          onError: incoming.add,
        );
        final connection = await connect(backend, transport, 41);
        final disconnected = connection.disconnected.first;
        transport.controller.add(Uint8List(length));
        expect(
          transport.closed,
          isFalse,
        ); // Transport close leaves the callback first.
        final scan = backend.startScan();
        final reconnect = backend.connect(const BleDeviceId('opaque-device'));
        expect(transport.pending, isEmpty);
        await expectLater(scan.result, throwsA(error(BleErrorCode.disposed)));
        await expectLater(
          reconnect.result,
          throwsA(error(BleErrorCode.disposed)),
        );
        await backend.close();
        expect(transport.closed, isTrue);
        await disconnected;
        expect(
          incoming.single,
          error(BleErrorCode.internal)
              .having((e) => e.context.operation, 'operation', 'decodeEvent'),
        );
        await listener.cancel();
      },
    );
  }
  for (final kind in [5, 6]) {
    test(
      'malformed current generation event $kind reports a typed error and closes',
      () async {
        final incoming = <Object>[];
        final listener = backend.advertisements.listen(
          (_) {},
          onError: incoming.add,
        );
        final connection = await connect(backend, transport, 41);
        final disconnected = connection.disconnected.first;
        transport.emit(kind, 41, Uint8List.fromList([0]));
        expect(
          transport.closed,
          isFalse,
        ); // Transport close leaves the callback first.
        final scan = backend.startScan();
        final reconnect = backend.connect(const BleDeviceId('opaque-device'));
        expect(transport.pending, isEmpty);
        await expectLater(scan.result, throwsA(error(BleErrorCode.disposed)));
        await expectLater(
          reconnect.result,
          throwsA(error(BleErrorCode.disposed)),
        );
        await backend.close();
        expect(transport.closed, isTrue);
        await disconnected;
        expect(incoming.single, error(BleErrorCode.internal));
        await listener.cancel();
      },
    );
  }
  test('late malformed values and errors cannot close or report into a fresh generation', () async {
    final errors = <Object>[];
    final listener = backend.advertisements.listen((_) {}, onError: errors.add);
    final old = await connect(backend, transport, 41);
    final ended = old.disconnected.first;
    transport.emit(5, 41, Uint8List(0));
    await ended;
    final fresh = await connect(backend, transport, 42);
    final values = <BackendNotification>[];
    final notificationErrors = <Object>[];
    final freshListener = fresh.notifications.listen(
      values.add,
      onError: notificationErrors.add,
    );
    for (final kind in [5, 6]) {
      for (final code in [0, 15, 0xffffffff]) {
        for (final length in [0, 1, 31, 32]) {
          transport.emit(kind, 41, Uint8List(length), code: code);
        }
      }
    }
    transport.emit(6, 42, Uint8List.fromList([...List.filled(32, 0), 0, 255]));
    final mtu = fresh.getMtu();
    transport.next(49).result.complete(Uint8List.fromList([23, 0]));
    expect(await mtu.result, 23);
    expect(transport.closed, isFalse);
    expect(errors, isEmpty);
    expect(notificationErrors, isEmpty);
    expect(values.single.generation, 42);
    expect(values.single.value, [0, 255]);
    await freshListener.cancel();
    await listener.cancel();
  });

  for (final operation in ['read', 'write']) {
    for (final lateError in [false, true]) {
      test(
        'old $operation result after reconnect cannot affect replacement; error=$lateError',
        () async {
          final ble = Ble(backend: backend);
          addTearDown(ble.close);
          final characteristic = BleCharacteristic(
            serviceUuid: BleUuid('180f'),
            uuid: BleUuid('2a19'),
            properties: const BleCharacteristicProperties(0x3e),
          );
          await ble.ready;
          final connecting = ble.connect(const BleDeviceId('opaque-device'));
          await Future<void>(() {});
          transport.next(30).result.complete(generation(41));
          final old = await connecting;
          final outcomes = <BleErrorCode>[];
          final Future<dynamic> result = operation == 'read'
              ? old.read(characteristic)
              : old.write(characteristic, Uint8List.fromList([1]));
          final terminal = result.then<void>(
            (_) => fail('Old request completed successfully'),
            onError: (Object error) =>
                outcomes.add((error as BleException).code),
          );
          await Future<void>(() {});
          final late = transport.next(operation == 'read' ? 41 : 42);
          transport.emit(5, 41, Uint8List(0));
          await terminal;
          expect(outcomes, [BleErrorCode.disconnected]);
          expect(late.cancelled, isTrue);
          final reconnecting = ble.connect(const BleDeviceId('opaque-device'));
          await Future<void>(() {});
          transport.next(30).result.complete(generation(42));
          final fresh = await reconnecting;
          expect(fresh.generation, greaterThan(old.generation));
          final diagnostics = <BleDiagnostic>[];
          final diagnosticListener = ble.diagnostics.listen(diagnostics.add);
          if (lateError) {
            late.result.completeError(
              const BleException(
                BleErrorCode.gattFailure,
                'Old OS request failed',
              ),
            );
          } else {
            late.result.complete(
              operation == 'read' ? Uint8List.fromList([99]) : Uint8List(0),
            );
          }
          // A full event-loop turn drains the already queued old-result continuations.
          await Future<void>(() {});
          expect(outcomes, [BleErrorCode.disconnected]);
          expect(fresh.state, BleConnectionState.connected);
          expect(diagnostics, isEmpty);
          final read = fresh.read(characteristic);
          await Future<void>(() {});
          final current = transport.next(41);
          expect(
            ByteData.sublistView(current.payload).getUint64(0, Endian.little),
            42,
          );
          current.result.complete(Uint8List.fromList([0, 255]));
          expect(await read, [0, 255]);
          await expectLater(
            old.read(characteristic),
            throwsA(error(BleErrorCode.disconnected)),
          );
          expect(transport.pending, isEmpty);
          await diagnosticListener.cancel();
          await ble.close();
          expect(transport.closed, isTrue);
        },
      );
    }
  }

  for (final operation in ['read', 'write', 'discovery']) {
    for (final terminal in [
      'completed',
      'cancelled',
      'cancelledBeforeDelivery',
      'timeout',
      'completionAtDeadlineFirst',
      'timeoutAtDeadlineFirst',
    ]) {
      for (final lateError
          in [
                'completed',
                'cancelledBeforeDelivery',
                'completionAtDeadlineFirst',
                'timeoutAtDeadlineFirst',
              ].contains(terminal)
              ? [false]
              : [false, true]) {
        test(
          'public $operation: $terminal wins over late result; error=$lateError',
          () async {
            final transport = Transport();
            late Ble ble;
            fakeAsync((clock) {
              final timeouts = BleTimeouts(
                read: Duration(seconds: operation == 'read' ? 1 : 3),
                write: Duration(seconds: operation == 'write' ? 1 : 3),
                discovery: Duration(seconds: operation == 'discovery' ? 1 : 3),
              );
              final native = NativeBleBackend(
                transport: transport,
                timeouts: timeouts,
              );
              ble = Ble(backend: native, timeouts: timeouts);
              BleConnection? connection;
              ble
                  .connect(const BleDeviceId('race-device'))
                  .then((value) => connection = value);
              clock.flushMicrotasks();
              transport.next(30).result.complete(generation(123));
              clock.flushMicrotasks();
              final characteristic = BleCharacteristic(
                serviceUuid: BleUuid('180f'),
                uuid: BleUuid('2a19'),
                properties: const BleCharacteristicProperties(0x3e),
              );
              final cancellation = BleCancellation();
              Uint8List payload() => switch (operation) {
                'read' => Uint8List.fromList([0, 255, 128]),
                'write' => Uint8List(0),
                _ => File('test/fixtures/services.bin').readAsBytesSync(),
              };
              Pending? boundaryPending;
              if (terminal == 'completionAtDeadlineFirst') {
                // Registered before the public deadline timer at the same instant.
                Timer(
                  const Duration(seconds: 1),
                  () => boundaryPending!.result.complete(payload()),
                );
              }
              final Future<dynamic> result = switch (operation) {
                'read' => connection!.read(
                  characteristic,
                  cancellation: cancellation,
                ),
                'write' => connection!.write(
                  characteristic,
                  Uint8List.fromList([42]),
                  cancellation: cancellation,
                ),
                _ => connection!.discoverServices(cancellation: cancellation),
              };
              final outcomes = <Object>[];
              result.then<void>(
                (_) => outcomes.add('completed'),
                onError: (Object error) {
                  outcomes.add((error as BleException).code);
                },
              );
              // This queued request proves that the terminal outcome releases FIFO,
              // even when the transport keeps the cancelled OS future unresolved.
              var barrierCompleted = false;
              final Future<dynamic> queued = operation == 'discovery'
                  ? connection!.getMtu()
                  : connection!.discoverServices();
              queued.then((_) => barrierCompleted = true);
              clock.flushMicrotasks();
              final pending = transport.next(switch (operation) {
                'read' => 41,
                'write' => 42,
                _ => 40,
              });
              boundaryPending = pending;
              if (terminal == 'timeoutAtDeadlineFirst') {
                // Registered after the public deadline timer, at the same instant.
                Timer(
                  const Duration(seconds: 1),
                  () => pending.result.complete(payload()),
                );
              }
              if (terminal == 'completed') {
                pending.result.complete(payload());
                clock.flushMicrotasks();
                cancellation.cancel();
              } else if (terminal == 'cancelledBeforeDelivery') {
                // The OS future has completed, but its Dart continuation has
                // not delivered a public terminal result yet.
                pending.result.complete(payload());
                cancellation.cancel();
                clock.flushMicrotasks();
              } else if (terminal == 'cancelled') {
                cancellation.cancel();
                cancellation.cancel();
                clock.flushMicrotasks();
              } else {
                clock.elapse(const Duration(seconds: 1));
                clock.flushMicrotasks();
                cancellation.cancel();
              }
              final expected = switch (terminal) {
                'completed' || 'completionAtDeadlineFirst' => 'completed',
                'cancelled' ||
                'cancelledBeforeDelivery' => BleErrorCode.cancelled,
                _ => BleErrorCode.timeout,
              };
              expect(outcomes, [expected]);
              expect(
                pending.cancelled,
                !['completed', 'completionAtDeadlineFirst'].contains(terminal),
              );
              final barrier = transport.next(
                operation == 'discovery' ? 49 : 40,
              );
              barrier.result.complete(
                operation == 'discovery'
                    ? Uint8List.fromList([23, 0])
                    : File('test/fixtures/services.bin').readAsBytesSync(),
              );
              clock.flushMicrotasks();
              expect(barrierCompleted, isTrue);
              if (terminal != 'completed' &&
                  terminal != 'cancelledBeforeDelivery' &&
                  terminal != 'completionAtDeadlineFirst' &&
                  terminal != 'timeoutAtDeadlineFirst') {
                // Unlike FakeOperation, this transport really delivers late OS
                // success/error, allowing the facade's terminal guard to be tested.
                if (lateError) {
                  pending.result.completeError(
                    const BleException(
                      BleErrorCode.gattFailure,
                      'Late OS error',
                    ),
                  );
                } else {
                  pending.result.complete(payload());
                }
                clock.flushMicrotasks();
              }
              clock.elapse(const Duration(seconds: 2));
              clock.flushMicrotasks();
              expect(outcomes, [expected]);
              expect(connection!.state, BleConnectionState.connected);
              expect(transport.pending, isEmpty);
              expect(clock.nonPeriodicTimerCount, 0);
            });
            // Stream cancellation can use SDK futures allocated outside the
            // virtual zone; perform disposal in the ordinary async test zone.
            await ble.close();
            expect(transport.closed, isTrue);
          },
        );
      }
    }
  }
  test('native discovery, reads and both writes use generation-scoped binary commands', () async {
    final connection = await connect(backend, transport, 41);
    final discovery = connection.discoverServices();
    final call = transport.next(40);
    expect(call.payload, generation(41));
    call.result.complete(File('test/fixtures/services.bin').readAsBytesSync());
    final services = await discovery.result;
    expect(services.single.primary, isTrue);
    final characteristic = services.single.characteristics.single;
    expect(
      characteristic.descriptors.single.uuid,
      BleUuid('00000000-0000-0000-0000-000000000003'),
    );
    expect(characteristic.properties.read, isTrue);
    expect(characteristic.properties.notify, isTrue);
    final read = connection.read(characteristic);
    final reading = transport.next(41);
    expect(reading.payload.length, 40);
    expect(reading.timeout, const Duration(seconds: 31));
    reading.result.complete(Uint8List.fromList([0, 255, 128]));
    final value = await read.result;
    expect(value, [0, 255, 128]);
    expect(() => value[0] = 1, throwsUnsupportedError);
    for (final withResponse in [true, false]) {
      final bytes = Uint8List.fromList([1, 0, 254]);
      final write = connection.write(characteristic, bytes, withResponse);
      bytes.fillRange(0, bytes.length, 0);
      final writing = transport.next(withResponse ? 42 : 43);
      expect(writing.payload.sublist(40), [1, 0, 254]);
      expect(writing.timeout, const Duration(seconds: 32));
      writing.result.complete(Uint8List(0));
      await write.result;
    }
  });
  test(
    'old generations cannot notify or disconnect a replacement connection',
    () async {
      final first = await connect(backend, transport, 41);
      final incoming = <BackendNotification>[];
      final listener = first.notifications.listen(incoming.add);
      final notification = Uint8List.fromList([
        ...List.filled(31, 0),
        2,
        0,
        255,
      ]);
      transport.emit(6, 40, notification);
      expect(incoming, isEmpty);
      final read = first.read(
        BleCharacteristic(
          serviceUuid: BleUuid('180f'),
          uuid: BleUuid('2a19'),
          properties: const BleCharacteristicProperties(2),
        ),
      );
      final pending = transport.next(41);
      transport.emit(5, 41, Uint8List(0));
      pending.result.complete(Uint8List.fromList([99]));
      await expectLater(read.result, throwsA(error(BleErrorCode.disconnected)));
      final second = await connect(backend, transport, 42);
      final secondIncoming = <BackendNotification>[];
      final secondListener = second.notifications.listen(secondIncoming.add);
      transport.emit(5, 41, Uint8List(0));
      transport.emit(6, 41, notification);
      transport.emit(6, 42, notification);
      // Native streams are asynchronous; this operation completion is the explicit delivery barrier.
      final mtu = second.getMtu();
      transport.next(49).result.complete(Uint8List.fromList([23, 0]));
      await mtu.result;
      expect(secondIncoming.single.generation, 42);
      expect(secondIncoming.single.value, [0, 255]);
      await listener.cancel();
      await secondListener.cancel();
    },
  );
  test('descriptor identity, connected RSSI, MTU and disconnect use dedicated commands', () async {
    final connection = await connect(backend, transport, 99);
    final descriptor = BleDescriptor(
      serviceUuid: BleUuid('180f'),
      characteristicUuid: BleUuid('2a19'),
      uuid: BleUuid('2901'),
    );
    final read = connection.readDescriptor(descriptor);
    final call = transport.next(46);
    expect(call.payload.length, 56);
    call.result.complete(Uint8List.fromList([65]));
    expect(await read.result, [65]);
    final write = connection.writeDescriptor(
      descriptor,
      Uint8List.fromList([66]),
    );
    final writing = transport.next(47);
    expect(writing.payload.sublist(56), [66]);
    writing.result.complete(Uint8List(0));
    await write.result;
    final rssi = connection.readRssi();
    transport.next(48).result.complete(Uint8List.fromList([183, 255]));
    expect(await rssi.result, -73);
    final mtu = connection.getMtu();
    transport.next(49).result.complete(Uint8List.fromList([247, 0]));
    expect(await mtu.result, 247);
    final requestedMtu = connection.requestMtu(247);
    final mtuRequest = transport.next(50);
    expect(mtuRequest.payload, [...generation(99), 247, 0]);
    mtuRequest.result.complete(Uint8List.fromList([185, 0]));
    expect(await requestedMtu.result, 185);
    final priority = connection.requestConnectionPriority(
      BleConnectionPriority.high,
    );
    final priorityRequest = transport.next(51);
    expect(priorityRequest.payload, [...generation(99), 1]);
    priorityRequest.result.complete(Uint8List(0));
    await priority.result;
    final disconnected = connection.disconnect();
    final stopping = transport.next(31);
    expect(stopping.payload, generation(99));
    expect(identical(connection.disconnect(), disconnected), isTrue);
    stopping.result.complete(Uint8List(0));
    await disconnected.result;
    await expectLater(
      connection.readDescriptor(descriptor).result,
      throwsA(error(BleErrorCode.disconnected)),
    );
  });
  test('native GATT errors retain platform evidence and exact UUID/generation context', () async {
    final connection = await connect(backend, transport, 77);
    final characteristic = BleCharacteristic(
      serviceUuid: BleUuid('180f'),
      uuid: BleUuid('2a19'),
      properties: const BleCharacteristicProperties(2),
    );
    final request = connection.read(characteristic);
    transport
        .next(41)
        .result
        .completeError(
          const BleException(
            BleErrorCode.gattFailure,
            'OS rejected read',
            context: BleErrorContext(
              platform: 'macos',
              nativeCode: '42',
              nativeMessage: 'underlying error',
            ),
          ),
        );
    await expectLater(
      request.result,
      throwsA(
        error(BleErrorCode.gattFailure)
            .having((e) => e.context.connectionGeneration, 'generation', 77)
            .having(
              (e) => e.context.characteristicUuid,
              'characteristic',
              characteristic.uuid,
            )
            .having(
              (e) => e.context.serviceUuid,
              'service',
              characteristic.serviceUuid,
            )
            .having((e) => e.context.platform, 'platform', 'macos')
            .having(
              (e) => e.context.nativeMessage,
              'native message',
              'underlying error',
            ),
      ),
    );
  });
  test('every truncated service payload is rejected', () {
    final bytes = File('test/fixtures/services.bin').readAsBytesSync();
    for (var length = 0; length < bytes.length; length++) {
      expect(
        () => decodeServices(Uint8List.sublistView(bytes, 0, length)),
        throwsFormatException,
      );
    }
    expect(
      () => decodeServices(Uint8List.fromList([...bytes, 1])),
      throwsFormatException,
    );
  });
}
