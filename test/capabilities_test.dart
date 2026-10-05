import 'dart:async';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:bletide/bletide.dart';
import 'package:bletide/testing.dart';
import 'package:test/test.dart';

final physical = <BleConnection, FakeBackendConnection>{};

final class CountedInitialization implements Future<void> {
  CountedInitialization(this.future);
  final Future<void> future;
  int observers = 0;
  @override
  Future<R> then<R>(FutureOr<R> Function(void) onValue, {Function? onError}) {
    observers++;
    return future.then(onValue, onError: onError);
  }

  @override
  Future<void> catchError(Function onError, {bool Function(Object)? test}) =>
      future.catchError(onError, test: test);
  @override
  Future<void> whenComplete(FutureOr<void> Function() action) =>
      future.whenComplete(action);
  @override
  Future<void> timeout(
    Duration duration, {
    FutureOr<void> Function()? onTimeout,
  }) => future.timeout(duration, onTimeout: onTimeout);
  @override
  Stream<void> asStream() => future.asStream();
}

const device = BleDeviceId('capability-fixture');
final characteristic = BleCharacteristic(
  serviceUuid: BleUuid('180f'),
  uuid: BleUuid('2a19'),
  properties: const BleCharacteristicProperties(0x3e),
);
Matcher failure(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);
Future<BleConnection> connected(Ble ble, FakeBleBackend backend) async {
  final result = ble.connect(device);
  await backend.waitFor<BackendConnection>('connect');
  final native = backend.completeConnect(device);
  final connection = await result;
  physical[connection] = native;
  return connection;
}

void main() {
  test(
    'GATT capability rejects all attribute work before backend dispatch',
    () async {
      // Even an inconsistent backend advertising descriptors must respect gatt.
      final backend = FakeBleBackend(
        capabilities: const BleCapabilities(
          connect: true,
          descriptorAccess: true,
        ),
      );
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final connection = await connected(ble, backend);
      final descriptor = BleDescriptor(
        serviceUuid: characteristic.serviceUuid,
        characteristicUuid: characteristic.uuid,
        uuid: BleUuid('2901'),
      );
      final expected = throwsA(
        isA<BleException>()
            .having((e) => e.code, 'code', BleErrorCode.notSupported)
            .having((e) => e.context.deviceId, 'device', device)
            .having(
              (e) => e.context.connectionGeneration,
              'generation',
              connection.generation,
            ),
      );
      await expectLater(connection.discoverServices(), expected);
      await expectLater(connection.read(characteristic), expected);
      for (final response in [true, false]) {
        await expectLater(
          connection.write(
            characteristic,
            Uint8List(1),
            withResponse: response,
          ),
          expected,
        );
      }
      await expectLater(connection.readDescriptor(descriptor), expected);
      await expectLater(
        connection.writeDescriptor(descriptor, Uint8List(1)),
        expected,
      );
      await expectLater(
        connection.enableNotifications(characteristic),
        expected,
      );
      await expectLater(connection.subscribe(characteristic).first, expected);
      expect(backend.history, ['connect']);
      expect(backend.pending, isEmpty);
      expect(backend.subscriptions, 0);
    },
  );

  test(
    'rediscovery preserves notification ownership until teardown completes',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final connection = await connected(ble, backend);
      final setup = connection.enableNotifications(characteristic);
      (await backend.waitFor<void>('subscribe')).complete(null);
      final owner = await setup;
      await expectLater(
        connection.discoverServices(),
        throwsA(failure(BleErrorCode.invalidState)),
      );
      expect(backend.history, isNot(contains('discover')));
      final received = Completer<Uint8List>();
      final listener = owner.values.listen(received.complete);
      physical[connection]!.emitNotification(
        characteristic,
        Uint8List.fromList([7]),
      );
      expect(await received.future, [7]);
      final stopping = listener.cancel();
      final teardown = await backend.waitFor<void>('unsubscribe');
      teardown.complete(null);
      await stopping;
      await owner.cancel();
      final discovering = connection.discoverServices();
      (await backend.waitFor<List<BleService>>('discover')).complete([]);
      expect(await discovering, isEmpty);
      expect(connection.state, BleConnectionState.connected);
    },
  );

  test(
    'scan initialization failure reaches current and later listeners',
    () async {
      final ready = Completer<void>();
      final initialization = CountedInitialization(ready.future);
      final backend = FakeBleBackend(initialization: initialization);
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final expected = throwsA(failure(BleErrorCode.permissionDenied));
      final pending = expectLater(ble.scan().first, expected);
      ready.completeError(
        const BleException(
          BleErrorCode.permissionDenied,
          'Initialization rejected',
        ),
      );
      await pending;
      await expectLater(ble.scan().first, expected);
      expect(initialization.observers, 1);
      expect(backend.history, isEmpty);
      expect(backend.pending, isEmpty);
    },
  );

  test(
    'scan cancellation before readiness releases ownership without waiting',
    () {
      fakeAsync((clock) {
        final ready = Completer<void>();
        final initialization = CountedInitialization(ready.future);
        final backend = FakeBleBackend(initialization: initialization);
        final ble = Ble(backend: backend);
        for (var i = 0; i < 100; i++) {
          final scan = ble.scan().listen((_) {});
          var cancelled = false;
          scan.cancel().then((_) => cancelled = true);
          clock.flushMicrotasks();
          expect(cancelled, isTrue);
        }
        expect(initialization.observers, 1);
        ready.complete();
        clock.flushMicrotasks();
        expect(backend.history, isEmpty);
        final retry = ble.scan().listen((_) {});
        clock.flushMicrotasks();
        backend.next<void>('scan.start').complete(null);
        clock.flushMicrotasks();
        expect(backend.scanning, isTrue);
        retry.cancel();
        clock.flushMicrotasks();
        backend.next<void>('scan.stop').complete(null);
        clock.flushMicrotasks();
        ble.close();
        clock.flushMicrotasks();
        expect(backend.pending, isEmpty);
        expect(backend.scanning, isFalse);
      });
    },
  );

  test('engine close during unresolved scan readiness finishes before late initialization', () async {
    final ready = Completer<void>();
    final backend = FakeBleBackend(initialization: ready.future);
    final ble = Ble(backend: backend);
    var scanDone = false;
    ble.scan().listen((_) {}, onDone: () => scanDone = true);
    await ble.close();
    expect(scanDone, isTrue);
    ready.complete();
    await ble.ready;
    expect(backend.history, isEmpty);
    expect(backend.pending, isEmpty);
  });

  test('shared readiness failure rejects current and later attempts with its cause', () async {
    final ready = Completer<void>();
    final initialization = CountedInitialization(ready.future);
    final backend = FakeBleBackend(initialization: initialization);
    final ble = Ble(backend: backend);
    addTearDown(ble.close);
    final matcher = isA<BleException>()
        .having((e) => e.code, 'code', BleErrorCode.permissionDenied)
        .having((e) => e.context.deviceId, 'device', device)
        .having(
          (e) => e.context.nativeMessage,
          'cause',
          'Bluetooth permission missing',
        );
    final pending = expectLater(ble.connect(device), throwsA(matcher));
    ready.completeError(
      const BleException(
        BleErrorCode.permissionDenied,
        'Initialization rejected',
        context: BleErrorContext(nativeMessage: 'Bluetooth permission missing'),
      ),
    );
    await pending;
    await expectLater(ble.connect(device), throwsA(matcher));
    expect(initialization.observers, 1);
    expect(backend.history, isEmpty);
    expect(backend.pending, isEmpty);
  });

  test(
    'cancelled initialization retries share one readiness observer',
    () async {
      final ready = Completer<void>();
      final initialization = CountedInitialization(ready.future);
      final backend = FakeBleBackend(initialization: initialization);
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      for (var i = 0; i < 100; i++) {
        final cancellation = BleCancellation();
        final result = ble.connect(device, cancellation: cancellation);
        final failed = expectLater(
          result,
          throwsA(failure(BleErrorCode.cancelled)),
        );
        cancellation.cancel();
        await failed;
        expect(backend.pending, isEmpty);
      }
      expect(initialization.observers, 1);
      final retry = ble.connect(device);
      ready.complete();
      await backend.waitFor<BackendConnection>('connect');
      backend.completeConnect(device);
      expect((await retry).state, BleConnectionState.connected);
      expect(initialization.observers, 1);
      expect(backend.history.where((name) => name == 'connect'), hasLength(1));
    },
  );

  test(
    'MTU diagnostics distinguish failure from successful queue recovery',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final connection = await connected(ble, backend);
      final events = <BleDiagnostic>[];
      final diagnostics = ble.diagnostics.listen(events.add);
      addTearDown(diagnostics.cancel);
      final failedEvent = ble.diagnostics.firstWhere(
        (event) => event.name == 'gatt.mtu.request.failed',
      );
      final requesting = connection.requestMtu(247);
      final failed = expectLater(
        requesting,
        throwsA(failure(BleErrorCode.gattFailure)),
      );
      final completedEvent = ble.diagnostics.firstWhere(
        (event) => event.name == 'gatt.mtu.get.completed',
      );
      final snapshot = connection.getMtu();
      (await backend.waitFor<int>('requestMtu')).fail(
        const BleException(
          BleErrorCode.gattFailure,
          'MTU callback rejected',
          context: BleErrorContext(nativeMessage: 'GATT status 133'),
        ),
      );
      (await backend.waitFor<int>('getMtu')).complete(23);
      await failed;
      expect(await snapshot, 23);
      final completedDiagnostic = await completedEvent;
      final started = events.where((event) => event.name.endsWith('.started'));
      expect(started.map((event) => event.operation), [
        'mtu.request',
        'mtu.get',
      ]);
      expect(started.every((event) => event.duration == null), isTrue);
      expect(completedDiagnostic.duration, isNotNull);
      expect(completedDiagnostic.duration!.isNegative, isFalse);
      expect(
        events.map((event) => event.name),
        isNot(contains('gatt.mtu.request.completed')),
      );
      final diagnostic = await failedEvent;
      expect(diagnostic.operation, 'mtu.request');
      expect(diagnostic.errorCode, 'gattFailure');
      expect(diagnostic.nativeMessage, 'GATT status 133');
      expect(diagnostic.deviceId, device);
      expect(diagnostic.generation, connection.generation);
      expect(diagnostic.duration, isNotNull);
      expect(diagnostic.duration!.isNegative, isFalse);
      expect(
        events.where((event) => event.name == 'gatt.mtu.request.failed'),
        hasLength(1),
      );
      expect(backend.pending, isEmpty);
    },
  );

  test(
    'replacement notification readiness preserves a failed teardown cause',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      final connection = await connected(ble, backend);
      final initial = connection.enableNotifications(characteristic);
      (await backend.waitFor<void>('subscribe')).complete(null);
      final owner = await initial;
      Object? releaseError;
      final releasing = owner.cancel().then<void>(
        (_) {
          fail('Teardown should fail');
        },
        onError: (Object error) {
          releaseError = error;
        },
      );
      final stopping = await backend.waitFor<void>('unsubscribe');
      Object? readinessError;
      final replacement = connection
          .enableNotifications(characteristic)
          .then<void>(
            (_) {
              fail('Replacement should fail');
            },
            onError: (Object error) {
              readinessError = error;
            },
          );
      stopping.fail(
        const BleException(
          BleErrorCode.gattFailure,
          'CCCD disable rejected',
          context: BleErrorContext(
            platform: 'fixture',
            nativeCode: '133',
            nativeMessage: 'descriptor write failed',
          ),
        ),
      );
      (await backend.waitFor<void>('disconnect')).complete(null);
      await replacement;
      await releasing;
      expect(readinessError, failure(BleErrorCode.gattFailure));
      final cause = readinessError as BleException;
      expect(cause.message, 'CCCD disable rejected');
      expect(cause.context.operation, 'unsubscribe');
      expect(cause.context.deviceId, device);
      expect(cause.context.connectionGeneration, connection.generation);
      expect(cause.context.nativeCode, '133');
      expect(cause.context.nativeMessage, 'descriptor write failed');
      expect(releaseError, failure(BleErrorCode.gattFailure));
      expect(connection.state, BleConnectionState.disconnected);
      expect(backend.pending, isEmpty);
      expect(backend.subscriptions, 0);
      final next = await connected(ble, backend);
      expect(next.generation, greaterThan(connection.generation));
    },
  );

  test('notification overflow during setup rejects readiness and cleans after enable ACK', () async {
    final backend = FakeBleBackend();
    final ble = Ble(backend: backend);
    final connection = await connected(ble, backend);
    final setup = connection.enableNotifications(characteristic);
    final enabling = await backend.waitFor<void>('subscribe');
    final failed = expectLater(
      setup,
      throwsA(failure(BleErrorCode.gattFailure)),
    );
    for (var i = 0; i < 257; i++) {
      physical[connection]!.emitNotification(
        characteristic,
        Uint8List.fromList([i & 255]),
      );
    }
    await failed;
    enabling.complete(null);
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    await ble.close();
    expect(backend.pending, isEmpty);
  });

  test(
    'explicit notification owner cancellation discards undelivered values',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      final connection = await connected(ble, backend);
      final setup = connection.enableNotifications(characteristic);
      (await backend.waitFor<void>('subscribe')).complete(null);
      final owner = await setup;
      physical[connection]!.emitNotification(
        characteristic,
        Uint8List.fromList([9]),
      );
      final barrier = connection.readRssi();
      (await backend.waitFor<int>('rssi')).complete(-40);
      await barrier;
      final stopping = owner.cancel();
      (await backend.waitFor<void>('unsubscribe')).complete(null);
      await stopping;
      expect(await owner.values.toList(), isEmpty);
      await ble.close();
    },
  );

  test('notification owner discards buffered values after its generation closes', () async {
    final backend = FakeBleBackend();
    final ble = Ble(backend: backend);
    final connection = await connected(ble, backend);
    final setup = connection.enableNotifications(characteristic);
    (await backend.waitFor<void>('subscribe')).complete(null);
    final owner = await setup;
    physical[connection]!.emitNotification(
      characteristic,
      Uint8List.fromList([9]),
    );
    // Drain the queued notification delivery through the operation coordinator.
    final barrier = connection.readRssi();
    (await backend.waitFor<int>('rssi')).complete(-40);
    await barrier;
    physical[connection]!.remoteDisconnect();
    expect(await owner.values.toList(), isEmpty);
    await owner.cancel();
    final next = await connected(ble, backend);
    expect(next.generation, greaterThan(connection.generation));
    await ble.close();
  });
  test('unlistened notification owner has bounded buffering and releases setup on overflow', () async {
    final backend = FakeBleBackend();
    final ble = Ble(backend: backend);
    final connection = await connected(ble, backend);
    final setup = connection.enableNotifications(characteristic);
    (await backend.waitFor<void>('subscribe')).complete(null);
    final owner = await setup;
    for (var i = 0; i < 300; i++) {
      physical[connection]!.emitNotification(
        characteristic,
        Uint8List.fromList([i & 255]),
      );
    }
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    var delivered = 0;
    final done = Completer<void>();
    Object? overflow;
    final listener = owner.values.listen(
      (_) {
        delivered++;
      },
      onError: (Object error) {
        overflow = error;
      },
      onDone: done.complete,
    );
    await done.future;
    expect(delivered, 0);
    expect(overflow, failure(BleErrorCode.gattFailure));
    expect((overflow as BleException).context.operation, 'notification.buffer');
    expect(connection.state, BleConnectionState.connected);
    await listener.cancel();
    await owner.cancel();
    await ble.close();
    expect(backend.pending, isEmpty);
  });

  test(
    'initialization cancellation returns promptly and allows retry',
    () async {
      final initialized = Completer<void>();
      final backend = FakeBleBackend(initialization: initialized.future);
      final ble = Ble(backend: backend);
      final token = BleCancellation();
      final first = ble.connect(device, cancellation: token);
      token.cancel();
      await expectLater(first, throwsA(failure(BleErrorCode.cancelled)));
      expect(backend.pending, isEmpty);
      final retry = ble.connect(device);
      initialized.complete();
      await backend.waitFor<BackendConnection>('connect');
      backend.completeConnect(device);
      expect((await retry).generation, 1);
      await ble.close();
    },
  );
  test('initialization is included in connect deadline and late readiness cannot connect', () {
    fakeAsync((clock) {
      final initialized = Completer<void>();
      final backend = FakeBleBackend(initialization: initialized.future);
      final ble = Ble(
        backend: backend,
        timeouts: const BleTimeouts(connect: Duration(seconds: 2)),
      );
      Object? result;
      ble
          .connect(device)
          .then<void>(
            (_) => fail('unexpected connection'),
            onError: (Object e) {
              result = e;
            },
          );
      clock.elapse(const Duration(seconds: 2));
      clock.flushMicrotasks();
      expect(result, failure(BleErrorCode.timeout));
      initialized.complete();
      clock.flushMicrotasks();
      expect(backend.pending, isEmpty);
      ble.close();
      clock.flushMicrotasks();
    });
  });
  test(
    'notification readiness waits for enable ACK and buffers early values',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      final connection = await connected(ble, backend);
      var ready = false;
      final enabling = connection.enableNotifications(characteristic).then((
        owner,
      ) {
        ready = true;
        return owner;
      });
      final setup = await backend.waitFor<void>('subscribe');
      expect(ready, isFalse);
      physical[connection]!.emitNotification(
        characteristic,
        Uint8List.fromList([7]),
      );
      setup.complete(null);
      final owner = await enabling;
      final received = Completer<Uint8List>();
      final listener = owner.values.listen(received.complete);
      expect(await received.future, [7]);
      final cancelling = listener.cancel();
      final stopping = await backend.waitFor<void>('unsubscribe');
      stopping.complete(null);
      await cancelling;
      await owner.cancel();
      await ble.close();
    },
  );
  test('notification cancellation releases setup and replacement waits for cleanup', () async {
    final backend = FakeBleBackend();
    final ble = Ble(backend: backend);
    final connection = await connected(ble, backend);
    final token = BleCancellation();
    final enabling = connection.enableNotifications(
      characteristic,
      cancellation: token,
    );
    final setup = await backend.waitFor<void>('subscribe');
    token.cancel();
    await expectLater(enabling, throwsA(failure(BleErrorCode.cancelled)));
    setup.complete(null);
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    // The queue ensures a replacement cannot bypass teardown.
    final next = connection.enableNotifications(characteristic);
    (await backend.waitFor<void>('subscribe')).complete(null);
    final owner = await next;
    final stopping = owner.cancel();
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    await stopping;
    await ble.close();
    expect(backend.pending, isEmpty);
  });
  test('notification enable failure rejects readiness and cleanup permits fresh setup', () async {
    final backend = FakeBleBackend();
    final ble = Ble(backend: backend);
    final connection = await connected(ble, backend);
    final enabling = connection.enableNotifications(characteristic);
    (await backend.waitFor<void>('subscribe'))
        .fail(const BleException(BleErrorCode.gattFailure, 'CCCD rejected'));
    await expectLater(enabling, throwsA(failure(BleErrorCode.gattFailure)));
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    final next = connection.enableNotifications(characteristic);
    (await backend.waitFor<void>('subscribe')).complete(null);
    final owner = await next;
    final stopping = owner.cancel();
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    await stopping;
    await ble.close();
  });
  test('disconnect rejects pending notification readiness and new generation works', () async {
    final backend = FakeBleBackend();
    final ble = Ble(backend: backend);
    final first = await connected(ble, backend);
    final enabling = first.enableNotifications(characteristic);
    await backend.waitFor<void>('subscribe');
    physical[first]!.remoteDisconnect();
    await expectLater(enabling, throwsA(failure(BleErrorCode.disconnected)));
    final next = await connected(ble, backend);
    expect(next.generation, greaterThan(first.generation));
    final setup = next.enableNotifications(characteristic);
    (await backend.waitFor<void>('subscribe')).complete(null);
    final owner = await setup;
    final stopping = owner.cancel();
    (await backend.waitFor<void>('unsubscribe')).complete(null);
    await stopping;
    await ble.close();
  });
  test('notification readiness timeout cleans setup before retry', () {
    fakeAsync((clock) {
      final backend = FakeBleBackend();
      final ble = Ble(
        backend: backend,
        timeouts: const BleTimeouts(subscribe: Duration(seconds: 1)),
      );
      late BleConnection connection;
      ble.connect(device).then((value) {
        connection = value;
      });
      clock.flushMicrotasks();
      backend.completeConnect(device);
      clock.flushMicrotasks();
      Object? failed;
      connection
          .enableNotifications(characteristic)
          .then<void>(
            (_) {
              fail('Unexpected enable');
            },
            onError: (Object error) {
              failed = error;
            },
          );
      clock.flushMicrotasks();
      expect(backend.pending.single.name, 'subscribe');
      clock.elapse(const Duration(seconds: 1));
      clock.flushMicrotasks();
      expect(failed, failure(BleErrorCode.timeout));
      expect(backend.pending.single.name, 'unsubscribe');
      backend.pending.single.complete(null);
      clock.flushMicrotasks();
      BleNotificationSubscription? next;
      connection.enableNotifications(characteristic).then((value) {
        next = value;
      });
      clock.flushMicrotasks();
      expect(backend.pending.single.name, 'subscribe');
      backend.pending.single.complete(null);
      clock.flushMicrotasks();
      expect(next, isNotNull);
      next!.cancel();
      clock.flushMicrotasks();
      backend.pending.single.complete(null);
      clock.flushMicrotasks();
      expect(backend.pending, isEmpty);
      ble.close();
      clock.flushMicrotasks();
    });
  });
  test(
    'write payload limit reflects reported MTU and attribute ceiling',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      final connection = await connected(ble, backend);
      for (final (mtu, limit) in [
        (23, 20),
        (185, 182),
        (247, 244),
        (517, 512),
      ]) {
        final result = connection.getWritePayloadLimit();
        (await backend.waitFor<int>('getMtu')).complete(mtu);
        expect(await result, limit);
      }
      await expectLater(
        connection.requestMtu(22),
        throwsA(failure(BleErrorCode.invalidState)),
      );
      final priority = connection.requestConnectionPriority(
        BleConnectionPriority.lowPower,
      );
      final request = await backend.waitFor<void>('connection.priority');
      expect(request.value, [2]);
      request.complete(null);
      await priority;
      await ble.close();
    },
  );
  test(
    'unsupported MTU and priority fail explicitly without platform work',
    () async {
      final backend = FakeBleBackend(
        capabilities: const BleCapabilities(connect: true, gatt: true),
      );
      final ble = Ble(backend: backend);
      final connection = await connected(ble, backend);
      await expectLater(
        connection.getWritePayloadLimit(),
        throwsA(failure(BleErrorCode.notSupported)),
      );
      await expectLater(
        connection.requestMtu(247),
        throwsA(failure(BleErrorCode.notSupported)),
      );
      await expectLater(
        connection.requestConnectionPriority(BleConnectionPriority.high),
        throwsA(failure(BleErrorCode.notSupported)),
      );
      expect(backend.pending, isEmpty);
      await ble.close();
    },
  );
}
