import 'dart:async';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:bletide/bletide.dart';
import 'package:bletide/testing.dart';
import 'package:test/test.dart';

import 'support/gatt_contract.dart';
import 'support/fake_gatt_fixture.dart';

TypeMatcher<BleException> error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);
const device = BleDeviceId('opaque-device');
BleCharacteristic characteristic() => BleCharacteristic(
  serviceUuid: BleUuid('180f'),
  uuid: BleUuid('2a19'),
  properties: const BleCharacteristicProperties(0x3e),
);

/// Contract fixtures resolve explicit platform operations; no sleeps or BLE hardware.
Future<(BleConnection, FakeBackendConnection)> connect(
  Ble ble,
  FakeBleBackend backend,
) async {
  final future = ble.connect(device);
  await backend.waitFor<BackendConnection>('connect');
  final native = backend.completeConnect(device);
  return (await future, native);
}

void main() {
  registerGattContracts('FakeBleBackend', fakeGattFixture);
  test(
    'unsupported notification policy is explicit and performs no setup',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      final (connection, native) = await connect(ble, backend);
      const policy = BleNotificationSetupMode.compat;
      await expectLater(
        connection.enableNotifications(characteristic(), setupMode: policy),
        throwsA(error(BleErrorCode.notSupported)),
      );
      await expectLater(
        connection.subscribe(characteristic(), setupMode: policy).first,
        throwsA(error(BleErrorCode.notSupported)),
      );
      expect(backend.pending, isEmpty);
      expect(native.subscriptions, isEmpty);
      await ble.close();
    },
  );

  test(
    'close reports backend cleanup failure and still closes Dart streams',
    () async {
      final backend = FakeBleBackend(
        closeError: const BleException(
          BleErrorCode.gattFailure,
          'OS stop failed',
        ),
      );
      final ble = Ble(backend: backend);
      final done = Completer<void>();
      final subscription = ble.diagnostics.listen(
        (_) {},
        onDone: done.complete,
      );
      final closing = ble.close();
      expect(identical(closing, ble.close()), isTrue);
      await expectLater(closing, throwsA(error(BleErrorCode.gattFailure)));
      await done.future;
      await subscription.cancel();
      expect(backend.pending, isEmpty);
      expect(backend.liveConnections, 0);
      expect(backend.scanning, isFalse);
    },
  );
  group('backend behavioral contract / FakeBleBackend', () {
    late FakeBleBackend backend;
    late Ble ble;
    setUp(() {
      backend = FakeBleBackend();
      ble = Ble(backend: backend);
    });
    tearDown(() => ble.close());

    test(
      'adapter state starts with a snapshot and delivers subsequent changes',
      () async {
        final states = <BleAdapterState>[];
        final listener = ble.adapterState.listen(states.add);
        backend.setAdapterState(BleAdapterState.disabled);
        await listener.cancel();
        // Cancellation may discard asynchronous events; use a new snapshot too.
        expect(await ble.adapterState.first, BleAdapterState.disabled);
      },
    );

    test('scan starts once, post-filters different consumers, stops on final owner', () async {
      final first = <BleAdvertisement>[];
      final second = <BleAdvertisement>[];
      final a = ble
          .scan(filter: const BleScanFilter(namePrefix: 'Test'))
          .listen(first.add);
      final b = ble
          .scan(filter: BleScanFilter(serviceUuids: [BleUuid('180f')]))
          .listen(second.add);
      final start = await backend.waitFor<void>('scan.start');
      start.complete(null);
      await start.request.result;
      final advertisement = BleAdvertisement(
        deviceId: device,
        name: 'Test A',
        serviceUuids: [BleUuid('180f')],
      );
      // Waiting on a received advertisement is an explicit event barrier.
      final delivered = Completer<void>();
      final c = ble.scan().listen((_) {
        if (!delivered.isCompleted) delivered.complete();
      });
      backend.advertise(advertisement);
      await delivered.future;
      expect(first, [advertisement]);
      expect(second, [advertisement]);
      await a.cancel();
      await a.cancel();
      await b.cancel();
      expect(backend.history.where((name) => name == 'scan.stop'), isEmpty);
      final stopped = c.cancel();
      (await backend.waitFor<void>('scan.stop')).complete(null);
      await stopped;
      expect(backend.scanning, isFalse);
      expect(backend.history.where((name) => name == 'scan.start').length, 1);
    });

    test(
      'scan cancellation during platform startup still stops the physical scan',
      () async {
        final listener = ble.scan().listen((_) {});
        final start = await backend.waitFor<void>('scan.start');
        final cancelled = listener.cancel();
        start.complete(null);
        (await backend.waitFor<void>('scan.stop')).complete(null);
        await cancelled;
        expect(backend.scanning, isFalse);
        expect(backend.pending, isEmpty);
      },
    );

    for (final queuedBeforeCancellation in [true, false]) {
      test(
        'final scan owner cancellation excludes racing advertisements; queued=$queuedBeforeCancellation',
        () async {
          final values = <BleAdvertisement>[];
          final delivered = Completer<void>();
          final listener = ble.scan().listen((value) {
            values.add(value);
            if (!delivered.isCompleted) delivered.complete();
          });
          final start = await backend.waitFor<void>('scan.start');
          start.complete(null);
          await start.request.result;
          final first = BleAdvertisement(deviceId: device, name: 'first');
          backend.advertise(first);
          await delivered.future;
          expect(backend.scanning, isTrue);
          final racing = BleAdvertisement(deviceId: device, name: 'racing');
          if (queuedBeforeCancellation) backend.advertise(racing);
          var finished = false;
          final cancellation = listener.cancel().then((_) => finished = true);
          final stop = await backend.waitFor<void>('scan.stop');
          expect(finished, isFalse);
          expect(backend.scanning, isTrue);
          if (!queuedBeforeCancellation) backend.advertise(racing);
          stop.complete(null);
          await cancellation;
          expect(finished, isTrue);
          expect(values, [first]);
          expect(backend.scanning, isFalse);
          expect(backend.pending, isEmpty);
          expect(
            backend.history.where((name) => name == 'scan.stop').length,
            1,
          );
        },
      );
    }

    test(
      'close joins an acknowledged active scan and closes its stream',
      () async {
        final delivered = Completer<void>();
        final done = Completer<void>();
        final listener = ble.scan().listen((_) {
          if (!delivered.isCompleted) delivered.complete();
        }, onDone: done.complete);
        final start = await backend.waitFor<void>('scan.start');
        start.complete(null);
        await start.request.result;
        backend.advertise(BleAdvertisement(deviceId: device));
        await delivered.future;
        expect(backend.scanning, isTrue);
        final close = ble.close();
        expect(identical(close, ble.close()), isTrue);
        await close;
        await done.future;
        await listener.cancel();
        expect(backend.scanning, isFalse);
        expect(backend.pending, isEmpty);
        expect(backend.liveConnections, 0);
        expect(backend.subscriptions, 0);
        final disposed = Completer<Object>();
        final late = ble.scan().listen(
          (_) => fail('Disposed scan delivered'),
          onError: disposed.complete,
        );
        expect(await disposed.future, error(BleErrorCode.disposed));
        await late.cancel();
        expect(backend.history.where((name) => name == 'scan.start').length, 1);
      },
    );

    test('scan start error goes to stream and cleanup runs', () async {
      final failure = Completer<Object>();
      final listener = ble.scan().listen((_) {}, onError: failure.complete);
      (await backend.waitFor<void>('scan.start')).fail(
        const BleException(BleErrorCode.permissionDenied, 'Permission lost'),
      );
      expect(await failure.future, error(BleErrorCode.permissionDenied));
      (await backend.waitFor<void>('scan.stop')).complete(null);
      await listener.cancel();
    });

    test(
      'scan stop failures are returned to the final cancelling owner',
      () async {
        final listener = ble.scan().listen((_) {});
        final start = await backend.waitFor<void>('scan.start');
        start.complete(null);
        await start.request.result;
        final cancel = listener.cancel();
        final assertion = expectLater(
          cancel,
          throwsA(error(BleErrorCode.gattFailure)),
        );
        (await backend.waitFor<void>('scan.stop')).fail(
          const BleException(
            BleErrorCode.gattFailure,
            'Controlled stop failure',
          ),
        );
        await assertion;
      },
    );

    test(
      'concurrent connect is rejected, connected duplicate is documented',
      () async {
        final first = ble.connect(device);
        await expectLater(
          ble.connect(device),
          throwsA(error(BleErrorCode.connectInProgress)),
        );
        await backend.waitFor<BackendConnection>('connect');
        backend.completeConnect(device);
        await first;
        await expectLater(
          ble.connect(device),
          throwsA(error(BleErrorCode.alreadyConnected)),
        );
        expect(backend.history.where((name) => name == 'connect').length, 1);
      },
    );

    test(
      'disconnect during connect cancels once and ignores late completion',
      () async {
        final waiting = ble.connect(device);
        final assertion = expectLater(
          waiting,
          throwsA(error(BleErrorCode.cancelled)),
        );
        final operation = await backend.waitFor<BackendConnection>('connect');
        await ble.disconnect(device);
        await assertion;
        operation.fail(
          const BleException(
            BleErrorCode.connectFailed,
            'Late platform failure',
          ),
        );
        expect(backend.pending, isEmpty);
      },
    );

    test(
      'GATT operations are FIFO; failure does not poison the queue',
      () async {
        final (connection, _) = await connect(ble, backend);
        final read = connection.read(characteristic());
        final assertion = expectLater(
          read,
          throwsA(error(BleErrorCode.gattFailure)),
        );
        final bytes = Uint8List.fromList([1, 255]);
        final write = connection.write(characteristic(), bytes);
        bytes[0] = 9;
        final readOperation = await backend.waitFor<Uint8List>('read');
        expect(backend.pending.map((operation) => operation.name), ['read']);
        readOperation.fail(
          const BleException(
            BleErrorCode.gattFailure,
            'Controlled read failure',
          ),
        );
        await assertion;
        final writeOperation = await backend.waitFor<void>('write');
        expect(writeOperation.value, [1, 255]);
        writeOperation.complete(null);
        await write;
      },
    );

    for (final operation in ['read', 'write', 'discover']) {
      test(
        'disconnect invalidates running $operation and queued work',
        () async {
          final (connection, _) = await connect(ble, backend);
          final Future<dynamic> waiting = switch (operation) {
            'read' => connection.read(characteristic()),
            'write' => connection.write(
              characteristic(),
              Uint8List.fromList([1]),
            ),
            _ => connection.discoverServices(),
          };
          final pendingAssertion = expectLater(
            waiting,
            throwsA(error(BleErrorCode.disconnected)),
          );
          final queued = connection.readRssi();
          final queuedAssertion = expectLater(
            queued,
            throwsA(error(BleErrorCode.disconnected)),
          );
          await backend.waitFor<dynamic>(operation);
          final closing = connection.disconnect();
          (await backend.waitFor<void>('disconnect')).complete(null);
          await closing;
          await pendingAssertion;
          await queuedAssertion;
          expect(backend.pending, isEmpty);
          expect(connection.state, BleConnectionState.disconnected);
          await connection.disconnect();
        },
      );
    }

    test('remote disconnect cancels queued and running requests', () async {
      final (connection, native) = await connect(ble, backend);
      final read = connection.read(characteristic());
      final readAssertion = expectLater(
        read,
        throwsA(error(BleErrorCode.disconnected)),
      );
      final write = connection.write(characteristic(), Uint8List.fromList([1]));
      final writeAssertion = expectLater(
        write,
        throwsA(error(BleErrorCode.disconnected)),
      );
      await backend.waitFor<Uint8List>('read');
      native.remoteDisconnect();
      await readAssertion;
      await writeAssertion;
      expect(backend.pending, isEmpty);
    });

    test(
      'fresh generations discard old events and reject old object use',
      () async {
        final (old, oldNative) = await connect(ble, backend);
        oldNative.remoteDisconnect();
        final (fresh, native) = await connect(ble, backend);
        expect(fresh.generation, greaterThan(old.generation));
        await expectLater(
          old.read(characteristic()),
          throwsA(error(BleErrorCode.disconnected)),
        );
        final received = Completer<Uint8List>();
        final listener = fresh.subscribe(characteristic()).listen((value) {
          if (!received.isCompleted) received.complete(value);
        });
        (await backend.waitFor<void>('subscribe')).complete(null);
        native.emitNotification(
          characteristic(),
          Uint8List.fromList([1]),
          sourceGeneration: old.generation,
        );
        native.emitNotification(characteristic(), Uint8List.fromList([2]));
        expect(await received.future, [2]);
        final cancellation = listener.cancel();
        (await backend.waitFor<void>('unsubscribe')).complete(null);
        await cancellation;
      },
    );

    test(
      'two notification consumers share startup; only last owner unsubscribes',
      () async {
        final (connection, native) = await connect(ble, backend);
        final stream = connection.subscribe(characteristic());
        final first = stream.listen((_) {});
        final lastEvent = Completer<Uint8List>();
        final second = connection
            .subscribe(characteristic())
            .listen(lastEvent.complete);
        final setup = await backend.waitFor<void>('subscribe');
        expect(backend.history.where((name) => name == 'subscribe').length, 1);
        await first.cancel();
        setup.complete(null);
        native.emitNotification(characteristic(), Uint8List.fromList([42]));
        expect(await lastEvent.future, [42]);
        expect(native.subscriptions.length, 1);
        final cancellation = second.cancel();
        (await backend.waitFor<void>('unsubscribe')).complete(null);
        await cancellation;
      },
    );

    test(
      'unsubscribe during subscribe startup compensates after startup finishes',
      () async {
        final (connection, native) = await connect(ble, backend);
        final listener = connection.subscribe(characteristic()).listen((_) {});
        final subscribe = await backend.waitFor<void>('subscribe');
        final cancellation = listener.cancel();
        subscribe.complete(null);
        (await backend.waitFor<void>('unsubscribe')).complete(null);
        await cancellation;
        // A new operation is a FIFO completion barrier for unsubscribe.
        final rssi = connection.readRssi();
        (await backend.waitFor<int>('rssi')).complete(-50);
        await rssi;
        expect(native.subscriptions, isEmpty);
      },
    );

    test(
      'final notification cancellation waits for stop acknowledgement',
      () async {
        final (connection, native) = await connect(ble, backend);
        final values = <Uint8List>[];
        final listener = connection
            .subscribe(characteristic())
            .listen(values.add);
        final setup = await backend.waitFor<void>('subscribe');
        setup.complete(null);
        await setup.request.result;
        var completed = false;
        final cancellation = listener.cancel().then((_) => completed = true);
        final stop = await backend.waitFor<void>('unsubscribe');
        // An issued request is a deterministic barrier, with teardown still pending.
        expect(completed, isFalse);
        native.emitNotification(characteristic(), Uint8List.fromList([99]));
        stop.complete(null);
        await cancellation;
        expect(completed, isTrue);
        expect(values, isEmpty);
        expect(native.subscriptions, isEmpty);
      },
    );

    for (final cleanupFails in [false, true]) {
      test(
        'failed notification stop closes generation; disconnect failure=$cleanupFails',
        () async {
          final (connection, native) = await connect(ble, backend);
          final diagnostics = <BleDiagnostic>[];
          final diagnosticListener = ble.diagnostics.listen(diagnostics.add);
          final listener = connection
              .subscribe(characteristic())
              .listen((_) {});
          final setup = await backend.waitFor<void>('subscribe');
          setup.complete(null);
          await setup.request.result;
          final cancellation = listener.cancel();
          final assertion = expectLater(
            cancellation,
            throwsA(error(BleErrorCode.gattFailure)),
          );
          (await backend.waitFor<void>('unsubscribe')).fail(
            const BleException(
              BleErrorCode.gattFailure,
              'Notification stop failed',
            ),
          );
          final disconnect = await backend.waitFor<void>('disconnect');
          expect(native.subscriptions, isNotEmpty);
          if (cleanupFails) {
            disconnect.fail(
              const BleException(
                BleErrorCode.permissionDenied,
                'Disconnect failed',
              ),
            );
          } else {
            disconnect.complete(null);
          }
          await assertion;
          await expectLater(
            connection.read(characteristic()),
            throwsA(error(BleErrorCode.disconnected)),
          );
          expect(
            diagnostics.map((event) => event.name),
            contains('gatt.unsubscribe.failed'),
          );
          if (cleanupFails) {
            expect(
              diagnostics.map((event) => event.name),
              contains('connection.disconnect.failed'),
            );
            await expectLater(
              connection.disconnect(),
              throwsA(error(BleErrorCode.permissionDenied)),
            );
          } else {
            expect(native.subscriptions, isEmpty);
            expect(backend.liveConnections, 0);
            final (fresh, _) = await connect(ble, backend);
            expect(fresh.generation, greaterThan(connection.generation));
          }
          await diagnosticListener.cancel();
        },
      );
    }

    test('new owner during setup retains setup and releases departed owner after acknowledgement', () async {
      final (connection, native) = await connect(ble, backend);
      final first = connection.subscribe(characteristic()).listen((_) {});
      final setup = await backend.waitFor<void>('subscribe');
      var cancelled = false;
      final cancellation = first.cancel().then((_) => cancelled = true);
      final received = Completer<Uint8List>();
      final next = connection
          .subscribe(characteristic())
          .listen(received.complete);
      final queued = connection.readRssi();
      expect(cancelled, isFalse);
      setup.complete(null);
      (await backend.waitFor<int>('rssi')).complete(-50);
      await queued;
      await cancellation;
      expect(
        backend.history.where((name) => name == 'subscribe'),
        hasLength(1),
      );
      expect(backend.history, isNot(contains('unsubscribe')));
      native.emitNotification(characteristic(), Uint8List.fromList([7]));
      expect(await received.future, [7]);
      final lastCancel = next.cancel();
      (await backend.waitFor<void>('unsubscribe')).complete(null);
      await lastCancel;
    });

    test('failed setup compensates before closing listeners and allows fresh broker', () async {
      final (connection, native) = await connect(ble, backend);
      final failure = Completer<Object>();
      final done = Completer<void>();
      final listener = connection
          .subscribe(characteristic())
          .listen((_) {}, onError: failure.complete, onDone: done.complete);
      (await backend.waitFor<void>('subscribe'))
          .fail(const BleException(BleErrorCode.gattFailure, 'Setup failed'));
      expect(await failure.future, error(BleErrorCode.gattFailure));
      (await backend.waitFor<void>('unsubscribe')).complete(null);
      await done.future;
      await listener.cancel();
      expect(native.subscriptions, isEmpty);
      final next = connection.subscribe(characteristic()).listen((_) {});
      (await backend.waitFor<void>('subscribe')).complete(null);
      final finalCancel = next.cancel();
      (await backend.waitFor<void>('unsubscribe')).complete(null);
      await finalCancel;
      expect(native.subscriptions, isEmpty);
    });

    for (final stage in ['subscribe', 'unsubscribe']) {
      for (final engineClose in [true, false]) {
        test(
          'generation close during $stage releases cancellation; engine=$engineClose',
          () async {
            final (connection, native) = await connect(ble, backend);
            final listener = connection
                .subscribe(characteristic())
                .listen((_) {}, onError: (_) {});
            final setup = await backend.waitFor<void>('subscribe');
            if (stage == 'unsubscribe') {
              setup.complete(null);
              final ready = connection.readRssi();
              (await backend.waitFor<int>('rssi')).complete(-50);
              await ready;
            }
            final cancellation = listener.cancel();
            if (stage == 'unsubscribe') {
              await backend.waitFor<void>('unsubscribe');
            }
            if (engineClose) {
              await ble.close();
            } else {
              native.remoteDisconnect();
            }
            await cancellation;
            await listener.cancel();
            expect(native.subscriptions, isEmpty);
            expect(backend.pending, isEmpty);
            expect(backend.liveConnections, 0);
          },
        );
      }
    }

    test(
      'cached notification streams can acquire a fresh broker after teardown',
      () async {
        final (connection, _) = await connect(ble, backend);
        final stream = connection.subscribe(characteristic());
        for (var cycle = 0; cycle < 3; cycle++) {
          final listener = stream.listen((_) {});
          (await backend.waitFor<void>('subscribe')).complete(null);
          final cancellation = listener.cancel();
          (await backend.waitFor<void>('unsubscribe')).complete(null);
          await cancellation;
          final barrier = connection.readRssi();
          (await backend.waitFor<int>('rssi')).complete(-50);
          await barrier;
        }
        expect(backend.history.where((name) => name == 'subscribe').length, 3);
        expect(backend.subscriptions, 0);
      },
    );

    test('GATT errors preserve native details and acquire portable operation context', () async {
      final (connection, _) = await connect(ble, backend);
      final result = connection.read(characteristic());
      final assertion = expectLater(
        result,
        throwsA(
          error(BleErrorCode.gattFailure)
              .having((e) => e.context.deviceId, 'deviceId', device)
              .having(
                (e) => e.context.connectionGeneration,
                'generation',
                connection.generation,
              )
              .having((e) => e.context.operation, 'operation', 'read')
              .having((e) => e.context.nativeCode, 'nativeCode', '42'),
        ),
      );
      (await backend.waitFor<Uint8List>('read')).fail(
        const BleException(
          BleErrorCode.gattFailure,
          'Platform failure',
          context: BleErrorContext(nativeCode: '42'),
        ),
      );
      await assertion;
    });

    test(
      'descriptor, binary read, and RSSI operations preserve typed results',
      () async {
        final (connection, _) = await connect(ble, backend);
        final descriptor = BleDescriptor(
          serviceUuid: BleUuid('180f'),
          characteristicUuid: BleUuid('2a19'),
          uuid: BleUuid('2902'),
        );
        final read = connection.readDescriptor(descriptor);
        final bytes = Uint8List.fromList([0, 255]);
        (await backend.waitFor<Uint8List>('readDescriptor')).complete(bytes);
        final result = await read;
        bytes[0] = 42;
        expect(result, [0, 255]);
        final rssi = connection.readRssi();
        (await backend.waitFor<int>('rssi')).complete(-60);
        expect(await rssi, -60);
      },
    );

    test(
      'idle adapter stream failure is handled and preserves diagnostic cause',
      () async {
        await ble.ready;
        final diagnostic = ble.diagnostics.firstWhere(
          (event) => event.name == 'backend.error',
        );
        const cause = BleException(
          BleErrorCode.internal,
          'Adapter callback failed',
          context: BleErrorContext(
            operation: 'adapterState',
            nativeMessage: 'original platform cause',
          ),
        );
        backend.failAdapterState(cause);
        final event = await diagnostic;
        expect(event.errorCode, 'internal');
        expect(event.nativeMessage, 'original platform cause');
        expect(event.operation, 'adapterState');
        expect(await ble.adapterState.first, BleAdapterState.unavailable);
      },
    );

    test(
      'adapter stream failure retires GATT and cancels pending connect',
      () async {
        final (connection, _) = await connect(ble, backend);
        final read = connection.read(characteristic());
        final readError = expectLater(
          read,
          throwsA(error(BleErrorCode.disconnected)),
        );
        await backend.waitFor<Uint8List>('read');
        final connecting = ble.connect(const BleDeviceId('another-device'));
        final connectError = expectLater(
          connecting,
          throwsA(error(BleErrorCode.internal)),
        );
        await backend.waitFor<BackendConnection>('connect');
        backend.failAdapterState(
          const BleException(
            BleErrorCode.internal,
            'Adapter event source failed',
          ),
        );
        await Future.wait([readError, connectError]);
        expect(connection.state, BleConnectionState.disconnected);
        expect(backend.liveConnections, 0);
        expect(backend.pending, isEmpty);
      },
    );

    test('adapter loss invalidates connections', () async {
      final (connection, _) = await connect(ble, backend);
      backend.setAdapterState(BleAdapterState.disabled);
      expect(connection.state, BleConnectionState.disconnected);
      await expectLater(
        connection.read(characteristic()),
        throwsA(error(BleErrorCode.disconnected)),
      );
    });

    test('close cancels scan startup, connection, and requests with zero resources', () async {
      final (connection, _) = await connect(ble, backend);
      final listener = ble.scan().listen((_) {}, onError: (_) {});
      await backend.waitFor<void>('scan.start');
      final read = connection.read(characteristic());
      final assertion = expectLater(
        read,
        throwsA(error(BleErrorCode.disconnected)),
      );
      await backend.waitFor<Uint8List>('read');
      await ble.close();
      await ble.close();
      await assertion;
      await listener.cancel();
      expect(backend.pending, isEmpty);
      expect(backend.liveConnections, 0);
      expect(backend.subscriptions, 0);
      expect(backend.scanning, isFalse);
    });
  });

  test(
    '100 scanner/connection/notification cycles leave zero fake resources',
    () async {
      final backend = FakeBleBackend();
      final ble = Ble(backend: backend);
      addTearDown(ble.close);
      var generation = 0;
      for (var cycle = 0; cycle < 100; cycle++) {
        final scan = ble.scan().listen((_) {});
        final start = await backend.waitFor<void>('scan.start');
        start.complete(null);
        await start.request.result;
        final stopping = scan.cancel();
        (await backend.waitFor<void>('scan.stop')).complete(null);
        await stopping;
        final (connection, native) = await connect(ble, backend);
        expect(connection.generation, greaterThan(generation));
        generation = connection.generation;
        final listener = connection.subscribe(characteristic()).listen((_) {});
        (await backend.waitFor<void>('subscribe')).complete(null);
        final cancellation = listener.cancel();
        (await backend.waitFor<void>('unsubscribe')).complete(null);
        await cancellation;
        final closing = connection.disconnect();
        (await backend.waitFor<void>('disconnect')).complete(null);
        await closing;
        expect(native.subscriptions, isEmpty);
        expect(backend.pending, isEmpty);
        expect(backend.liveConnections, 0);
        expect(backend.subscriptions, 0);
        expect(backend.scanning, isFalse);
      }
    },
  );

  for (final arrivesBeforeStopBarrier in [true, false]) {
    test(
      'new notification owner during stop preserves departing acknowledgement; early=$arrivesBeforeStopBarrier',
      () async {
        final backend = FakeBleBackend();
        final ble = Ble(backend: backend);
        addTearDown(ble.close);
        final (connection, native) = await connect(ble, backend);
        final first = connection.subscribe(characteristic()).listen((_) {});
        final initialSetup = await backend.waitFor<void>('subscribe');
        initialSetup.complete(null);
        await initialSetup.request.result;
        final ready = connection.readRssi();
        (await backend.waitFor<int>('rssi')).complete(-50);
        await ready;
        var cancelled = false;
        final cancellation = first.cancel().then((_) => cancelled = true);
        FakeOperation<void>? stop;
        if (!arrivesBeforeStopBarrier) {
          stop = await backend.waitFor<void>('unsubscribe');
        }
        final values = <Uint8List>[];
        final received = Completer<void>();
        final next = connection.subscribe(characteristic()).listen((value) {
          values.add(value);
          if (!received.isCompleted) received.complete();
        });
        stop ??= await backend.waitFor<void>('unsubscribe');
        expect(cancelled, isFalse, reason: 'Stop is still pending');
        stop.complete(null);
        final replacementSetup = await backend.waitFor<void>('subscribe');
        // Await cancellation with replacement setup deliberately unresolved.
        await cancellation;
        expect(cancelled, isTrue);
        replacementSetup.complete(null);
        await replacementSetup.request.result;
        native.emitNotification(characteristic(), Uint8List.fromList([42]));
        await received.future;
        expect(values.single, [42]);
        final finalCancel = next.cancel();
        (await backend.waitFor<void>('unsubscribe')).complete(null);
        await finalCancel;
        expect(native.subscriptions, isEmpty);
        expect(backend.pending, isEmpty);
      },
      timeout: const Timeout(Duration(seconds: 5)),
    );
  }

  test('virtual clock: running timeout cancels backend and unblocks FIFO', () {
    fakeAsync((clock) {
      final backend = FakeBleBackend();
      final ble = Ble(
        backend: backend,
        timeouts: const BleTimeouts(read: Duration(seconds: 1)),
      );
      BleConnection? connection;
      ble.connect(device).then((result) => connection = result);
      clock.flushMicrotasks();
      backend.completeConnect(device);
      clock.flushMicrotasks();
      final failures = <BleErrorCode>[];
      final diagnostics = <BleDiagnostic>[];
      ble.diagnostics.listen(diagnostics.add);
      connection!.read(characteristic()).catchError((Object failure) {
        failures.add((failure as BleException).code);
        return Uint8List(0);
      });
      connection!.write(characteristic(), Uint8List.fromList([1]));
      clock.flushMicrotasks();
      final late = backend.next<Uint8List>('read');
      clock.elapse(const Duration(seconds: 1));
      clock.flushMicrotasks();
      expect(failures, [BleErrorCode.timeout]);
      expect(late.completed, isTrue);
      late.complete(Uint8List.fromList([99]));
      backend.next<void>('write').complete(null);
      clock.flushMicrotasks();
      expect(backend.pending, isEmpty);
      final failed = diagnostics.singleWhere(
        (event) => event.name == 'gatt.read.failed',
      );
      expect(failed.operation, 'read');
      expect(failed.errorCode, 'timeout');
      expect(failed.duration, isNotNull);
      expect(failed.duration!.isNegative, isFalse);
      final completed = diagnostics.singleWhere(
        (event) => event.name == 'gatt.write.completed',
      );
      expect(completed.operation, 'write');
      expect(completed.duration, isNotNull);
      expect(completed.duration!.isNegative, isFalse);
      ble.close();
      clock.flushMicrotasks();
      expect(clock.nonPeriodicTimerCount, 0);
    });
  });

  test(
    'virtual clock: connect timeout and cancel/completion races are terminal',
    () {
      fakeAsync((clock) {
        final backend = FakeBleBackend();
        final ble = Ble(
          backend: backend,
          timeouts: const BleTimeouts(connect: Duration(seconds: 1)),
        );
        final failures = <BleErrorCode>[];
        ble
            .connect(device)
            .then<void>(
              (_) => fail('Unexpected connection'),
              onError: (Object failure) {
                failures.add((failure as BleException).code);
              },
            );
        clock.flushMicrotasks();
        clock.elapse(const Duration(seconds: 1));
        clock.flushMicrotasks();
        expect(failures, [BleErrorCode.timeout]);
        expect(backend.pending, isEmpty);
        final cancellation = BleCancellation();
        ble
            .connect(device, cancellation: cancellation)
            .then<void>(
              (_) => fail('Unexpected connection'),
              onError: (Object failure) {
                failures.add((failure as BleException).code);
              },
            );
        clock.flushMicrotasks();
        cancellation.cancel();
        cancellation.cancel();
        clock.flushMicrotasks();
        expect(failures, [BleErrorCode.timeout, BleErrorCode.cancelled]);
        ble.close();
        clock.flushMicrotasks();
        expect(clock.nonPeriodicTimerCount, 0);
      });
    },
  );
}
