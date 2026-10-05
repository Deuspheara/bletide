@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';
import 'package:bletide/bletide.dart';
import 'package:bletide/src/native/bindings.dart' as bindings;
import 'package:bletide/src/native/event_bridge.dart';
import 'package:bletide/src/native/native_backend.dart';
import 'package:test/test.dart';

import 'support/native_fixture_contracts.dart';

TypeMatcher<BleException> error(BleErrorCode code) =>
    isA<BleException>().having((e) => e.code, 'code', code);

// Debug-only counters stay outside the package's public ABI/API.
@Native<Int64 Function()>(
  symbol: 'bletide_resource_counts',
  assetId: 'package:bletide/src/native/bindings.dart',
)
external int resourceCounts();

@Native<Int64 Function(Uint32)>(
  symbol: 'bletide_worker_resource_count',
  assetId: 'package:bletide/src/native/bindings.dart',
)
external int workerResourceCount(int kind);

@Native<Int64 Function(Int64, Pointer<Void>)>(
  symbol: 'bletide_open_fixture',
  assetId: 'package:bletide/src/native/bindings.dart',
)
external int openFixtureForCapacity(int port, Pointer<Void> post);

List<int> workerResources() => List.generate(4, workerResourceCount);

void openIdleEngine(SendPort parent) {
  final port = RawReceivePort((dynamic _) {});
  final engine = bindings.openEngine(
    port.sendPort.nativePort,
    NativeApi.postCObject.cast(),
  );
  parent.send(engine);
}

// Dart_CObject_kNull reads only its type tag. Zero-initialized, aligned storage
// is large enough for the native union on each supported ABI; posting copies it.
bool postNull(int port) {
  final object = calloc<Uint64>(6);
  try {
    return NativeApi.postCObject
        .cast<NativeFunction<Bool Function(Int64, Pointer<Void>)>>()
        .asFunction<bool Function(int, Pointer<Void>)>()(port, object.cast());
  } finally {
    calloc.free(object);
  }
}

final class _NativeTypedData extends Struct {
  @Int32()
  external int kind;
  @IntPtr()
  external int length;
  external Pointer<Uint8> data;
}

final class _NativeObjectValue extends Union {
  external _NativeTypedData typed;
  @Array(5)
  external Array<UintPtr> storage;
  // The native union includes these members. They also provide its 8-byte
  // alignment on ARMv7, where pointer-sized storage alone has alignment 4.
  @Int64()
  external int integer;
  @Double()
  external double floating;
}

final class _NativeObject extends Struct {
  @Int32()
  external int kind;
  external _NativeObjectValue value;
}

bool postEvent(int port, int kind, int request, int code, [String cause = '']) {
  final payload = utf8.encode(cause);
  final bytes = Uint8List(16 + payload.length);
  final header = ByteData.sublistView(bytes);
  header.setUint32(0, kind, Endian.little);
  header.setUint64(4, request, Endian.little);
  header.setUint32(12, code, Endian.little);
  bytes.setRange(16, bytes.length, payload);
  final object = calloc<_NativeObject>();
  final data = calloc<Uint8>(bytes.length);
  try {
    data.asTypedList(bytes.length).setAll(0, bytes);
    object.ref.kind = 7; // Dart_CObject_kTypedData: VM copies before returning.
    object.ref.value.typed.kind = 2; // Dart_TypedData_kUint8.
    object.ref.value.typed.length = bytes.length;
    object.ref.value.typed.data = data;
    return NativeApi.postCObject
        .cast<NativeFunction<Bool Function(Int64, Pointer<Void>)>>()
        .asFunction<bool Function(int, Pointer<Void>)>()(port, object.cast());
  } finally {
    calloc.free(data);
    calloc.free(object);
  }
}

void main() {
  test(
    'actual FFI preserves native error codes for replies and scan events',
    () async {
      final baseline = resourceCounts();
      final bridge = NativeEventBridge(openEngine: openFixtureForCapacity);
      final backend = NativeBleBackend(transport: bridge);
      try {
        await backend.ready;
        final reply = bridge.submit(
          60,
          Uint8List.fromList([7]),
          operationName: 'controlledRead',
          timeout: const Duration(seconds: 5),
        );
        await expectLater(
          reply.result,
          throwsA(
            error(BleErrorCode.unknown)
                .having((e) => e.context.nativeCode, 'OS code', '133')
                .having(
                  (e) => e.context.operation,
                  'operation',
                  'controlledRead',
                )
                .having(
                  (e) => e.context.nativeMessage,
                  'cause',
                  'Controlled native platform failure',
                ),
          ),
        );
        await expectLater(
          bridge
              .submit(
                60,
                Uint8List.fromList([10]),
                timeout: const Duration(seconds: 5),
                operationName: 'controlledAccess',
              )
              .result,
          throwsA(
            error(BleErrorCode.permissionDenied)
                .having((e) => e.context.nativeCode, 'HRESULT', '0x80070005')
                .having(
                  (e) => e.context.nativeMessage,
                  'cause',
                  'Controlled native access failure',
                ),
          ),
        );
        await backend.startScan().result;
        final scan = expectLater(
          backend.advertisements.first,
          throwsA(
            error(BleErrorCode.unknown)
                .having((e) => e.context.nativeCode, 'OS code', '3')
                .having((e) => e.context.operation, 'operation', 'scan')
                .having(
                  (e) => e.context.nativeMessage,
                  'cause',
                  'Controlled native scan failure',
                ),
          ),
        );
        await bridge
            .submit(
              60,
              Uint8List.fromList([8]),
              operationName: 'controlledScanFailure',
              timeout: const Duration(seconds: 5),
            )
            .result;
        await scan;
        await backend.startScan().result;
        await backend.stopScan().result;
        expect(bridge.pendingCount, 0);
      } finally {
        await backend.close();
      }
      expect(resourceCounts(), baseline);
    },
  );
  registerNativeFixtureContracts(resourceCounts, workerResources);
  test('process engine capacity rejects excess FFI opens and reopens after joined close', () async {
    final baseline = resourceCounts();
    final workers = workerResources();
    final bridges = <NativeEventBridge>[];
    try {
      for (var i = baseline >> 32; i < 128; i++) {
        bridges.add(NativeEventBridge(openEngine: openFixtureForCapacity));
      }
      expect(resourceCounts() >> 32, 128);
      BleException? failure;
      try {
        bridges.add(NativeEventBridge(openEngine: openFixtureForCapacity));
      } on BleException catch (error) {
        failure = error;
      }
      expect(failure, error(BleErrorCode.invalidState));
      expect(failure!.context.operation, 'initialize');
      expect(resourceCounts() >> 32, 128);
    } finally {
      await Future.wait(bridges.map((bridge) => bridge.close()));
    }
    expect(bridges.every((bridge) => bridge.pendingCount == 0), isTrue);
    expect(resourceCounts(), baseline);
    expect(workerResources(), workers);
    final reopened = NativeEventBridge(openEngine: openFixtureForCapacity);
    await reopened.close();
    expect(resourceCounts(), baseline);
    expect(workerResources(), workers);
  });
  for (final queuedBeforeClose in [true, false]) {
    test(
      'ordinary native event queued at close drains before port retirement; before=$queuedBeforeClose',
      () async {
        final baseline = resourceCounts();
        final workers = workerResources();
        late int port;
        final bridge = NativeEventBridge(
          openEngine: (nativePort, post) {
            port = nativePort;
            return bindings.openEngine(nativePort, post);
          },
          closeEngine: (engine) {
            if (!queuedBeforeClose) {
              expect(postEvent(port, 4, 0, 0, 'queued BLE event'), isTrue);
            }
            return bindings.closeEngine(engine);
          },
        );
        addTearDown(bridge.close);
        final values = <Uint8List>[];
        final done = Completer<void>();
        final listener = bridge.events.listen(
          values.add,
          onDone: done.complete,
        );
        addTearDown(listener.cancel);
        final pending = bridge.submit(
          3,
          Uint8List(0),
          timeout: const Duration(minutes: 1),
        );
        final cancelled = expectLater(
          pending.result,
          throwsA(error(BleErrorCode.disposed)),
        );
        if (queuedBeforeClose) {
          expect(postEvent(port, 4, 0, 0, 'queued BLE event'), isTrue);
        }
        // No event-loop yield between enqueue and close: the event is still in flight.
        expect(values, isEmpty);
        final closing = bridge.close();
        expect(identical(closing, bridge.close()), isTrue);
        await closing;
        await done.future;
        await cancelled;
        expect(values, hasLength(1));
        expect(
          ByteData.sublistView(values.single).getUint32(0, Endian.little),
          4,
        );
        expect(utf8.decode(values.single.sublist(16)), 'queued BLE event');
        expect(bridge.pendingCount, 0);
        expect(postEvent(port, 4, 0, 0, 'after close'), isFalse);
        expect(resourceCounts(), baseline);
        expect(workerResources(), workers);
      },
    );
  }

  test(
    'unexpected shutdown request cannot replace the owned acknowledgement',
    () async {
      late int port;
      var closeCalls = 0;
      final bridge = NativeEventBridge(
        openEngine: (nativePort, _) {
          port = nativePort;
          return 999999;
        },
        closeEngine: (_) {
          closeCalls++;
          return 41;
        },
      );
      final failed = Completer<Object>();
      final done = Completer<void>();
      final listener = bridge.events.listen(
        (_) {},
        onError: (Object e) {
          if (!failed.isCompleted) failed.complete(e);
        },
        onDone: () {
          if (!failed.isCompleted) {
            failed.completeError(
              StateError('port closed before the owned acknowledgement'),
            );
          }
          done.complete();
        },
      );
      addTearDown(listener.cancel);
      expect(postEvent(port, 2, 99, 0), isTrue);
      expect(
        await failed.future,
        error(BleErrorCode.internal)
            .having((e) => e.context.operation, 'operation', 'decodeEvent'),
      );
      var completed = false;
      final closing = bridge.close();
      final observed = closing.then((_) {
        completed = true;
      });
      expect(identical(closing, bridge.close()), isTrue);
      await Future<void>(() {});
      expect(completed, isFalse);
      expect(closeCalls, 1);
      expect(postEvent(port, 2, 41, 0), isTrue);
      await observed;
      await done.future;
      expect(postNull(port), isFalse);
    },
  );
  test(
    'wrong shutdown acknowledgement while closing leaves the join pending',
    () async {
      late int port;
      final bridge = NativeEventBridge(
        openEngine: (nativePort, _) {
          port = nativePort;
          return 999999;
        },
        closeEngine: (_) => 41,
      );
      final failure = Completer<Object>();
      final listener = bridge.events.listen(
        (_) {},
        onError: (Object e) {
          if (!failure.isCompleted) failure.complete(e);
        },
      );
      addTearDown(listener.cancel);
      final closing = bridge.close();
      final assertion = expectLater(
        closing,
        throwsA(
          error(BleErrorCode.gattFailure)
              .having((e) => e.message, 'cause', 'cleanup failed'),
        ),
      );
      expect(postEvent(port, 2, 42, 0), isTrue);
      expect(await failure.future, error(BleErrorCode.internal));
      expect(postEvent(port, 2, 41, 15, 'cleanup failed'), isTrue);
      await assertion;
      expect(identical(closing, bridge.close()), isTrue);
      expect(postNull(port), isFalse);
    },
  );
  test('unsolicited terminal failure is reported and cached for a late close caller', () async {
    late int port;
    var closeCalls = 0;
    final bridge = NativeEventBridge(
      openEngine: (nativePort, _) {
        port = nativePort;
        return 999999;
      },
      closeEngine: (_) {
        closeCalls++;
        return 41;
      },
    );
    final errors = <Object>[];
    final done = Completer<void>();
    final listener = bridge.events.listen(
      (_) {},
      onError: errors.add,
      onDone: done.complete,
    );
    addTearDown(listener.cancel);
    expect(postEvent(port, 2, 0, 15, 'supervisor cleanup failed'), isTrue);
    await done.future;
    await Future<void>(() {});
    expect(errors.single, error(BleErrorCode.gattFailure));
    final closing = bridge.close();
    expect(identical(closing, bridge.close()), isTrue);
    await expectLater(
      closing,
      throwsA(
        error(BleErrorCode.gattFailure)
            .having((e) => e.message, 'cause', 'supervisor cleanup failed'),
      ),
    );
    expect(closeCalls, 0);
    expect(postNull(port), isFalse);
  });
  for (final nativeCloseResult in [0, 41]) {
    test(
      'supervisor terminal event joins close returning $nativeCloseResult',
      () async {
        late int port;
        final bridge = NativeEventBridge(
          openEngine: (nativePort, _) {
            port = nativePort;
            return 999999;
          },
          closeEngine: (_) => nativeCloseResult,
        );
        final events = <Object>[];
        final listener = bridge.events.listen((_) {}, onError: events.add);
        addTearDown(listener.cancel);
        var completed = false;
        final closing = bridge.close();
        final assertion =
            expectLater(
              closing,
              throwsA(
                error(BleErrorCode.gattFailure).having(
                  (e) => e.message,
                  'cause',
                  'late supervisor cleanup failure',
                ),
              ),
            ).then((_) {
              completed = true;
            });
        await Future<void>(() {});
        expect(completed, isFalse);
        expect(
          postEvent(port, 2, 0, 15, 'late supervisor cleanup failure'),
          isTrue,
        );
        await assertion;
        expect(events, isEmpty);
        expect(identical(closing, bridge.close()), isTrue);
        expect(postNull(port), isFalse);
      },
    );
  }
  test('shutdown entry exception settles the cached close future and closes its port', () async {
    late int port;
    var calls = 0;
    final bridge = NativeEventBridge(
      openEngine: (nativePort, post) {
        port = nativePort;
        return 999999;
      },
      closeEngine: (_) {
        calls++;
        throw StateError('shutdown entry failed');
      },
    );
    final done = bridge.events.drain<void>();
    final closing = bridge.close();
    expect(identical(closing, bridge.close()), isTrue);
    await expectLater(
      closing,
      throwsA(
        error(BleErrorCode.internal)
            .having((e) => e.context.operation, 'operation', 'close')
            .having(
              (e) => e.context.nativeMessage,
              'cause',
              contains('shutdown entry failed'),
            ),
      ),
    );
    await done;
    expect(calls, 1);
    expect(postNull(port), isFalse);
  });
  test('engine-open exception closes its newly created native port', () {
    late int port;
    final failure = StateError('engine entry failed');
    expect(
      () => NativeEventBridge(
        openEngine: (nativePort, post) {
          port = nativePort;
          throw failure;
        },
      ),
      throwsA(same(failure)),
    );
    expect(postNull(port), isFalse);
  });
  test(
    'malformed native-port message reports cause and joins real engine close',
    () async {
      final baseline = resourceCounts();
      final workers = workerResources();
      late int port;
      final bridge = NativeEventBridge(
        openEngine: (nativePort, post) {
          port = nativePort;
          return bindings.openEngine(nativePort, post);
        },
      );
      addTearDown(bridge.close);
      final received = Completer<Object>();
      final done = Completer<void>();
      final listener = bridge.events.listen(
        (_) => fail('Malformed port data must not become a BLE event'),
        onError: (Object error, StackTrace stack) => received.complete(error),
        onDone: done.complete,
      );
      addTearDown(listener.cancel);
      final pending = bridge.submit(
        3,
        Uint8List(0),
        timeout: const Duration(minutes: 1),
      );
      final assertion = expectLater(
        pending.result,
        throwsA(error(BleErrorCode.internal)),
      );
      expect(postNull(port), isTrue);
      expect(
        await received.future,
        error(BleErrorCode.internal)
            .having((e) => e.context.operation, 'operation', 'decodeEvent')
            .having(
              (e) => e.context.nativeMessage,
              'cause',
              'Malformed native event',
            ),
      );
      await done.future;
      await assertion;
      await bridge.close();
      expect(bridge.pendingCount, 0);
      expect(postNull(port), isFalse);
      expect(resourceCounts(), baseline);
      expect(workerResources(), workers);
    },
  );
  test('automatic port-error shutdown owns failure before a late close listener', () async {
    late int port;
    var closeCalls = 0;
    final bridge = NativeEventBridge(
      openEngine: (nativePort, post) {
        port = nativePort;
        return 999999;
      },
      closeEngine: (_) {
        closeCalls++;
        return -18;
      },
    );
    final failures = <Object>[];
    final done = Completer<void>();
    final listener = bridge.events.listen(
      (_) {},
      onError: failures.add,
      onDone: done.complete,
    );
    addTearDown(listener.cancel);
    expect(postNull(port), isTrue);
    await done.future;
    // A listener installed after this event turn cannot hide an unowned error.
    await Future<void>(() {});
    expect(failures.single, error(BleErrorCode.internal));
    final closing = bridge.close();
    expect(identical(closing, bridge.close()), isTrue);
    await expectLater(
      closing,
      throwsA(
        error(BleErrorCode.internal)
            .having((e) => e.message, 'message', 'Native shutdown rejected'),
      ),
    );
    expect(closeCalls, 1);
    expect(postNull(port), isFalse);
  });
  test(
    'killed idle isolate releases native engine without a command or BLE event',
    () async {
      final baseline = resourceCounts();
      final workers = workerResources();
      final started = ReceivePort();
      final exited = ReceivePort();
      addTearDown(started.close);
      addTearDown(exited.close);
      final isolate = await Isolate.spawn(
        openIdleEngine,
        started.sendPort,
        onExit: exited.sendPort,
      );
      addTearDown(() => isolate.kill(priority: Isolate.immediate));
      final handle = await started.first as int;
      expect(handle, greaterThan(0));
      expect(resourceCounts() >> 32, (baseline >> 32) + 1);
      isolate.kill(priority: Isolate.immediate);
      await exited.first;
      // Poll authoritative counters, not a delay used as evidence of cleanup.
      final deadline = Stopwatch()..start();
      while (resourceCounts() != baseline &&
          deadline.elapsed < const Duration(seconds: 5)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(resourceCounts(), baseline);
      expect(workerResources(), workers);
      expect(bindings.closeEngine(handle), 0);
    },
  );
  test(
    'malformed correlated close metadata fails cached close without hanging',
    () async {
      late int port;
      final bridge = NativeEventBridge(
        openEngine: (nativePort, _) {
          port = nativePort;
          return 4242;
        },
        closeEngine: (_) => 77,
      );
      final closing = bridge.close();
      final failure = expectLater(
        closing,
        throwsA(
          error(BleErrorCode.internal).having(
            (e) => e.context.nativeMessage,
            'cause',
            'Malformed native error metadata',
          ),
        ),
      );
      expect(postEvent(port, 2, 77, 0x80000013, 'x'), isTrue);
      await failure;
      expect(identical(bridge.close(), closing), isTrue);
      expect(postEvent(port, 4, 0, 0), isFalse);
    },
  );
  test(
    'malformed unsolicited error metadata joins real engine shutdown',
    () async {
      final baseline = resourceCounts();
      late int port;
      final bridge = NativeEventBridge(
        openEngine: (nativePort, post) {
          port = nativePort;
          return bindings.openEngine(nativePort, post);
        },
      );
      final received = bridge.events.first;
      final errorReceived = expectLater(
        received,
        throwsA(
          error(BleErrorCode.internal).having(
            (e) => e.context.nativeMessage,
            'cause',
            'Malformed native error metadata',
          ),
        ),
      );
      expect(postEvent(port, 2, 0, 0x80000013, 'x'), isTrue);
      await errorReceived;
      await bridge.close();
      expect(bridge.pendingCount, 0);
      expect(resourceCounts(), baseline);
      expect(postEvent(port, 4, 0, 0), isFalse);
    },
  );
  test('ABI version and mismatch are checked before engine creation', () {
    expect(bindings.abiVersion(), 2);
    expect(
      () => NativeEventBridge(expectedAbi: 1),
      throwsA(error(BleErrorCode.internal)),
    );
  });
  test(
    'Dart → FFI → Rust → native port → Dart copies arbitrary binary',
    () async {
      final bridge = NativeEventBridge();
      addTearDown(bridge.close);
      final bytes = Uint8List.fromList(
        List.generate(4096, (index) => index % 256),
      );
      final request = bridge.submit(
        1,
        bytes,
        timeout: const Duration(seconds: 5),
      );
      bytes.fillRange(0, bytes.length, 0);
      expect(await request.result, List.generate(4096, (index) => index % 256));
      expect(bridge.pendingCount, 0);
    },
  );
  test('native connection registry rejects stale generations and malformed GATT commands', () async {
    final bridge = NativeEventBridge();
    addTearDown(bridge.close);
    final stale = ByteData(8)..setUint64(0, 999999, Endian.little);
    await expectLater(
      bridge
          .submit(
            41,
            stale.buffer.asUint8List(),
            timeout: const Duration(seconds: 5),
          )
          .result,
      throwsA(error(BleErrorCode.disconnected)),
    );
    await expectLater(
      bridge
          .submit(
            41,
            Uint8List.fromList([1]),
            timeout: const Duration(seconds: 5),
          )
          .result,
      throwsA(error(BleErrorCode.invalidState)),
    );
    await bridge
        .submit(
          31,
          stale.buffer.asUint8List(),
          timeout: const Duration(seconds: 5),
        )
        .result;
    expect(bridge.pendingCount, 0);
  });
  test('controlled native error retains typed context', () async {
    final bridge = NativeEventBridge();
    addTearDown(bridge.close);
    await expectLater(
      bridge
          .submit(
            2,
            Uint8List(0),
            timeout: const Duration(seconds: 5),
            operationName: 'probe',
          )
          .result,
      throwsA(
        error(BleErrorCode.notSupported)
            .having((e) => e.context.operation, 'operation', 'probe')
            .having((e) => e.context.nativeCode, 'nativeCode', '11'),
      ),
    );
  });
  test('explicit cancellation and native timeout clean requests', () async {
    final bridge = NativeEventBridge();
    addTearDown(bridge.close);
    final request = bridge.submit(
      3,
      Uint8List(0),
      timeout: const Duration(seconds: 5),
    );
    final assertion = expectLater(
      request.result,
      throwsA(error(BleErrorCode.cancelled)),
    );
    request.cancel();
    request.cancel();
    await assertion;
    await expectLater(
      bridge
          .submit(3, Uint8List(0), timeout: const Duration(milliseconds: 1))
          .result,
      throwsA(error(BleErrorCode.timeout)),
    );
    expect(bridge.pendingCount, 0);
  });
  test(
    'close with pending requests acknowledges native task termination',
    () async {
      final bridge = NativeEventBridge();
      final waiting = bridge.submit(
        3,
        Uint8List(0),
        timeout: const Duration(minutes: 1),
      );
      final assertion = expectLater(
        waiting.result,
        throwsA(error(BleErrorCode.disposed)),
      );
      final close = bridge.close();
      expect(identical(close, bridge.close()), isTrue);
      await close;
      await assertion;
      expect(bridge.pendingCount, 0);
      await expectLater(
        bridge
            .submit(1, Uint8List(0), timeout: const Duration(seconds: 1))
            .result,
        throwsA(error(BleErrorCode.disposed)),
      );
    },
  );
  test('100 repeated real FFI create/close cycles', () async {
    final baseline = resourceCounts();
    final workers = workerResources();
    for (var i = 0; i < 100; i++) {
      final bridge = NativeEventBridge();
      await bridge
          .submit(
            1,
            Uint8List.fromList([i, 255]),
            timeout: const Duration(seconds: 5),
          )
          .result;
      await bridge.close();
      expect(bridge.pendingCount, 0);
      expect(resourceCounts(), baseline);
      expect(workerResources(), workers);
    }
  });
  test('unknown native handles are rejected and close is idempotent', () {
    expect(workerResourceCount(4), -16);
    expect(workerResourceCount(0xffffffff), -16);
    expect(bindings.closeEngine(0), 0);
    expect(bindings.cancel(0, 0), 0);
  });
}
