import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import '../errors.dart';
import 'bindings.dart' as bindings;
import 'wire.dart';

/// Internal request token; never exported in the public BLE API.
final class NativeRequest {
  NativeRequest(this.result, this.cancel);
  final Future<Uint8List> result;
  final void Function() cancel;
}

final class _Pending {
  _Pending(this.operation);
  final String operation;
  final Completer<Uint8List> result = Completer();
}

/// Owns a native engine, a ReceivePort, and its pending Dart futures.
/// VM copies every native event. No Rust pointer survives an FFI call.
abstract interface class NativeTransport {
  Stream<Uint8List> get events;
  NativeRequest submit(
    int operation,
    Uint8List payload, {
    required Duration timeout,
    String? operationName,
  });
  Future<void> close();
}

final class NativeEventBridge implements NativeTransport {
  NativeEventBridge({
    int expectedAbi = 2,
    int Function(int, Pointer<Void>) openEngine = bindings.openEngine,
    this.closeEngine = bindings.closeEngine,
  }) {
    final actual = bindings.abiVersion();
    if (actual != expectedAbi) {
      throw BleException(
        BleErrorCode.internal,
        'Native ABI mismatch: expected $expectedAbi, got $actual',
      );
    }
    _port = ReceivePort('bletide events');
    _subscription = _port.listen(_receive);
    try {
      _engine = openEngine(
        _port.sendPort.nativePort,
        NativeApi.postCObject.cast(),
      );
      if (_engine <= 0) {
        throw _error(
          -_engine,
          'Cannot initialize native runtime',
          'initialize',
        );
      }
    } catch (_) {
      _port.close();
      unawaited(_subscription.cancel());
      unawaited(_events.close());
      rethrow;
    }
  }

  late final ReceivePort _port;
  late final StreamSubscription<dynamic> _subscription;
  late final int _engine;
  final int Function(int) closeEngine;
  final Map<int, _Pending> _pending = {};
  Completer<void>? _closing;
  int? _closeRequest;
  bool _closed = false;
  final StreamController<Uint8List> _events = StreamController.broadcast();
  @override
  Stream<Uint8List> get events => _events.stream;
  int get pendingCount => _pending.length;

  @override
  NativeRequest submit(
    int operation,
    Uint8List payload, {
    required Duration timeout,
    String? operationName,
  }) {
    final name = operationName ?? 'command.$operation';
    if (_closing != null || _closed) {
      return NativeRequest(
        Future.error(_error(17, 'Engine closed', name)),
        () {},
      );
    }
    final milliseconds = timeout.inMilliseconds;
    if (milliseconds < 1 ||
        milliseconds > 0xffffffff ||
        payload.length > 1048576) {
      return NativeRequest(
        Future.error(_error(16, 'Invalid request bounds', name)),
        () {},
      );
    }
    final pointer = payload.isEmpty
        ? nullptr.cast<Uint8>()
        : calloc<Uint8>(payload.length);
    final int id;
    try {
      if (payload.isNotEmpty) {
        pointer.asTypedList(payload.length).setAll(0, payload);
      }
      id = bindings.command(
        _engine,
        operation,
        pointer,
        payload.length,
        milliseconds,
      );
    } finally {
      if (payload.isNotEmpty) calloc.free(pointer);
    }
    if (id <= 0) {
      return NativeRequest(
        Future.error(_error(-id, 'Native command rejected', name)),
        () {},
      );
    }
    final pending = _Pending(name);
    _pending[id] = pending;
    return NativeRequest(pending.result.future, () {
      if (!_closed && _pending.containsKey(id)) bindings.cancel(_engine, id);
    });
  }

  BleException _error(int code, String message, String operation) =>
      BleException(
        code >= 1 && code <= BleErrorCode.values.length
            ? BleErrorCode.values[code - 1]
            : BleErrorCode.unknown,
        message,
        context: BleErrorContext(
          platform: Platform.operatingSystem,
          operation: operation,
          nativeCode: '$code',
          nativeMessage: message,
        ),
      );

  BleException _eventError(int code, Uint8List payload, String operation) {
    try {
      return decodeNativeError(
        code,
        payload,
        platform: Platform.operatingSystem,
        operation: operation,
      );
    } on FormatException {
      final failure = _error(
        18,
        'Malformed native error metadata',
        'decodeEvent',
      );
      _fail(failure);
      return failure;
    }
  }

  void _receive(dynamic message) {
    if (_closed) return;
    if (message is! Uint8List || message.length < 16) {
      _fail(_error(18, 'Malformed native event', 'decodeEvent'));
      return;
    }
    final bytes = ByteData.sublistView(message);
    final kind = bytes.getUint32(0, Endian.little);
    // Internal VM-port liveness probe. It owns no request and is not a BLE event.
    if (kind == 9) return;
    final request = bytes.getUint64(4, Endian.little);
    final code = bytes.getUint32(12, Endian.little);
    final payload = Uint8List.fromList(message.sublist(16));
    if (kind == 2) {
      if (request != 0 && (_closing == null || request != _closeRequest)) {
        _fail(
          _error(
            18,
            'Unexpected native shutdown acknowledgement',
            'decodeEvent',
          ),
        );
        return;
      }
      BleException? failure;
      try {
        failure = code == 0
            ? null
            : decodeNativeError(
                code,
                payload,
                platform: Platform.operatingSystem,
                operation: 'close',
              );
      } on FormatException {
        final malformed = _error(
          18,
          'Malformed native error metadata',
          'decodeEvent',
        );
        if (_closing == null) {
          // An undecodable unsolicited event cannot prove native retirement.
          _fail(malformed);
        } else {
          // Correlated acknowledgement proves retirement, but fails its result.
          _finishClose(malformed);
        }
        return;
      }
      final unsolicited = _closing == null;
      if (unsolicited) {
        // Request zero is the supervisor's unsolicited terminal event (e.g.
        // an engine panic). Preserve its cleanup result for later close callers.
        final closing = _closing = Completer<void>();
        unawaited(
          closing.future.then<void>(
            (_) {},
            onError: (Object _, StackTrace _) {},
          ),
        );
      }
      if (unsolicited && failure != null) _events.addError(failure);
      _finishClose(failure);
      return;
    }
    if (kind != 1) {
      _events.add(message.asUnmodifiableView());
      return;
    }
    final pending = _pending.remove(request);
    if (pending == null) {
      return; // Late/duplicate completion cannot complete twice.
    }
    if (code == 0) {
      pending.result.complete(payload);
    } else {
      pending.result.completeError(
        _eventError(code, payload, pending.operation),
      );
    }
  }

  void _failPending(BleException error) {
    final pending = _pending.values.toList();
    _pending.clear();
    for (final request in pending) {
      if (!request.result.isCompleted) request.result.completeError(error);
    }
  }

  void _fail(BleException error) {
    _failPending(error);
    _events.addError(error);
    unawaited(
      close().then<void>(
        (_) {},
        onError: (Object error, StackTrace stack) {
          // The primary event error is already reported. Keep the original close
          // future cached so explicit callers can observe cleanup failure later.
        },
      ),
    );
  }

  void _finishClose(BleException? failure) {
    _failPending(failure ?? _error(17, 'Engine closed', 'close'));
    _closed = true;
    _port.close();
    unawaited(_subscription.cancel());
    unawaited(_events.close());
    final closing = _closing;
    if (closing != null && !closing.isCompleted) {
      if (failure == null) {
        closing.complete();
      } else {
        closing.completeError(failure);
      }
    }
  }

  @override
  Future<void> close() {
    if (_closing != null) return _closing!.future;
    if (_closed) return Future.value();
    final closing = _closing = Completer<void>();
    try {
      final id = closeEngine(_engine);
      _closeRequest = id;
      if (id < 0) {
        _finishClose(_error(-id, 'Native shutdown rejected', 'close'));
      }
      // Zero means the native registry already retired this engine. Its
      // supervisor still owns a terminal event, possibly not posted yet; wait
      // for that result instead of losing cleanup failure by closing the port.
    } catch (error) {
      _finishClose(_error(18, 'Native shutdown entry failed: $error', 'close'));
    }
    return closing.future;
  }
}
