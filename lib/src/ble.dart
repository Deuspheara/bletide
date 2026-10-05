import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'backend.dart';
import 'backend_factory.dart';
import 'errors.dart';
import 'models.dart';

// These parts share private access so engine invalidation can retire connection
// work atomically. They introduce no additional lifecycle owners.
part 'ble/operation.dart';
part 'ble/connection.dart';

/// Owns one backend. Always close explicitly; connections represent one generation.
final class Ble {
  Ble({BleBackend? backend, this.timeouts = const BleTimeouts()})
    : _backend = backend ?? createBackend(timeouts: timeouts) {
    ready = _backend.ready;
    // Observe initialization once. Cancelled retries leave the active registry
    // instead of accumulating callbacks on a possibly never-settling Future.
    unawaited(
      ready.then(
        (_) {
          _initialized = true;
          for (final work in _connecting.values.toList()) {
            if (!work.done) unawaited(work.run());
          }
          if (!_disposed && _scans.isNotEmpty) {
            unawaited(
              _reconcileScan().catchError(
                (Object error, StackTrace stack) => _scanError(error, stack),
              ),
            );
          }
        },
        onError: (Object error, StackTrace stack) {
          _initializationError = error;
          _initializationStack = stack;
          for (final work in _connecting.values.toList()) {
            work.finishError(error, stack);
          }
          if (_scans.isNotEmpty) _scanError(error, stack);
        },
      ),
    );
    _advertisements = _backend.advertisements.listen(
      _advertise,
      onError: _scanError,
    );
    _adapter = _backend.adapterState.listen((state) {
      if (state != BleAdapterState.ready) {
        final code = switch (state) {
          BleAdapterState.unauthorized => BleErrorCode.permissionDenied,
          BleAdapterState.unavailable => BleErrorCode.adapterUnavailable,
          BleAdapterState.unsupported => BleErrorCode.notSupported,
          _ => BleErrorCode.adapterDisabled,
        };
        _adapterError(_exception(code, 'scan'));
      }
    }, onError: _adapterError);
  }
  final BleBackend _backend;
  final BleTimeouts timeouts;
  late final Future<void> ready;
  bool _initialized = false;
  Object? _initializationError;
  StackTrace? _initializationStack;
  late final StreamSubscription<BleAdvertisement> _advertisements;
  late final StreamSubscription<BleAdapterState> _adapter;
  final _diagnostics = StreamController<BleDiagnostic>.broadcast();
  final Map<BleDeviceId, _Work<BackendConnection>> _connecting = {};
  final Map<BleDeviceId, BleConnection> _connections = {};
  final Map<StreamController<BleAdvertisement>, BleScanFilter> _scans = {};
  _Work<BleAdvertisement>? _choosing;
  bool _physicalScan = false;
  Future<void>? _scanTransition;
  bool _disposed = false;
  Future<void>? _closing;
  BleCapabilities get capabilities => _backend.capabilities;
  Stream<BleDiagnostic> get diagnostics => _diagnostics.stream;
  Stream<BleAdapterState> get adapterState => Stream.multi((controller) {
    final subscription = _backend.adapterState.listen(
      controller.add,
      onError: controller.addError,
      onDone: controller.close,
    );
    controller.add(_backend.currentAdapterState);
    controller.onCancel = subscription.cancel;
  });

  void _check(String operation) {
    if (_disposed) throw _exception(BleErrorCode.disposed, operation);
  }

  void _diagnose(
    String name, {
    BleDeviceId? deviceId,
    int? generation,
    String? errorCode,
    String? nativeMessage,
    String? operation,
    Duration? duration,
  }) {
    if (!_diagnostics.isClosed) {
      _diagnostics.add(
        BleDiagnostic(
          name,
          deviceId: deviceId,
          generation: generation,
          errorCode: errorCode,
          nativeMessage: nativeMessage,
          operation: operation,
          duration: duration,
        ),
      );
    }
  }

  /// Call directly in a user gesture. This method deliberately does not await
  /// ready before invoking the browser chooser, preserving transient activation.
  Future<BleAdvertisement> requestDevice({
    BleDeviceRequest? options,
    BleCancellation? cancellation,
    Duration timeout = const Duration(minutes: 1),
  }) {
    if (_disposed || !capabilities.requestDevice || _choosing != null) {
      return Future.error(
        _exception(
          _disposed
              ? BleErrorCode.disposed
              : !capabilities.requestDevice
              ? BleErrorCode.notSupported
              : BleErrorCode.invalidState,
          'requestDevice',
        ),
      );
    }
    final work = _choosing = _Work<BleAdvertisement>(
      'requestDevice',
      () => _backend.requestDevice(options ?? BleDeviceRequest()),
      timeout,
      cancellation: cancellation,
    );
    // run() invokes create synchronously before its first await.
    unawaited(work.run());
    return work.result.future.whenComplete(() {
      if (identical(_choosing, work)) _choosing = null;
    });
  }

  /// A single-listener scan lease. Listening starts shared native discovery;
  /// cancelling the last lease awaits stop. Errors terminate the lease.
  /// Web callers must use [requestDevice] instead.
  Stream<BleAdvertisement> scan({
    BleScanFilter filter = const BleScanFilter(),
  }) {
    late final StreamController<BleAdvertisement> controller;
    final snapshot = BleScanFilter(
      namePrefix: filter.namePrefix,
      serviceUuids: List.unmodifiable(filter.serviceUuids),
    );
    controller = StreamController(
      onListen: () {
        if (_disposed || !capabilities.scan) {
          controller.addError(
            _exception(
              _disposed ? BleErrorCode.disposed : BleErrorCode.notSupported,
              'scan',
            ),
          );
          unawaited(controller.close());
          return;
        }
        _scans[controller] = snapshot;
        unawaited(
          _reconcileScan().catchError(
            (Object error, StackTrace stack) => _scanError(error, stack),
          ),
        );
      },
      onCancel: () {
        _scans.remove(controller);
        return _reconcileScan();
      },
    );
    return controller.stream;
  }

  void _advertise(BleAdvertisement advertisement) {
    if (_disposed) return;
    for (final entry in _scans.entries.toList()) {
      if (!entry.key.isClosed && entry.value.matches(advertisement)) {
        entry.key.add(advertisement);
      }
    }
  }

  void _adapterError(Object error, [StackTrace? stack]) {
    _scanError(error, stack);
    for (final connection in _connections.values.toList()) {
      connection._invalidate();
    }
    final code = error is BleException ? error.code : BleErrorCode.internal;
    for (final request in _connecting.values.toList()) {
      request.abort(code);
    }
  }

  void _scanError(Object error, [StackTrace? stack]) {
    _diagnose(
      'backend.error',
      errorCode: error is BleException ? error.code.name : 'internal',
      nativeMessage: error is BleException
          ? error.context.nativeMessage ?? error.message
          : error.toString(),
      operation: error is BleException ? error.context.operation : null,
    );
    for (final controller in _scans.keys.toList()) {
      if (!controller.isClosed) {
        controller.addError(error, stack);
        unawaited(controller.close());
      }
    }
    _scans.clear();
    unawaited(
      _reconcileScan().catchError((Object error) {
        _diagnose(
          'scan.failed',
          errorCode: error is BleException ? error.code.name : 'internal',
        );
      }),
    );
  }

  Future<void> _reconcileScan() {
    // Pending scanner ownership lives in _scans. Cancelling it or closing the
    // engine must not wait for initialization or retain another ready callback.
    if (!_initialized) {
      if (_initializationError case final error?
          when !_disposed && _scans.isNotEmpty) {
        return Future.error(error, _initializationStack);
      }
      return Future.value();
    }
    if (_scanTransition != null) return _scanTransition!;
    final complete = Completer<void>();
    _scanTransition = complete.future;
    Object? stopError;
    StackTrace? stopStack;
    unawaited(() async {
      try {
        while (_physicalScan != (!_disposed && _scans.isNotEmpty)) {
          if (!_disposed && _scans.isNotEmpty) {
            _physicalScan =
                true; // Startup may already have reached the platform.
            _diagnose('scan.starting');
            final work = _Work<void>(
              'scan.start',
              _backend.startScan,
              timeouts.discovery,
            );
            unawaited(work.run());
            try {
              await work.result.future;
              _diagnose('scan.started');
            } catch (error, stack) {
              _scanError(error, stack);
            }
          } else {
            _physicalScan = false;
            if (_disposed) break; // Backend.close owns physical cleanup.
            _diagnose('scan.stopping');
            final work = _Work<void>(
              'scan.stop',
              _backend.stopScan,
              timeouts.disconnect,
            );
            unawaited(work.run());
            try {
              await work.result.future;
              _diagnose('scan.stopped');
            } catch (error, stack) {
              stopError = error;
              stopStack = stack;
              _diagnose(
                'scan.failed',
                errorCode: error is BleException ? error.code.name : 'internal',
              );
            }
          }
        }
      } catch (error, stack) {
        _scanError(error, stack);
      } finally {
        _scanTransition = null;
        if (stopError == null) {
          complete.complete();
        } else {
          complete.completeError(stopError!, stopStack);
        }
      }
    }());
    return complete.future;
  }

  /// Connects once, including initialization in the connect deadline.
  /// A reconnect returns a new generation; rediscover its attributes.
  /// Duplicate active or pending connects fail explicitly.
  Future<BleConnection> connect(
    BleDeviceId deviceId, {
    BleCancellation? cancellation,
  }) async {
    _check('connect');
    if (!capabilities.connect) {
      throw _exception(BleErrorCode.notSupported, 'connect');
    }
    if (_connections.containsKey(deviceId)) {
      throw _exception(
        BleErrorCode.alreadyConnected,
        'connect',
        deviceId: deviceId,
      );
    }
    if (_connecting.containsKey(deviceId)) {
      throw _exception(
        BleErrorCode.connectInProgress,
        'connect',
        deviceId: deviceId,
      );
    }
    final work = _Work<BackendConnection>(
      'connect',
      () => _backend.connect(deviceId),
      timeouts.connect,
      cancellation: cancellation,
      deviceId: deviceId,
      onLateResult: (connection) {
        unawaited(_discardConnection(connection));
      },
    );
    _connecting[deviceId] = work;
    _diagnose('connect.starting', deviceId: deviceId);
    try {
      // Start the deadline before initialization and observe cancellation even
      // when the backend never becomes ready. A retired work item cannot connect.
      if (_initialized) {
        unawaited(work.run());
      } else if (_initializationError case final error?) {
        work.finishError(error, _initializationStack);
      }
      final backend = await work.result.future;
      if (_disposed) {
        await _discardConnection(backend);
        throw _exception(BleErrorCode.disposed, 'connect');
      }
      final connection = BleConnection._(this, backend);
      _connections[deviceId] = connection;
      _diagnose(
        'connect.connected',
        deviceId: deviceId,
        generation: connection.generation,
      );
      return connection;
    } finally {
      if (identical(_connecting[deviceId], work)) _connecting.remove(deviceId);
    }
  }

  Future<void> _discardConnection(BackendConnection connection) async {
    final work = _Work<void>(
      'disconnect',
      connection.disconnect,
      timeouts.disconnect,
    );
    unawaited(work.run());
    try {
      await work.result.future;
    } catch (error) {
      _diagnose('connect.cleanup.failed', deviceId: connection.deviceId);
    }
  }

  Future<void> disconnect(BleDeviceId deviceId) async {
    _connecting[deviceId]?.abort(BleErrorCode.cancelled);
    await _connections[deviceId]?.disconnect();
  }

  /// Releases this engine and its scans/connections. Idempotent; repeated calls
  /// await the same result, including any physical cleanup failure.
  Future<void> close() => _closing ??= _close();
  Future<void> _close() async {
    _disposed = true;
    _choosing?.abort(BleErrorCode.disposed);
    for (final work in _connecting.values.toList()) {
      work.abort(BleErrorCode.disposed);
    }
    for (final controller in _scans.keys.toList()) {
      unawaited(controller.close());
    }
    _scans.clear();
    for (final connection in _connections.values.toList()) {
      connection._invalidate();
    }
    // Backend close owns physical cleanup and cancellation of scanner startup.
    try {
      await _backend.close();
    } finally {
      await _advertisements.cancel();
      await _adapter.cancel();
      if (_scanTransition != null) await _scanTransition;
      unawaited(_diagnostics.close());
    }
  }
}
