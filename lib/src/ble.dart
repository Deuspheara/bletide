import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'backend.dart';
import 'errors.dart';
import 'models.dart';
import 'backend_factory.dart';

/// Explicit cancellation of one or more asynchronous operations.
final class BleCancellation {
  bool _cancelled = false;
  final Set<void Function()> _listeners = {};
  bool get isCancelled => _cancelled;
  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    for (final listener in _listeners.toList()) {
      listener();
    }
    _listeners.clear();
  }

  void Function() _listen(void Function() callback) {
    if (_cancelled) {
      callback();
    } else {
      _listeners.add(callback);
    }
    return () => _listeners.remove(callback);
  }
}

BleException _exception(
  BleErrorCode code,
  String operation, {
  BleDeviceId? deviceId,
  int? generation,
}) => BleException(
  code,
  '${code.name} during $operation',
  context: BleErrorContext(
    operation: operation,
    deviceId: deviceId,
    connectionGeneration: generation,
  ),
);

/// A single terminal result, with timeout/cancellation propagated to the backend.
final class _Work<T> {
  _Work(
    this.operation,
    this.create,
    Duration timeout, {
    BleCancellation? cancellation,
    this.deviceId,
    this.generation,
    this.onLateResult,
  }) {
    result.future
        .ignore(); // Queued errors remain observable when callers await.
    if (timeout <= Duration.zero) {
      finishError(_exception(BleErrorCode.invalidState, operation));
    } else {
      _timer = Timer(timeout, () => abort(BleErrorCode.timeout));
      _removeCancellation = cancellation?._listen(
        () => abort(BleErrorCode.cancelled),
      );
    }
  }
  final String operation;
  final BackendRequest<T> Function() create;
  final BleDeviceId? deviceId;
  final int? generation;
  final void Function(T)? onLateResult;
  final Completer<T> result = Completer();
  BackendRequest<T>? _request;
  Timer? _timer;
  void Function()? _removeCancellation;
  bool get done => result.isCompleted;
  void _cleanup() {
    _timer?.cancel();
    _removeCancellation?.call();
  }

  void finishError(Object error, [StackTrace? stack]) {
    if (done) return;
    _cleanup();
    if (error is BleException) {
      final context = error.context;
      error = BleException(
        error.code,
        error.message,
        context: BleErrorContext(
          operation: context.operation ?? operation,
          deviceId: context.deviceId ?? deviceId,
          connectionGeneration: context.connectionGeneration ?? generation,
          platform: context.platform,
          nativeCode: context.nativeCode,
          nativeMessage: context.nativeMessage,
          serviceUuid: context.serviceUuid,
          characteristicUuid: context.characteristicUuid,
          descriptorUuid: context.descriptorUuid,
        ),
      );
    }
    result.completeError(error, stack);
  }

  void abort(BleErrorCode code) {
    if (done) return;
    finishError(
      _exception(code, operation, deviceId: deviceId, generation: generation),
    );
    _request?.cancel();
  }

  Future<void> run() async {
    if (done) return;
    try {
      final request = _request = create();
      final value = await request.result;
      if (done) {
        onLateResult?.call(value);
        return;
      }
      _cleanup();
      result.complete(value);
    } catch (error, stack) {
      finishError(error, stack);
    }
  }
}

/// Owns one notification listener. Values arriving during setup are buffered
/// until [values] is listened to; explicitly [cancel] to release ownership.
final class BleNotificationSubscription {
  BleNotificationSubscription._(this.values, this._cancel);
  final Stream<Uint8List> values;
  final Future<void> Function() _cancel;
  Future<void>? _cancelling;
  Future<void> cancel() => _cancelling ??= _cancel();
}

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

final class BleConnection {
  BleConnection._(this._ble, this._backend) {
    _remote = _backend.disconnected.listen((_) => _invalidate());
    _notifications = _backend.notifications.listen(
      (event) {
        if (_state != BleConnectionState.connected ||
            event.generation != generation) {
          return;
        }
        final entry = _subscriptions[event.characteristicKey];
        if (entry != null && entry.desired && !entry.controller.isClosed) {
          entry.controller.add(event.value);
        }
      },
      onError: (Object error) {
        _ble._diagnose(
          'notification.failed',
          deviceId: deviceId,
          generation: generation,
          errorCode: error is BleException ? error.code.name : null,
          nativeMessage: error is BleException
              ? error.context.nativeMessage
              : null,
        );
        for (final entry in _subscriptions.values) {
          if (!entry.controller.isClosed) entry.controller.addError(error);
        }
      },
    );
  }
  final Ble _ble;
  final BackendConnection _backend;
  late final StreamSubscription<void> _remote;
  late final StreamSubscription<BackendNotification> _notifications;
  final _states = StreamController<BleConnectionState>.broadcast();
  final Queue<_Work<dynamic>> _queue = Queue();
  final Map<String, _NotificationOwners> _subscriptions = {};
  _Work<dynamic>? _running;
  bool _draining = false;
  BleConnectionState _state = BleConnectionState.connected;
  Future<void>? _disconnecting;
  BleDeviceId get deviceId => _backend.deviceId;
  int get generation => _backend.generation;
  BleConnectionState get state => _state;
  Stream<BleConnectionState> get states => Stream.multi((controller) {
    final subscription = _states.stream.listen(
      controller.add,
      onDone: controller.close,
    );
    controller.add(_state);
    controller.onCancel = subscription.cancel;
  });
  Future<T> _enqueue<T>(
    String name,
    BackendRequest<T> Function() create,
    Duration timeout, {
    BleCancellation? cancellation,
    bool supported = true,
  }) {
    if (_state != BleConnectionState.connected) {
      return Future.error(
        _exception(
          BleErrorCode.disconnected,
          name,
          deviceId: deviceId,
          generation: generation,
        ),
      );
    }
    if (!supported) {
      return Future.error(
        _exception(
          BleErrorCode.notSupported,
          name,
          deviceId: deviceId,
          generation: generation,
        ),
      );
    }
    final work = _Work<T>(
      name,
      create,
      timeout,
      cancellation: cancellation,
      deviceId: deviceId,
      generation: generation,
    );
    _queue.add(work);
    _drain();
    return work.result.future;
  }

  void _drain() {
    if (_draining) return;
    _draining = true;
    unawaited(() async {
      try {
        while (_queue.isNotEmpty) {
          final work = _running = _queue.removeFirst();
          if (!work.done) {
            final elapsed = Stopwatch()..start();
            _ble._diagnose(
              'gatt.${work.operation}.started',
              deviceId: deviceId,
              generation: generation,
              operation: work.operation,
            );
            // run can wait for a late platform result; queue waits only for the
            // cancellable terminal future, then proceeds to the next request.
            unawaited(work.run());
            try {
              await work.result.future;
              elapsed.stop();
              _ble._diagnose(
                'gatt.${work.operation}.completed',
                deviceId: deviceId,
                generation: generation,
                operation: work.operation,
                duration: elapsed.elapsed,
              );
            } catch (error) {
              elapsed.stop();
              // The caller receives the terminal error; diagnostics must not
              // describe failed or cancelled operations as successful.
              _ble._diagnose(
                'gatt.${work.operation}.failed',
                deviceId: deviceId,
                generation: generation,
                operation: work.operation,
                duration: elapsed.elapsed,
                errorCode: error is BleException ? error.code.name : 'unknown',
                nativeMessage: error is BleException
                    ? error.context.nativeMessage
                    : null,
              );
            }
          }
          _running = null;
        }
      } finally {
        _draining = false;
      }
    }());
  }

  Future<List<BleService>> discoverServices({
    BleCancellation? cancellation,
  }) async => List.unmodifiable(
    await _enqueue(
      'discovery',
      _backend.discoverServices,
      _ble.timeouts.discovery,
      cancellation: cancellation,
    ),
  );
  Future<Uint8List> read(
    BleCharacteristic characteristic, {
    BleCancellation? cancellation,
  }) async => Uint8List.fromList(
    await _enqueue(
      'read',
      () => _backend.read(characteristic),
      _ble.timeouts.read,
      cancellation: cancellation,
    ),
  );
  Future<void> write(
    BleCharacteristic characteristic,
    Uint8List value, {
    bool withResponse = true,
    BleCancellation? cancellation,
  }) {
    final snapshot = Uint8List.fromList(value);
    return _enqueue(
      'write',
      () => _backend.write(characteristic, snapshot, withResponse),
      _ble.timeouts.write,
      cancellation: cancellation,
    );
  }

  Future<Uint8List> readDescriptor(
    BleDescriptor descriptor, {
    BleCancellation? cancellation,
  }) async => Uint8List.fromList(
    await _enqueue(
      'descriptor.read',
      () => _backend.readDescriptor(descriptor),
      _ble.timeouts.read,
      cancellation: cancellation,
      supported: _ble.capabilities.descriptorAccess,
    ),
  );
  Future<void> writeDescriptor(
    BleDescriptor descriptor,
    Uint8List value, {
    BleCancellation? cancellation,
  }) {
    final snapshot = Uint8List.fromList(value);
    return _enqueue(
      'descriptor.write',
      () => _backend.writeDescriptor(descriptor, snapshot),
      _ble.timeouts.write,
      cancellation: cancellation,
      supported: _ble.capabilities.descriptorAccess,
    );
  }

  Future<int> readRssi() => _enqueue(
    'rssi',
    _backend.readRssi,
    _ble.timeouts.read,
    supported: _ble.capabilities.readRssi,
  );
  Future<int> getMtu() => _enqueue(
    'mtu.get',
    _backend.getMtu,
    _ble.timeouts.read,
    supported: _ble.capabilities.getMtu,
  );
  Future<int> requestMtu(int mtu, {BleCancellation? cancellation}) {
    if (mtu < 23 || mtu > 517) {
      return Future.error(_exception(BleErrorCode.invalidState, 'mtu.request'));
    }
    return _enqueue(
      'mtu.request',
      () => _backend.requestMtu(mtu),
      _ble.timeouts.write,
      cancellation: cancellation,
      supported: _ble.capabilities.requestMtu,
    );
  }

  /// Conservative single ATT write payload, capped by the 512-byte attribute
  /// limit. Apple reports an inferred MTU; Linux may report the 23-byte fallback.
  /// Web Bluetooth cannot report this value and returns notSupported.
  Future<int> getWritePayloadLimit() async {
    final mtu = await getMtu();
    if (mtu < 23 || mtu > 517) {
      throw _exception(BleErrorCode.internal, 'write.payloadLimit');
    }
    return mtu - 3 > 512 ? 512 : mtu - 3;
  }

  Future<void> requestConnectionPriority(
    BleConnectionPriority priority, {
    BleCancellation? cancellation,
  }) => _enqueue(
    'connection.priority',
    () => _backend.requestConnectionPriority(priority),
    _ble.timeouts.write,
    cancellation: cancellation,
    supported: _ble.capabilities.requestConnectionPriority,
  );

  /// Acquires notification ownership and waits for the backend enable ACK.
  /// Success confirms setup for this generation, not delivery of a first value.
  /// [setupMode] is opt-in for this characteristic; simultaneous owners must
  /// agree. Unsupported platforms and non-notify attributes return notSupported.
  Future<BleNotificationSubscription> enableNotifications(
    BleCharacteristic characteristic, {
    BleCancellation? cancellation,
    BleNotificationSetupMode setupMode = BleNotificationSetupMode.standard,
  }) async {
    if (_state != BleConnectionState.connected) {
      throw _exception(
        BleErrorCode.disconnected,
        'subscribe',
        deviceId: deviceId,
        generation: generation,
      );
    }
    if (cancellation?.isCancelled ?? false) {
      throw _exception(BleErrorCode.cancelled, 'subscribe');
    }
    final entry = _notificationOwners(characteristic, setupMode);
    final values = StreamController<Uint8List>();
    var pendingEvents = 0;
    var overflowed = false;
    final cancelled = Completer<void>();
    cancelled.future.ignore();
    late final BleNotificationSubscription handle;
    bool reserveEvent() {
      if (overflowed || values.isClosed) return false;
      if (pendingEvents == 256) {
        overflowed = true;
        final error = BleException(
          BleErrorCode.gattFailure,
          'Notification consumer buffer exceeded 256 events',
          context: BleErrorContext(
            operation: 'notification.buffer',
            deviceId: deviceId,
            connectionGeneration: generation,
          ),
        );
        pendingEvents++; // The final overflow error is queued once as well.
        values.addError(error);
        if (!cancelled.isCompleted) cancelled.completeError(error);
        unawaited(
          handle.cancel().catchError((Object error) {
            _ble._diagnose(
              'gatt.unsubscribe.failed',
              deviceId: deviceId,
              generation: generation,
              errorCode: error is BleException ? error.code.name : 'unknown',
            );
          }),
        );
        return false;
      }
      pendingEvents++;
      return true;
    }

    final listener = entry.controller.stream.listen(
      (value) {
        if (reserveEvent()) values.add(value);
      },
      onError: (Object error, StackTrace stack) {
        if (reserveEvent()) values.addError(error, stack);
      },
      onDone: values.close,
    );
    Future<void> release() async {
      final cancelling = listener.cancel();
      final released = entry.controller.hasListener
          ? null
          : entry._release?.future;
      await cancelling;
      if (released != null) await released;
      // Closing a buffered single-subscription controller waits for a listener.
      unawaited(values.close());
    }

    handle = BleNotificationSubscription._(
      values.stream
          .transform(
            StreamTransformer<Uint8List, Uint8List>.fromHandlers(
              handleData: (value, sink) {
                pendingEvents--;
                sink.add(value);
              },
              handleError: (Object error, StackTrace stack, sink) {
                pendingEvents--;
                sink.addError(error, stack);
              },
            ),
          )
          .where(
            (_) =>
                handle._cancelling == null &&
                _state == BleConnectionState.connected,
          ),
      release,
    );
    values.onCancel = handle.cancel;
    final remove = cancellation?._listen(() {
      if (!cancelled.isCompleted) {
        cancelled.completeError(
          _exception(BleErrorCode.cancelled, 'subscribe'),
        );
      }
    });
    try {
      await Future.any([entry.ready.future, cancelled.future]);
      if (_state != BleConnectionState.connected) {
        throw _exception(BleErrorCode.disconnected, 'subscribe');
      }
      return handle;
    } catch (_) {
      // Release the acquired owner even when setup/cancellation fails.
      // Cleanup may need the enable ACK before issuing disable.
      unawaited(handle.cancel().catchError((Object _) {}));
      rethrow;
    } finally {
      remove?.call();
    }
  }

  Stream<Uint8List> subscribe(
    BleCharacteristic characteristic, {
    BleNotificationSetupMode setupMode = BleNotificationSetupMode.standard,
  }) => Stream<Uint8List>.multi((controller) {
    if (_state != BleConnectionState.connected) {
      controller.addError(
        _exception(
          BleErrorCode.disconnected,
          'subscribe',
          deviceId: deviceId,
          generation: generation,
        ),
      );
      controller.close();
      return;
    }
    late final _NotificationOwners entry;
    try {
      entry = _notificationOwners(characteristic, setupMode);
    } catch (error, stack) {
      controller.addError(error, stack);
      controller.close();
      return;
    }
    final subscription = entry.controller.stream.listen(
      controller.add,
      onError: controller.addError,
      onDone: controller.close,
    );
    controller.onCancel = () {
      final cancellation = subscription.cancel();
      // Capture this owner's release before another listener can acquire it.
      final release = entry.controller.hasListener
          ? null
          : entry._release?.future;
      return () async {
        await cancellation;
        if (release != null) await release;
      }();
    };
  }).where((_) => _state == BleConnectionState.connected);

  _NotificationOwners _notificationOwners(
    BleCharacteristic characteristic,
    BleNotificationSetupMode setupMode,
  ) {
    if (setupMode != BleNotificationSetupMode.standard) {
      if (_backend is! BackendNotificationCompatibility ||
          !(_backend as BackendNotificationCompatibility)
              .supportsNotificationCompatibility) {
        throw _exception(BleErrorCode.notSupported, 'subscribe.compatibility');
      }
      if (!characteristic.properties.notify) {
        throw _exception(BleErrorCode.notSupported, 'subscribe.compatibility');
      }
    }
    final existing = _subscriptions[characteristic.key];
    if (existing != null && existing.setupMode != setupMode) {
      throw _exception(BleErrorCode.invalidState, 'subscribe.compatibility');
    }
    return _subscriptions.putIfAbsent(
      characteristic.key,
      () => _NotificationOwners(this, characteristic, setupMode),
    );
  }

  void _invalidate() {
    if (_state == BleConnectionState.disconnected) return;
    _state = BleConnectionState.disconnected;
    _states.add(_state);
    _running?.abort(BleErrorCode.disconnected);
    for (final work in _queue) {
      work.abort(BleErrorCode.disconnected);
    }
    _queue.clear();
    for (final entry in _subscriptions.values) {
      entry.dispose();
    }
    _subscriptions.clear();
    unawaited(_remote.cancel());
    unawaited(_notifications.cancel());
    unawaited(_states.close());
    if (identical(_ble._connections[deviceId], this)) {
      _ble._connections.remove(deviceId);
    }
    _ble._diagnose(
      'connect.disconnected',
      deviceId: deviceId,
      generation: generation,
    );
  }

  Future<void> disconnect() => _disconnecting ??= _disconnect();
  Future<void> _disconnect() async {
    if (_state == BleConnectionState.disconnected) return;
    _state = BleConnectionState.disconnecting;
    _states.add(_state);
    _invalidate();
    final work = _Work<void>(
      'disconnect',
      _backend.disconnect,
      _ble.timeouts.disconnect,
      deviceId: deviceId,
      generation: generation,
    );
    unawaited(work.run());
    await work.result.future;
  }
}

final class _NotificationOwners {
  _NotificationOwners(this.connection, this.characteristic, this.setupMode) {
    ready.future.ignore();
    controller = StreamController.broadcast(
      onListen: () {
        if (ready.isCompleted) {
          ready = Completer<void>();
          ready.future.ignore();
          if (physical && !_updating) ready.complete();
        }
        desired = true;
        _reconcile();
      },
      onCancel: () {
        desired = false;
        if (_disposed) return;
        _release ??= Completer<void>();
        _release!.future.ignore();
        _reconcile();
      },
    );
  }
  final BleConnection connection;
  final BleCharacteristic characteristic;
  final BleNotificationSetupMode setupMode;
  late final StreamController<Uint8List> controller;
  bool desired = false;
  bool physical = false;
  bool _updating = false;
  bool _disposed = false;
  Completer<void>? _release;
  Completer<void> ready = Completer<void>();

  void _finishRelease([Object? error, StackTrace? stack]) {
    final release = _release;
    _release = null;
    if (release == null) return;
    if (error == null) {
      release.complete();
    } else {
      release.completeError(error, stack);
    }
  }

  void _reconcile() {
    if (_updating || _disposed) return;
    _updating = true;
    final transition = () async {
      try {
        while (!_disposed && desired != physical) {
          final subscribing = desired;
          // Even failed startup may have changed platform subscription state.
          physical = subscribing;
          try {
            await connection._enqueue<void>(
              subscribing ? 'subscribe' : 'unsubscribe',
              () => setupMode != BleNotificationSetupMode.standard
                  ? (connection._backend as BackendNotificationCompatibility)
                        .setNotificationsWithCompatibility(
                          characteristic,
                          subscribing,
                          setupMode,
                        )
                  : subscribing
                  ? connection._backend.subscribe(characteristic)
                  : connection._backend.unsubscribe(characteristic),
              connection._ble.timeouts.subscribe,
            );
            // Stop acknowledgement releases departed owners before replacement
            // setup begins. Reacquisition during setup can retain that setup.
            if (subscribing && !ready.isCompleted) ready.complete();
            if (!subscribing || desired) _finishRelease();
          } catch (error, stack) {
            // Replacement readiness also depends on teardown of the old owner.
            // Preserve that failure before disconnect invalidates the generation.
            if (!ready.isCompleted) ready.completeError(error, stack);
            connection._ble._diagnose(
              subscribing ? 'gatt.subscribe.failed' : 'gatt.unsubscribe.failed',
              deviceId: connection.deviceId,
              generation: connection.generation,
              errorCode: error is BleException ? error.code.name : 'unknown',
            );
            if (!_disposed && !controller.isClosed) {
              controller.addError(error, stack);
            }
            if (subscribing) {
              desired = false;
            } else if (!_disposed) {
              // Failed teardown leaves physical state uncertain. Retain ownership
              // until disconnect invalidates this generation, then report stop failure.
              physical = true;
              try {
                await connection.disconnect();
              } catch (cleanup) {
                connection._ble._diagnose(
                  'connection.disconnect.failed',
                  deviceId: connection.deviceId,
                  generation: connection.generation,
                  errorCode: cleanup is BleException
                      ? cleanup.code.name
                      : 'unknown',
                );
              }
              _finishRelease(error, stack);
              return;
            }
          }
        }
      } finally {
        _updating = false;
        if (_disposed) _finishRelease();
        if (!_disposed &&
            !desired &&
            !physical &&
            identical(connection._subscriptions[characteristic.key], this)) {
          connection._subscriptions.remove(characteristic.key);
          dispose();
        }
      }
    }();
    transition
        .ignore(); // Setup errors use the stream; release owns stop errors.
  }

  void dispose() {
    _disposed = true;
    if (!ready.isCompleted) {
      ready.completeError(_exception(BleErrorCode.disconnected, 'subscribe'));
    }
    desired = false;
    if (!_updating) _finishRelease();
    unawaited(controller.close());
  }
}
