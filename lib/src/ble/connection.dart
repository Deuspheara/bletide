part of '../ble.dart';

/// Owns one notification listener. Values arriving during setup are buffered
/// until [values] is listened to; explicitly [cancel] to release ownership.
final class BleNotificationSubscription {
  BleNotificationSubscription._(this.values, this._cancel);
  final Stream<Uint8List> values;
  final Future<void> Function() _cancel;
  Future<void>? _cancelling;
  Future<void> cancel() => _cancelling ??= _cancel();
}

/// One connection generation. Operations run FIFO; deadlines include queue time.
/// Cancellation of running native/browser work may retire this generation.
/// Listen to [states] and explicitly [disconnect] when finished.
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

  /// Discovers attributes for this generation. Release notification owners
  /// before rediscovery so live callbacks keep their original attribute objects.
  Future<List<BleService>> discoverServices({
    BleCancellation? cancellation,
  }) async => List.unmodifiable(
    await _enqueue(
      'discovery',
      () {
        if (_subscriptions.isNotEmpty) {
          throw _exception(BleErrorCode.invalidState, 'discovery');
        }
        return _backend.discoverServices();
      },
      _ble.timeouts.discovery,
      cancellation: cancellation,
      supported: _ble.capabilities.gatt,
    ),
  );

  /// Reads a discovered characteristic and returns an independent byte copy.
  Future<Uint8List> read(
    BleCharacteristic characteristic, {
    BleCancellation? cancellation,
  }) async => Uint8List.fromList(
    await _enqueue(
      'read',
      () => _backend.read(characteristic),
      _ble.timeouts.read,
      cancellation: cancellation,
      supported: _ble.capabilities.gatt,
    ),
  );

  /// Writes a snapshot of [value]. No automatic chunking or protocol framing.
  /// A response acknowledges ATT acceptance, not application processing.
  /// Check properties and the platform write budget before sending.
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
      supported: _ble.capabilities.gatt,
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
      supported: _ble.capabilities.gatt && _ble.capabilities.descriptorAccess,
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
      supported: _ble.capabilities.gatt && _ble.capabilities.descriptorAccess,
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

  /// Lazy notification ownership for each listener. Setup failures arrive as
  /// stream errors; use [enableNotifications] when setup must be awaited.
  /// Cancel the listener to await final-owner teardown.
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
    if (!_ble.capabilities.gatt) {
      throw _exception(
        BleErrorCode.notSupported,
        'subscribe',
        deviceId: deviceId,
        generation: generation,
      );
    }
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

  /// Ends logical delivery immediately and awaits backend cleanup. Repeated
  /// calls share the terminal result. This object cannot be connected again.
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
