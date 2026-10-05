import 'dart:async';
import 'dart:js_interop';
import 'dart:typed_data';

import '../backend.dart';
import '../errors.dart';
import '../models.dart';
import 'bluetooth.dart';

BleException browserFailure(
  Object error,
  BleErrorContext context, {
  BleErrorCode missing = BleErrorCode.gattFailure,
}) {
  if (error is BleException) {
    final previous = error.context;
    return BleException(
      error.code,
      error.message,
      context: BleErrorContext(
        platform: previous.platform ?? 'web',
        operation: context.operation,
        deviceId: context.deviceId,
        connectionGeneration: context.connectionGeneration,
        serviceUuid: context.serviceUuid ?? previous.serviceUuid,
        characteristicUuid:
            context.characteristicUuid ?? previous.characteristicUuid,
        descriptorUuid: context.descriptorUuid ?? previous.descriptorUuid,
        nativeCode: previous.nativeCode,
        nativeMessage: previous.nativeMessage,
      ),
    );
  }
  String? name, message;
  try {
    final exception = BrowserError(error as JSObject);
    name = exception.name;
    message = exception.message;
  } catch (_) {
    message = error.toString();
  }
  return BleException(
    switch (name) {
      'NotFoundError' => missing,
      'SecurityError' || 'NotAllowedError' => BleErrorCode.permissionDenied,
      'NotSupportedError' => BleErrorCode.notSupported,
      'TypeError' || 'InvalidStateError' => BleErrorCode.invalidState,
      'NetworkError' =>
        context.operation == 'connect'
            ? BleErrorCode.connectFailed
            : BleErrorCode.disconnected,
      _ => BleErrorCode.gattFailure,
    },
    message ?? 'Browser Bluetooth operation failed',
    context: BleErrorContext(
      platform: 'web',
      operation: context.operation,
      deviceId: context.deviceId,
      connectionGeneration: context.connectionGeneration,
      serviceUuid: context.serviceUuid,
      characteristicUuid: context.characteristicUuid,
      descriptorUuid: context.descriptorUuid,
      nativeCode: name,
      nativeMessage: message,
    ),
  );
}

/// Owns one physical generation, FIFO, event listeners and discovery cache.
final class WebBleConnection implements BackendConnection {
  WebBleConnection(
    this.deviceId,
    this.generation,
    this.device,
    this.server,
    this.timeouts,
    this.onReleased,
  ) {
    _remote = ((JSAny? _) {
      closeGeneration(BleErrorCode.disconnected);
    }).toJS;
    device.addEventListener('gattserverdisconnected', _remote);
  }
  @override
  final BleDeviceId deviceId;
  @override
  final int generation;
  final BrowserDevice device;
  final BrowserGatt server;
  final BleTimeouts timeouts;
  final void Function(Object?) onReleased;
  late final JSFunction _remote;
  final _disconnected = StreamController<void>.broadcast(sync: true);
  final _notifications = StreamController<BackendNotification>.broadcast(
    sync: true,
  );
  final _characters = <String, BrowserCharacteristic>{};
  final _descriptors = <String, BrowserDescriptor>{};
  final _listeners = <String, JSFunction>{};
  final _pending = <int, void Function(BleErrorCode)>{};
  final _queued = <int, Future<void> Function()>{};
  bool _busy = false;
  Future<void>? _activeJob;
  Future<void> get debugActiveCompletion => _activeJob ?? Future.value();

  // Internal inspection only; the public Ble API does not expose diagnostics.
  ({
    int pending,
    int queued,
    int active,
    int listeners,
    int characters,
    int descriptors,
  })
  get debugResources => (
    pending: _pending.length,
    queued: _queued.length,
    active: _busy ? 1 : 0,
    listeners: _listeners.length + (closed ? 0 : 1),
    characters: _characters.length,
    descriptors: _descriptors.length,
  );

  void _pump() {
    if (_busy || closed || _queued.isEmpty) return;
    final run = _queued.remove(_queued.keys.first)!;
    _busy = true;
    final job = run().whenComplete(() {
      _busy = false;
      _activeJob = null;
      _pump();
    });
    _activeJob = job;
    unawaited(job);
  }

  int _nextRequest = 0;
  bool closed = false;
  Object? _cleanupFailure;
  @override
  Stream<void> get disconnected => _disconnected.stream;
  @override
  Stream<BackendNotification> get notifications => _notifications.stream;

  BleErrorContext _context(
    String operation, {
    BleCharacteristic? characteristic,
    BleDescriptor? descriptor,
  }) => BleErrorContext(
    platform: 'web',
    operation: operation,
    deviceId: deviceId,
    connectionGeneration: generation,
    serviceUuid: characteristic?.serviceUuid ?? descriptor?.serviceUuid,
    characteristicUuid: characteristic?.uuid ?? descriptor?.characteristicUuid,
    descriptorUuid: descriptor?.uuid,
  );
  BleException _error(BleErrorCode code, String operation) => BleException(
    code,
    '${code.name} during $operation',
    context: _context(operation),
  );

  BackendRequest<T> _request<T>(
    String operation,
    Future<T> Function() call,
    Duration timeout, {
    BleCharacteristic? characteristic,
    BleDescriptor? descriptor,
  }) {
    if (closed) {
      return BackendRequest(
        Future.error(_error(BleErrorCode.disconnected, operation)),
        () {},
      );
    }
    if (_pending.length >= 1024) {
      return BackendRequest(
        Future.error(_error(BleErrorCode.invalidState, operation)),
        () {},
      );
    }
    final id = ++_nextRequest;
    final result = Completer<T>();
    Timer? timer;
    bool running = false;
    Future<T> Function()? action = call;
    final context = _context(
      operation,
      characteristic: characteristic,
      descriptor: descriptor,
    );
    void finishError(Object error) {
      if (result.isCompleted) return;
      timer?.cancel();
      _pending.remove(id);
      _queued.remove(id);
      action = null;
      result.completeError(
        browserFailure(
          error,
          context,
          missing: descriptor != null
              ? BleErrorCode.descriptorNotFound
              : characteristic != null
              ? BleErrorCode.characteristicNotFound
              : BleErrorCode.serviceNotFound,
        ),
      );
    }

    void abort(BleErrorCode code) {
      finishError(
        BleException(code, '${code.name} during $operation', context: context),
      );
      if (running && !closed) closeGeneration(BleErrorCode.disconnected);
    }

    _pending[id] = abort;
    timer = Timer(timeout, () => abort(BleErrorCode.timeout));
    _queued[id] = () async {
      if (result.isCompleted) return;
      if (closed) {
        abort(BleErrorCode.disconnected);
        return;
      }
      final invoke = action;
      action = null;
      if (invoke == null) return;
      running = true;
      try {
        final value = await invoke();
        if (result.isCompleted) return;
        if (closed || !server.connected) {
          abort(BleErrorCode.disconnected);
          return;
        }
        timer?.cancel();
        _pending.remove(id);
        result.complete(value);
      } catch (error) {
        finishError(error);
      } finally {
        running = false;
      }
    };
    scheduleMicrotask(_pump);
    return BackendRequest(result.future, () => abort(BleErrorCode.cancelled));
  }

  void closeGeneration(BleErrorCode code) {
    if (closed) return;
    closed = true;
    void cleanup(void Function() action) {
      try {
        action();
      } catch (error) {
        _cleanupFailure ??= error;
      }
    }

    cleanup(
      () => device.removeEventListener('gattserverdisconnected', _remote),
    );
    for (final entry in _listeners.entries) {
      cleanup(
        () => _characters[entry.key]?.removeEventListener(
          'characteristicvaluechanged',
          entry.value,
        ),
      );
    }
    _listeners.clear();
    _characters.clear();
    _descriptors.clear();
    for (final abort in _pending.values.toList()) {
      abort(code);
    }
    _queued.clear();
    cleanup(() => server.disconnect());
    _disconnected.add(null);
    unawaited(_disconnected.close());
    unawaited(_notifications.close());
    onReleased(_cleanupFailure);
  }

  Uint8List _copy(JSDataView value) {
    final view = value.toDart;
    return Uint8List.fromList(
      view.buffer.asUint8List(view.offsetInBytes, view.lengthInBytes),
    );
  }

  BrowserCharacteristic _character(BleCharacteristic value, String operation) {
    final found = _characters[value.key];
    if (found == null) {
      throw _error(BleErrorCode.characteristicNotFound, operation);
    }
    return found;
  }

  String _descriptorKey(BleDescriptor value) =>
      '${value.serviceUuid}/${value.characteristicUuid}/${value.uuid}';
  BrowserDescriptor _descriptor(BleDescriptor value, String operation) {
    final found = _descriptors[_descriptorKey(value)];
    if (found == null) throw _error(BleErrorCode.descriptorNotFound, operation);
    return found;
  }

  @override
  BackendRequest<List<BleService>> discoverServices() => _request(
    'discoverServices',
    () async {
      // Rediscovery would replace characteristic objects with active callbacks.
      if (_listeners.isNotEmpty) {
        throw _error(BleErrorCode.invalidState, 'discoverServices');
      }
      final services = await server.getPrimaryServices().toDart;
      if (closed) throw _error(BleErrorCode.disconnected, 'discoverServices');
      final characters = <String, BrowserCharacteristic>{};
      final descriptors = <String, BrowserDescriptor>{};
      final result = <BleService>[];
      final seen = <BleUuid>{};
      for (final service in services.toDart) {
        final serviceUuid = BleUuid(service.uuid);
        if (!seen.add(serviceUuid)) {
          throw _error(BleErrorCode.notSupported, 'ambiguous service UUID');
        }
        final discovered = await service.getCharacteristics().toDart;
        if (closed) throw _error(BleErrorCode.disconnected, 'discoverServices');
        final models = <BleCharacteristic>[];
        for (final characteristic in discovered.toDart) {
          final uuid = BleUuid(characteristic.uuid);
          final key = '$serviceUuid/$uuid';
          if (characters.containsKey(key)) {
            throw _error(
              BleErrorCode.notSupported,
              'ambiguous characteristic UUID',
            );
          }
          final p = characteristic.properties;
          final properties = BleCharacteristicProperties(
            (p.read ? 2 : 0) |
                (p.writeWithoutResponse ? 4 : 0) |
                (p.write ? 8 : 0) |
                (p.notify ? 16 : 0) |
                (p.indicate ? 32 : 0),
          );
          final descriptorModels = <BleDescriptor>[];
          final discoveredDescriptors = await characteristic
              .getDescriptors()
              .toDart;
          if (closed) {
            throw _error(BleErrorCode.disconnected, 'discoverServices');
          }
          for (final descriptor in discoveredDescriptors.toDart) {
            final model = BleDescriptor(
              serviceUuid: serviceUuid,
              characteristicUuid: uuid,
              uuid: BleUuid(descriptor.uuid),
            );
            if (descriptors.containsKey(_descriptorKey(model))) {
              throw _error(
                BleErrorCode.notSupported,
                'ambiguous descriptor UUID',
              );
            }
            descriptors[_descriptorKey(model)] = descriptor;
            descriptorModels.add(model);
          }
          characters[key] = characteristic;
          models.add(
            BleCharacteristic(
              serviceUuid: serviceUuid,
              uuid: uuid,
              properties: properties,
              descriptors: descriptorModels,
            ),
          );
        }
        result.add(
          BleService(
            uuid: serviceUuid,
            primary: service.isPrimary,
            characteristics: models,
          ),
        );
      }
      if (closed) throw _error(BleErrorCode.disconnected, 'discoverServices');
      _characters
        ..clear()
        ..addAll(characters);
      _descriptors
        ..clear()
        ..addAll(descriptors);
      return List.unmodifiable(result);
    },
    timeouts.discovery,
  );
  @override
  BackendRequest<Uint8List> read(BleCharacteristic characteristic) => _request(
    'read',
    () async {
      final target = _character(characteristic, 'read');
      if (!target.properties.read) {
        throw _error(BleErrorCode.notSupported, 'read');
      }
      return _copy(await target.readValue().toDart);
    },
    timeouts.read,
    characteristic: characteristic,
  );
  @override
  BackendRequest<void> write(
    BleCharacteristic characteristic,
    Uint8List value,
    bool withResponse,
  ) {
    final bytes = Uint8List.fromList(value);
    return _request(
      'write',
      () async {
        final target = _character(characteristic, 'write');
        if (withResponse
            ? !target.properties.write
            : !target.properties.writeWithoutResponse) {
          throw _error(BleErrorCode.notSupported, 'write');
        }
        if (withResponse) {
          await target.writeValueWithResponse(bytes.toJS).toDart;
        } else {
          await target.writeValueWithoutResponse(bytes.toJS).toDart;
        }
      },
      timeouts.write,
      characteristic: characteristic,
    );
  }

  @override
  BackendRequest<Uint8List> readDescriptor(BleDescriptor descriptor) =>
      _request(
        'readDescriptor',
        () async => _copy(
          await _descriptor(descriptor, 'readDescriptor').readValue().toDart,
        ),
        timeouts.read,
        descriptor: descriptor,
      );
  @override
  BackendRequest<void> writeDescriptor(
    BleDescriptor descriptor,
    Uint8List value,
  ) {
    final bytes = Uint8List.fromList(value);
    return _request(
      'writeDescriptor',
      () async {
        await _descriptor(
          descriptor,
          'writeDescriptor',
        ).writeValue(bytes.toJS).toDart;
      },
      timeouts.write,
      descriptor: descriptor,
    );
  }

  @override
  BackendRequest<void> subscribe(BleCharacteristic characteristic) => _request(
    'subscribe',
    () async {
      if (_listeners.containsKey(characteristic.key)) return;
      final target = _character(characteristic, 'subscribe');
      if (!target.properties.notify && !target.properties.indicate) {
        throw _error(BleErrorCode.notSupported, 'subscribe');
      }
      late final JSFunction listener;
      listener = ((JSAny? _) {
        final value = target.value;
        if (!closed &&
            identical(_listeners[characteristic.key], listener) &&
            value != null) {
          _notifications.add(
            BackendNotification(generation, characteristic.key, _copy(value)),
          );
        }
      }).toJS;
      _listeners[characteristic.key] = listener;
      target.addEventListener('characteristicvaluechanged', listener);
      try {
        await target.startNotifications().toDart;
      } catch (_) {
        target.removeEventListener('characteristicvaluechanged', listener);
        _listeners.remove(characteristic.key);
        rethrow;
      }
    },
    timeouts.subscribe,
    characteristic: characteristic,
  );
  @override
  BackendRequest<void> unsubscribe(
    BleCharacteristic characteristic,
  ) => _request(
    'unsubscribe',
    () async {
      final listener = _listeners[characteristic.key];
      if (listener == null) return;
      final target = _character(characteristic, 'unsubscribe');
      // Keep ownership until the browser acknowledges stop. A failure can
      // be retried and generation cleanup still knows which listener to remove.
      await target.stopNotifications().toDart;
      target.removeEventListener('characteristicvaluechanged', listener);
      _listeners.remove(characteristic.key);
    },
    timeouts.subscribe,
    characteristic: characteristic,
  );
  BackendRequest<int> _unsupported(String operation) => BackendRequest(
    Future.error(_error(BleErrorCode.notSupported, operation)),
    () {},
  );
  @override
  BackendRequest<int> readRssi() => _unsupported('readRssi');
  @override
  BackendRequest<int> getMtu() => _unsupported('getMtu');
  @override
  BackendRequest<int> requestMtu(int mtu) => _unsupported('requestMtu');
  @override
  BackendRequest<void> requestConnectionPriority(
    BleConnectionPriority priority,
  ) => _unsupported('connection.priority');
  @override
  BackendRequest<void> disconnect() {
    closeGeneration(BleErrorCode.disconnected);
    final failure = _cleanupFailure;
    return BackendRequest(
      failure == null
          ? Future.value()
          : Future.error(browserFailure(failure, _context('disconnect'))),
      () {},
    );
  }
}
