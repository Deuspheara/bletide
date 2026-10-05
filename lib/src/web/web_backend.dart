import 'dart:async';
import 'dart:js_interop';

import 'package:web/web.dart' as web;

import '../backend.dart';
import '../errors.dart';
import '../models.dart';
import 'bluetooth.dart';
import 'web_connection.dart';

BleBackend createBackend({BleTimeouts timeouts = const BleTimeouts()}) =>
    WebBleBackend(timeouts: timeouts);

final class WebBleBackend implements BleBackend {
  WebBleBackend({
    BrowserBluetooth? bluetooth,
    bool? secureContext,
    this.timeouts = const BleTimeouts(),
  }) : _bluetooth = (secureContext ?? web.window.isSecureContext)
           ? bluetooth ?? BluetoothNavigator(web.window.navigator).bluetooth
           : null {
    _ready.future.ignore();
    _observeAvailability();
  }
  final BleTimeouts timeouts;
  final BrowserBluetooth? _bluetooth;
  // Process-wide leases prevent two backends from driving one browser server.
  static final _leases = <BleDeviceId, Object>{};
  static final _retiring = <BleDeviceId, StreamController<void>>{};
  final _retrying = <BleDeviceId, void Function(BleErrorCode)>{};
  static int _nextGeneration = 0;
  final _connections = <BleDeviceId, WebBleConnection>{};
  final _connecting = <BleDeviceId, void Function(BleErrorCode)>{};
  final Map<BleDeviceId, BrowserDevice> _devices = {};
  Completer<BleAdvertisement>? _chooser;
  bool _physicalChooser = false;
  bool _closed = false;
  Object? _cleanupFailure;
  final _ready = Completer<void>();
  final _states = StreamController<BleAdapterState>.broadcast();
  BleAdapterState _state = BleAdapterState.unsupported;
  JSFunction? _availabilityListener;
  Timer? _availabilityTimer;
  int _availabilityRevision = 0;

  // Derived internal counts distinguish logical ownership from browser calls
  // that cannot be aborted and retain their reservation until settlement.
  ({
    int devices,
    int connections,
    int browserConnects,
    int chooser,
    int availabilityListeners,
  })
  get debugResources => (
    devices: _devices.length,
    connections: _connections.length,
    browserConnects: _connecting.length,
    chooser: _physicalChooser ? 1 : 0,
    availabilityListeners: _availabilityListener == null ? 0 : 1,
  );

  void _observeAvailability() {
    final bluetooth = _bluetooth;
    if (bluetooth == null || bluetooth.availabilityQuery == null) {
      _ready.complete();
      return;
    }
    void changed(JSObject event) {
      if (_closed) return;
      _availabilityRevision++;
      _setAvailability(BrowserAvailabilityEvent(event).available);
    }

    void failed(Object error) {
      if (_closed || _ready.isCompleted) return;
      _availabilityTimer?.cancel();
      _ready.completeError(
        browserFailure(
          error,
          const BleErrorContext(platform: 'web', operation: 'getAvailability'),
        ),
      );
    }

    try {
      final listener = changed.toJS;
      bluetooth.addEventListener('availabilitychanged', listener);
      _availabilityListener = listener;
      final revision = _availabilityRevision;
      _availabilityTimer = Timer(const Duration(seconds: 10), () {
        if (!_ready.isCompleted) {
          _ready.completeError(
            _error(
              BleErrorCode.timeout,
              'getAvailability',
              'Availability query timed out',
            ),
          );
        }
      });
      bluetooth.getAvailability().toDart.then((available) {
        if (_closed || _ready.isCompleted) return;
        _availabilityTimer?.cancel();
        // A newer event is authoritative over an older query response.
        if (revision == _availabilityRevision) {
          _setAvailability(available.toDart);
        }
        _ready.complete();
      }, onError: failed);
    } catch (error) {
      failed(error);
    }
  }

  void _setAvailability(bool available) {
    final next = available
        ? BleAdapterState.ready
        : BleAdapterState.unavailable;
    if (_state == next) return;
    _state = next;
    if (!available) {
      for (final abort in _connecting.values.toList()) {
        abort(BleErrorCode.adapterUnavailable);
      }
      for (final connection in _connections.values.toList()) {
        connection.closeGeneration(BleErrorCode.adapterUnavailable);
      }
    }
    _states.add(next);
  }

  @override
  Future<void> get ready => _ready.future;
  @override
  BleCapabilities get capabilities => BleCapabilities(
    requestDevice: _bluetooth != null,
    connect: _bluetooth != null,
    gatt: _bluetooth != null,
    descriptorAccess: _bluetooth != null,
    adapterState: _availabilityListener != null,
  );
  @override
  BleAdapterState get currentAdapterState => _state;
  @override
  Stream<BleAdapterState> get adapterState => _states.stream;
  @override
  Stream<BleAdvertisement> get advertisements => const Stream.empty();
  BleException _error(BleErrorCode code, String operation, String message) =>
      BleException(
        code,
        message,
        context: BleErrorContext(platform: 'web', operation: operation),
      );
  BackendRequest<T> _rejected<T>(
    BleErrorCode code,
    String operation,
    String message,
  ) => BackendRequest(Future.error(_error(code, operation, message)), () {});
  @override
  BackendRequest<BleAdvertisement> requestDevice(BleDeviceRequest options) {
    const operation = 'requestDevice';
    if (_closed) {
      return _rejected(BleErrorCode.disposed, operation, 'Web backend closed');
    }
    final bluetooth = _bluetooth;
    if (bluetooth == null) {
      return _rejected(
        BleErrorCode.notSupported,
        operation,
        'Web Bluetooth requires a supporting browser and secure context',
      );
    }
    if (_physicalChooser) {
      return _rejected(
        BleErrorCode.invalidState,
        operation,
        'A browser chooser is still open',
      );
    }
    if (_devices.length >= 128) {
      return _rejected(
        BleErrorCode.invalidState,
        operation,
        'Selected device capacity exceeded',
      );
    }
    final filters = <BrowserDeviceFilter>[
      if (options.serviceUuids.isEmpty && options.namePrefix != null)
        BrowserDeviceFilter(namePrefix: options.namePrefix),
      for (final uuid in options.serviceUuids)
        if (options.namePrefix == null)
          BrowserDeviceFilter(services: [uuid.value.toJS].toJS)
        else
          BrowserDeviceFilter(
            namePrefix: options.namePrefix,
            services: [uuid.value.toJS].toJS,
          ),
    ];
    final chooser = _chooser = Completer<BleAdvertisement>();
    _physicalChooser = true;
    final permissions = options.optionalServices
        .map((uuid) => uuid.value.toJS)
        .toList()
        .toJS;
    final requestOptions = filters.isEmpty
        ? BrowserDeviceOptions(
            acceptAllDevices: true,
            optionalServices: permissions,
          )
        : BrowserDeviceOptions(
            filters: filters.toJS,
            optionalServices: permissions,
          );
    void settled() {
      if (identical(_chooser, chooser)) {
        _physicalChooser = false;
        _chooser = null;
      }
    }

    void failed(Object error) {
      settled();
      if (!chooser.isCompleted) chooser.completeError(_browserError(error));
    }

    try {
      // Invoke before any await, preserving the browser's user activation.
      final promise = bluetooth.requestDevice(requestOptions);
      unawaited(
        promise.toDart
            .then<void>((device) {
              settled();
              if (_closed || chooser.isCompleted) return;
              if (device.id.isEmpty) {
                throw _error(
                  BleErrorCode.internal,
                  operation,
                  'Browser returned an empty device identifier',
                );
              }
              final id = BleDeviceId(device.id);
              _devices[id] = device;
              chooser.complete(
                BleAdvertisement(deviceId: id, name: device.name),
              );
            })
            .catchError((Object error) {
              failed(error);
            })
            .whenComplete(settled),
      );
    } catch (error) {
      failed(error);
      settled();
    }
    return BackendRequest(chooser.future, () {
      if (!chooser.isCompleted) {
        chooser.completeError(
          _error(
            BleErrorCode.cancelled,
            operation,
            'Device selection cancelled',
          ),
        );
      }
      // Browser modal has no abort API. Retain its reservation until settlement;
      // late selection is discarded and never retained as an owned device.
    });
  }

  BleException _browserError(Object error) => browserFailure(
    error,
    const BleErrorContext(platform: 'web', operation: 'requestDevice'),
    missing: BleErrorCode.cancelled,
  );

  @override
  BackendRequest<void> startScan() => _rejected(
    BleErrorCode.notSupported,
    'startScan',
    'Use requestDevice from a user gesture; continuous scanning is unsupported',
  );
  @override
  BackendRequest<void> stopScan() => _rejected(
    BleErrorCode.notSupported,
    'stopScan',
    'Continuous scanning is unsupported',
  );
  BackendRequest<BackendConnection> _retryConnect(
    BleDeviceId id,
    Stream<void> retired,
  ) {
    final result = Completer<BackendConnection>();
    BackendRequest<BackendConnection>? child;
    StreamSubscription<void>? waiting;
    late final Timer timer;
    void abort(BleErrorCode code) {
      if (result.isCompleted) return;
      timer.cancel();
      unawaited(waiting?.cancel());
      _retrying.remove(id);
      child?.cancel();
      result.completeError(
        BleException(
          code,
          'Connect retired while waiting for browser cleanup',
          context: BleErrorContext(
            platform: 'web',
            operation: 'connect',
            deviceId: id,
          ),
        ),
      );
    }

    timer = Timer(timeouts.connect, () => abort(BleErrorCode.timeout));
    _retrying[id] = abort;
    Future<void> start() async {
      if (result.isCompleted) return;
      _retrying.remove(id);
      try {
        child = connect(id);
        final connection = await child!.result;
        if (result.isCompleted) {
          await connection.disconnect().result;
        } else {
          result.complete(connection);
        }
      } catch (error, stack) {
        if (!result.isCompleted) result.completeError(error, stack);
      } finally {
        timer.cancel();
      }
    }

    // A cancelled retry removes its listener even if the old browser promise
    // never settles; repeated retries cannot accumulate retained Future handlers.
    waiting = retired.listen(
      (_) {},
      onDone: () {
        unawaited(start());
      },
    );
    return BackendRequest(result.future, () => abort(BleErrorCode.cancelled));
  }

  @override
  BackendRequest<BackendConnection> connect(BleDeviceId deviceId) {
    if (_closed) {
      return _rejected(BleErrorCode.disposed, 'connect', 'Web backend closed');
    }
    if (_state == BleAdapterState.unavailable) {
      return _rejected(
        BleErrorCode.adapterUnavailable,
        'connect',
        'Browser reports Bluetooth unavailable',
      );
    }
    if (_bluetooth == null) {
      return _rejected(
        BleErrorCode.notSupported,
        'connect',
        'Web Bluetooth unavailable',
      );
    }
    if (_connections.containsKey(deviceId)) {
      return _rejected(
        BleErrorCode.alreadyConnected,
        'connect',
        'Device already connected',
      );
    }
    final retiring = _retiring[deviceId];
    if (retiring != null && !_retrying.containsKey(deviceId)) {
      return _retryConnect(deviceId, retiring.stream);
    }
    if (_connecting.containsKey(deviceId) || _leases.containsKey(deviceId)) {
      return _rejected(
        BleErrorCode.connectInProgress,
        'connect',
        'Device has an owned browser connection',
      );
    }
    final device = _devices[deviceId];
    if (device == null) {
      return _rejected(
        BleErrorCode.deviceNotFound,
        'connect',
        'Select this device using requestDevice first',
      );
    }
    final server = device.gatt;
    if (server == null) {
      return _rejected(
        BleErrorCode.notSupported,
        'connect',
        'Selected device has no GATT server',
      );
    }
    if (_nextGeneration >= 9007199254740991) {
      return _rejected(
        BleErrorCode.internal,
        'connect',
        'Connection generation space exhausted',
      );
    }
    final generation = ++_nextGeneration;
    final owner = Object();
    _leases[deviceId] = owner;
    final result = Completer<BackendConnection>();
    final context = BleErrorContext(
      platform: 'web',
      operation: 'connect',
      deviceId: deviceId,
      connectionGeneration: generation,
    );
    Timer? timer;
    void release() {
      if (identical(_leases[deviceId], owner)) {
        _leases.remove(deviceId);
        unawaited(_retiring.remove(deviceId)?.close());
      }
      _connecting.remove(deviceId);
    }

    void abort(BleErrorCode code) {
      if (result.isCompleted) return;
      timer?.cancel();
      Object failure = BleException(
        code,
        '$code during connect',
        context: context,
      );
      try {
        server.disconnect();
      } catch (error) {
        _cleanupFailure ??= error;
        failure = browserFailure(error, context);
      }
      _retiring.putIfAbsent(deviceId, () => StreamController<void>.broadcast());
      result.completeError(failure);
      // Keep the lease until connect settles: a late connect can re-enable GATT.
    }

    _connecting[deviceId] = abort;
    timer = Timer(timeouts.connect, () => abort(BleErrorCode.timeout));
    void failed(Object error) {
      timer?.cancel();
      release();
      if (!result.isCompleted) {
        result.completeError(browserFailure(error, context));
      }
    }

    try {
      unawaited(
        server
            .connect()
            .toDart
            .then<void>((connected) {
              timer?.cancel();
              if (result.isCompleted || _closed) {
                try {
                  connected.disconnect();
                } finally {
                  release();
                }
                return;
              }
              if (!connected.connected) {
                throw _error(
                  BleErrorCode.connectFailed,
                  'connect',
                  'Browser connect returned a disconnected server',
                );
              }
              late final WebBleConnection connection;
              connection = WebBleConnection(
                deviceId,
                generation,
                device,
                connected,
                timeouts,
                (failure) {
                  _cleanupFailure ??= failure;
                  if (identical(_connections[deviceId], connection)) {
                    _connections.remove(deviceId);
                  }
                  release();
                },
              );
              _connecting.remove(deviceId);
              _connections[deviceId] = connection;
              result.complete(connection);
            })
            .catchError((Object error) {
              failed(error);
            }),
      );
    } catch (error) {
      failed(error);
    }
    return BackendRequest(result.future, () => abort(BleErrorCode.cancelled));
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _availabilityTimer?.cancel();
    if (!_ready.isCompleted) {
      _ready.completeError(
        _error(BleErrorCode.disposed, 'getAvailability', 'Web backend closed'),
      );
    }
    final availabilityListener = _availabilityListener;
    _availabilityListener = null;
    if (availabilityListener != null) {
      try {
        _bluetooth!.removeEventListener(
          'availabilitychanged',
          availabilityListener,
        );
      } catch (error) {
        _cleanupFailure ??= error;
      }
    }
    unawaited(_states.close());
    for (final cancel in _retrying.values.toList()) {
      cancel(BleErrorCode.disposed);
    }
    for (final cancel in _connecting.values.toList()) {
      cancel(BleErrorCode.disposed);
    }
    Object? cleanupFailure;
    for (final connection in _connections.values.toList()) {
      try {
        connection.closeGeneration(BleErrorCode.disposed);
      } catch (error) {
        cleanupFailure ??= error;
      }
    }
    final chooser = _chooser;
    _chooser = null;
    if (chooser != null && !chooser.isCompleted) {
      chooser.completeError(
        _error(BleErrorCode.disposed, 'requestDevice', 'Web backend closed'),
      );
    }
    _devices.clear();
    cleanupFailure ??= _cleanupFailure;
    if (cleanupFailure != null) {
      throw browserFailure(
        cleanupFailure,
        const BleErrorContext(platform: 'web', operation: 'close'),
      );
    }
  }
}
