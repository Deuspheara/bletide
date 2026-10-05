/// Deterministic test backend. Requests finish only when the test resolves them.
library;

import 'dart:async';
import 'dart:typed_data';

import 'backend.dart';
import 'errors.dart';
import 'models.dart';

final class FakeOperation<T> {
  FakeOperation(
    this.name,
    this._onDone, {
    this.deviceId,
    this.generation,
    this.value,
  }) {
    request = BackendRequest(_completer.future, cancel);
  }
  final String name;
  final BleDeviceId? deviceId;
  final int? generation;
  final Uint8List? value;
  final void Function() _onDone;
  final Completer<T> _completer = Completer<T>();
  late final BackendRequest<T> request;
  bool get completed => _completer.isCompleted;
  void complete(T result) {
    if (completed) return;
    _onDone();
    _completer.complete(result);
  }

  void fail(BleException error) {
    if (completed) return;
    _onDone();
    _completer.completeError(error);
  }

  void cancel() => fail(
    const BleException(BleErrorCode.cancelled, 'Fake request cancelled'),
  );
}

final class FakeBleBackend implements BleBackend {
  FakeBleBackend({
    this.closeError,
    this.initialization,
    this.capabilities = const BleCapabilities(
      scan: true,
      continuousScan: true,
      connect: true,
      gatt: true,
      descriptorAccess: true,
      readRssi: true,
      getMtu: true,
      requestMtu: true,
      requestConnectionPriority: true,
      adapterState: true,
      manufacturerData: true,
      serviceData: true,
    ),
  });
  @override
  final BleCapabilities capabilities;

  /// Inject a platform cleanup failure after owned fake resources are released.
  final BleException? closeError;
  final Future<void>? initialization;
  @override
  Future<void> get ready => initialization ?? Future.value();
  final _states = StreamController<BleAdapterState>.broadcast(sync: true);
  final _ads = StreamController<BleAdvertisement>.broadcast(sync: true);
  final List<FakeOperation<dynamic>> _pending = [];
  final _operations = StreamController<FakeOperation<dynamic>>.broadcast();
  final List<FakeBackendConnection> _connections = [];
  final List<String> history = [];
  bool _closed = false;
  bool scanning = false;
  int _generation = 0;
  @override
  BleAdapterState currentAdapterState = BleAdapterState.ready;
  @override
  Stream<BleAdapterState> get adapterState => _states.stream;
  @override
  Stream<BleAdvertisement> get advertisements => _ads.stream;
  List<FakeOperation<dynamic>> get pending => List.unmodifiable(_pending);

  /// Observe issued operations to drive a deterministic simulated peripheral.
  Stream<FakeOperation<dynamic>> get operations => _operations.stream;
  Future<FakeOperation<T>> waitFor<T>(String name) async {
    for (final operation in _pending) {
      if (operation.name == name) return operation as FakeOperation<T>;
    }
    return await _operations.stream.firstWhere(
      (operation) => operation.name == name,
    ) as FakeOperation<T>;
  }

  int get liveConnections => _connections.where((c) => !c.closed).length;
  int get subscriptions =>
      _connections.fold(0, (count, c) => count + c.subscriptions.length);
  FakeOperation<T> next<T>(String name) =>
      _pending.firstWhere((op) => op.name == name) as FakeOperation<T>;

  FakeOperation<T> operation<T>(
    String name, {
    BleDeviceId? deviceId,
    int? generation,
    Uint8List? value,
  }) {
    late final FakeOperation<T> op;
    op = FakeOperation(
      name,
      () => _pending.remove(op),
      deviceId: deviceId,
      generation: generation,
      value: value == null
          ? null
          : Uint8List.fromList(value).asUnmodifiableView(),
    );
    history.add(name);
    if (_closed) {
      op.fail(const BleException(BleErrorCode.disposed, 'Fake backend closed'));
    } else {
      _pending.add(op);
      _operations.add(op);
    }
    return op;
  }

  void advertise(BleAdvertisement advertisement) {
    if (!_closed && scanning) _ads.add(advertisement);
  }

  /// Inject a failed adapter event source without throwing in the test zone.
  void failAdapterState(BleException error) {
    if (_closed) return;
    currentAdapterState = BleAdapterState.unavailable;
    _states.addError(error);
    scanning = false;
    for (final connection in _connections.toList()) {
      connection.remoteDisconnect();
    }
  }

  void setAdapterState(BleAdapterState state) {
    if (_closed) return;
    currentAdapterState = state;
    _states.add(state);
    if (state != BleAdapterState.ready) {
      scanning = false;
      for (final connection in _connections.toList()) {
        connection.remoteDisconnect();
      }
    }
  }

  @override
  BackendRequest<BleAdvertisement> requestDevice(BleDeviceRequest options) =>
      operation<BleAdvertisement>('requestDevice').request;

  @override
  BackendRequest<void> startScan() {
    final op = operation<void>('scan.start');
    return BackendRequest(
      op.request.result.then((_) {
        scanning = true;
      }),
      op.cancel,
    );
  }

  @override
  BackendRequest<void> stopScan() {
    final op = operation<void>('scan.stop');
    return BackendRequest(
      op.request.result.then((_) {
        scanning = false;
      }),
      op.cancel,
    );
  }

  @override
  BackendRequest<BackendConnection> connect(BleDeviceId deviceId) =>
      operation<BackendConnection>('connect', deviceId: deviceId).request;
  FakeBackendConnection completeConnect(BleDeviceId deviceId) {
    final op = _pending.firstWhere(
      (op) => op.name == 'connect' && op.deviceId == deviceId,
    ) as FakeOperation<BackendConnection>;
    final connection = FakeBackendConnection._(this, deviceId, ++_generation);
    _connections.add(connection);
    op.complete(connection);
    return connection;
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    scanning = false;
    for (final op in _pending.toList()) {
      op.fail(const BleException(BleErrorCode.disposed, 'Fake backend closed'));
    }
    for (final connection in _connections.toList()) {
      await connection.close();
    }
    _connections.clear();
    await _operations.close();
    await _states.close();
    await _ads.close();
    final error = closeError;
    if (error != null) throw error;
  }
}

final class FakeBackendConnection implements BackendConnection {
  FakeBackendConnection._(this.backend, this.deviceId, this.generation);
  final FakeBleBackend backend;
  @override
  final BleDeviceId deviceId;
  @override
  final int generation;
  final _disconnected = StreamController<void>.broadcast(sync: true);
  final _notifications = StreamController<BackendNotification>.broadcast(
    sync: true,
  );
  final Set<String> subscriptions = {};
  bool closed = false;
  @override
  Stream<void> get disconnected => _disconnected.stream;
  @override
  Stream<BackendNotification> get notifications => _notifications.stream;
  BackendRequest<T> _op<T>(String name, {Uint8List? value}) {
    final op = backend.operation<T>(
      name,
      deviceId: deviceId,
      generation: generation,
      value: value,
    );
    if (closed) {
      op.fail(
        const BleException(BleErrorCode.disconnected, 'Fake connection closed'),
      );
    }
    return op.request;
  }

  void emitNotification(
    BleCharacteristic characteristic,
    Uint8List value, {
    int? sourceGeneration,
  }) {
    if (!closed) {
      _notifications.add(
        BackendNotification(
          sourceGeneration ?? generation,
          characteristic.key,
          value,
        ),
      );
    }
  }

  void remoteDisconnect() {
    if (closed) return;
    closed = true;
    subscriptions.clear();
    _disconnected.add(null);
    backend._connections.remove(this);
    unawaited(_disconnected.close());
    unawaited(_notifications.close());
    for (final op in backend._pending.toList()) {
      if (op.generation == generation) {
        op.fail(
          const BleException(BleErrorCode.disconnected, 'Remote disconnect'),
        );
      }
    }
  }

  Future<void> close() async {
    remoteDisconnect();
    await _disconnected.close();
    await _notifications.close();
  }

  @override
  BackendRequest<List<BleService>> discoverServices() => _op('discover');
  @override
  BackendRequest<Uint8List> read(BleCharacteristic characteristic) =>
      _op('read');
  @override
  BackendRequest<void> write(
    BleCharacteristic characteristic,
    Uint8List value,
    bool withResponse,
  ) => _op(withResponse ? 'write' : 'writeWithoutResponse', value: value);
  @override
  BackendRequest<Uint8List> readDescriptor(BleDescriptor descriptor) =>
      _op('readDescriptor');
  @override
  BackendRequest<void> writeDescriptor(
    BleDescriptor descriptor,
    Uint8List value,
  ) => _op('writeDescriptor', value: value);
  @override
  BackendRequest<void> subscribe(BleCharacteristic characteristic) {
    final request = _op<void>('subscribe');
    return BackendRequest(
      request.result.then((_) {
        if (!closed) subscriptions.add(characteristic.key);
      }),
      request.cancel,
    );
  }

  @override
  BackendRequest<void> unsubscribe(BleCharacteristic characteristic) {
    final request = _op<void>('unsubscribe');
    return BackendRequest(
      request.result.then((_) {
        subscriptions.remove(characteristic.key);
      }),
      request.cancel,
    );
  }

  @override
  BackendRequest<int> readRssi() => _op('rssi');
  @override
  BackendRequest<int> getMtu() => _op('getMtu');
  @override
  BackendRequest<int> requestMtu(int mtu) => _op('requestMtu');
  @override
  BackendRequest<void> requestConnectionPriority(
    BleConnectionPriority priority,
  ) => _op('connection.priority', value: Uint8List.fromList([priority.index]));
  @override
  BackendRequest<void> disconnect() {
    final request = _op<void>('disconnect');
    return BackendRequest(request.result.then((_) => close()), request.cancel);
  }
}
