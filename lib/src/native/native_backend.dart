import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../backend.dart';
import '../errors.dart';
import '../models.dart';
import 'event_bridge.dart';
import 'wire.dart';
import 'native_connection.dart';

BleBackend createBackend({BleTimeouts timeouts = const BleTimeouts()}) =>
    NativeBleBackend(timeouts: timeouts);

final class NativeBleBackend implements BleBackend {
  NativeBleBackend({
    NativeTransport? transport,
    this.timeouts = const BleTimeouts(),
  }) : _bridge = transport ?? NativeEventBridge() {
    _events = _bridge.events.listen(
      _receive,
      onError: _transportFailed,
      onDone: _nativeClosed,
    );
    ready = _initialize();
  }
  final NativeTransport _bridge;
  final BleTimeouts timeouts;
  final Map<int, NativeBleConnection> _connections = {};
  late final StreamSubscription<Uint8List> _events;
  Future<void>? _closing;
  bool _closed = false;
  final _states = StreamController<BleAdapterState>.broadcast();
  final _advertisements = StreamController<BleAdvertisement>.broadcast();
  @override
  late final Future<void> ready;
  @override
  BleAdapterState currentAdapterState = BleAdapterState.unavailable;
  @override
  BleCapabilities get capabilities => BleCapabilities(
    connect: true,
    gatt: true,
    descriptorAccess: true,
    getMtu: true,
    requestMtu: Platform.isAndroid,
    requestConnectionPriority: Platform.isAndroid,
    readRssi: Platform.isMacOS || Platform.isIOS || Platform.isAndroid,
    adapterState: true,
    scan: true,
    continuousScan: true,
    manufacturerData: true,
    serviceData: true,
  );
  Future<void> _initialize() async {
    try {
      final bytes = await _bridge
          .submit(
            10,
            Uint8List(0),
            timeout: const Duration(seconds: 10),
            operationName: 'initialize',
          )
          .result;
      if (bytes.length != 1 || bytes[0] >= BleAdapterState.values.length) {
        throw const BleException(
          BleErrorCode.internal,
          'Invalid adapter state event',
        );
      }
      if (!_closed) {
        currentAdapterState = BleAdapterState.values[bytes[0]];
        _states.add(currentAdapterState);
      }
    } on BleException catch (error) {
      currentAdapterState = error.code == BleErrorCode.permissionDenied
          ? BleAdapterState.unauthorized
          : BleAdapterState.unavailable;
      if (!_closed) _states.add(currentAdapterState);
      _closeAutomatically();
      rethrow;
    }
  }

  @override
  Stream<BleAdapterState> get adapterState => _states.stream;
  @override
  Stream<BleAdvertisement> get advertisements => _advertisements.stream;
  @override
  BackendRequest<BleAdvertisement> requestDevice(BleDeviceRequest options) =>
      BackendRequest(
        Future.error(
          const BleException(
            BleErrorCode.notSupported,
            'Device chooser is available only on Web',
            context: BleErrorContext(operation: 'requestDevice'),
          ),
        ),
        () {},
      );
  @override
  BackendRequest<void> startScan() => _scan(20, 'startScan');
  @override
  BackendRequest<void> stopScan() => _scan(21, 'stopScan');
  @override
  BackendRequest<BackendConnection> connect(BleDeviceId deviceId) {
    if (_closed) return _disposed();
    final request = _bridge.submit(
      30,
      Uint8List.fromList(utf8.encode(deviceId.value)),
      timeout: timeouts.connect,
      operationName: 'connect',
    );
    return BackendRequest(
      request.result.then((payload) {
        if (_closed) {
          throw const BleException(
            BleErrorCode.disposed,
            'Engine closed during connect',
          );
        }
        if (payload.length != 8) {
          throw const BleException(
            BleErrorCode.internal,
            'Invalid connection generation',
          );
        }
        final generation = ByteData.sublistView(payload)
            .getUint64(0, Endian.little);
        if (generation <= 0 || _connections.containsKey(generation)) {
          throw const BleException(
            BleErrorCode.internal,
            'Invalid or reused connection generation',
          );
        }
        final connection = NativeBleConnection(
          _bridge,
          deviceId,
          generation,
          timeouts,
        );
        _connections[generation] = connection;
        if (_closed) {
          connection.invalidate();
          throw const BleException(
            BleErrorCode.disposed,
            'Engine closed during connect',
          );
        }
        return connection;
      }),
      request.cancel,
    );
  }

  BackendRequest<void> _scan(int code, String operation) {
    if (_closed) return _disposed();
    final request = _bridge.submit(
      code,
      Uint8List(0),
      timeout: const Duration(seconds: 10),
      operationName: operation,
    );
    return BackendRequest(request.result.then((_) {}), request.cancel);
  }

  BackendRequest<T> _disposed<T>() => BackendRequest(
    Future.error(
      const BleException(BleErrorCode.disposed, 'Native backend closed'),
    ),
    () {},
  );

  void _receive(Uint8List message) {
    if (_closed) return;
    if (message.length < 16) {
      _malformedEvent('Truncated native event header');
      return;
    }
    final header = ByteData.sublistView(message);
    final kind = header.getUint32(0, Endian.little);
    final code = header.getUint32(12, Endian.little);
    final generation = header.getUint64(4, Endian.little);
    // Retired generations cannot invalidate a fresh connection, even if their
    // late payload/error is malformed. Inspect identity before decoding it.
    if ((kind == 5 || kind == 6) && !_connections.containsKey(generation)) {
      return;
    }
    final payload = Uint8List.sublistView(message, 16);
    try {
      if (code != 0) {
        throw decodeNativeError(
          code,
          payload,
          platform: Platform.operatingSystem,
          operation: kind == 3
              ? 'adapterState'
              : kind == 6
              ? 'notification'
              : 'scan',
          generation: kind == 6 ? generation : null,
        );
      }
      if (kind == 3) {
        if (payload.length != 1 ||
            payload[0] >= BleAdapterState.values.length) {
          throw const FormatException('Invalid adapter state event');
        }
        currentAdapterState = BleAdapterState.values[payload[0]];
        _states.add(currentAdapterState);
      } else if (kind == 4) {
        _advertisements.add(decodeAdvertisement(payload));
      } else if (kind == 5) {
        if (payload.isNotEmpty) {
          throw const FormatException('Invalid disconnection payload');
        }
        _connections.remove(generation)?.invalidate();
      } else if (kind == 6) {
        _connections[generation]?.notification(payload);
      }
    } on FormatException catch (error) {
      _malformedEvent(error.message);
    } on BleException catch (error) {
      if (kind == 3) {
        _states.addError(error);
      } else if (kind == 6) {
        _connections[generation]?.notificationError(error);
      } else {
        _advertisements.addError(error);
      }
    }
  }

  void _malformedEvent(String message) {
    _advertisements.addError(
      BleException(
        BleErrorCode.internal,
        message,
        context: BleErrorContext(
          platform: Platform.operatingSystem,
          operation: 'decodeEvent',
          nativeMessage: message,
        ),
      ),
    );
    _closeAutomatically();
  }

  void _transportFailed(Object error, StackTrace stack) {
    if (_closed) return;
    currentAdapterState = BleAdapterState.unavailable;
    _states.add(currentAdapterState);
    _advertisements.addError(error, stack);
    _closeAutomatically();
  }

  void _nativeClosed() {
    if (_closed) return;
    currentAdapterState = BleAdapterState.unavailable;
    _states.add(currentAdapterState);
    _advertisements.addError(
      const BleException(BleErrorCode.disposed, 'Native engine stopped'),
    );
    _closeAutomatically();
  }

  void _closeAutomatically() {
    unawaited(
      close().then<void>(
        (_) {},
        onError: (Object error, StackTrace stack) {
          // _close reports the error before closing streams. The original failed
          // future stays cached in _closing for an explicit close caller to await.
        },
      ),
    );
  }

  @override
  Future<void> close() => _closing ??= _close();
  Future<void> _close() async {
    _closed = true;
    for (final connection in _connections.values) {
      connection.invalidate();
    }
    _connections.clear();
    try {
      // A transport may deliver events synchronously. Leave its callback before
      // asking it to close; logical disposal and request rejection are immediate.
      await Future<void>.microtask(_bridge.close);
    } catch (error, stack) {
      _advertisements.addError(error, stack);
      rethrow;
    } finally {
      await _events.cancel();
      await _states.close();
      await _advertisements.close();
    }
  }
}
