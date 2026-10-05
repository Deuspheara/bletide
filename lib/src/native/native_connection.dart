import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../backend.dart';
import '../errors.dart';
import '../models.dart';
import 'event_bridge.dart';
import 'wire.dart';

/// Dart owns logical delivery; Rust owns the physical generation and cleanup.
final class NativeBleConnection
    implements BackendConnection, BackendNotificationCompatibility {
  NativeBleConnection(
    this._bridge,
    this.deviceId,
    this.generation,
    this._timeouts,
  );
  final NativeTransport _bridge;
  final BleTimeouts _timeouts;
  @override
  final BleDeviceId deviceId;
  @override
  final int generation;
  final _disconnected = StreamController<void>.broadcast();
  final _notifications = StreamController<BackendNotification>.broadcast();
  bool _closed = false;
  BackendRequest<void>? _disconnecting;
  @override
  Stream<void> get disconnected => _disconnected.stream;
  @override
  Stream<BackendNotification> get notifications => _notifications.stream;

  BackendRequest<T> _request<T>(
    int code,
    String name,
    T Function(Uint8List) decode, {
    BleCharacteristic? characteristic,
    BleDescriptor? descriptor,
    Uint8List? value,
    Duration? timeout,
  }) {
    if (_closed) {
      return BackendRequest(
        Future.error(
          BleException(
            BleErrorCode.disconnected,
            'Connection generation ended',
            context: BleErrorContext(
              operation: name,
              deviceId: deviceId,
              connectionGeneration: generation,
            ),
          ),
        ),
        () {},
      );
    }
    final writer = NativeWriter()..u64(generation);
    if (characteristic != null) {
      writer
        ..uuid(characteristic.serviceUuid)
        ..uuid(characteristic.uuid);
    } else if (descriptor != null) {
      writer
        ..uuid(descriptor.serviceUuid)
        ..uuid(descriptor.characteristicUuid)
        ..uuid(descriptor.uuid);
    }
    if (value != null) writer.bytes(value);
    final request = _bridge.submit(
      code,
      writer.finish(),
      timeout:
          timeout ??
          switch (code) {
            40 => _timeouts.discovery,
            42 || 43 || 47 => _timeouts.write,
            44 || 45 => _timeouts.subscribe,
            _ => _timeouts.read,
          },
      operationName: name,
    );
    return BackendRequest(
      _contextual(
        request.result.then((payload) {
          if (_closed) {
            throw const BleException(
              BleErrorCode.disconnected,
              'Late result from ended generation',
            );
          }
          try {
            return decode(payload);
          } on FormatException catch (error) {
            throw BleException(BleErrorCode.internal, error.message);
          }
        }),
        name,
        characteristic,
        descriptor,
      ),
      request.cancel,
    );
  }

  Future<T> _contextual<T>(
    Future<T> result,
    String name, [
    BleCharacteristic? characteristic,
    BleDescriptor? descriptor,
  ]) async {
    try {
      return await result;
    } on BleException catch (error, stack) {
      final source = error.context;
      Error.throwWithStackTrace(
        BleException(
          error.code,
          error.message,
          context: BleErrorContext(
            platform: source.platform,
            operation: source.operation ?? name,
            deviceId: deviceId,
            connectionGeneration: generation,
            serviceUuid:
                characteristic?.serviceUuid ??
                descriptor?.serviceUuid ??
                source.serviceUuid,
            characteristicUuid:
                characteristic?.uuid ??
                descriptor?.characteristicUuid ??
                source.characteristicUuid,
            descriptorUuid: descriptor?.uuid ?? source.descriptorUuid,
            nativeCode: source.nativeCode,
            nativeMessage: source.nativeMessage,
          ),
        ),
        stack,
      );
    }
  }

  @override
  BackendRequest<List<BleService>> discoverServices() =>
      _request(40, 'discoverServices', decodeServices);
  @override
  BackendRequest<Uint8List> read(BleCharacteristic characteristic) => _request(
    41,
    'read',
    (value) => Uint8List.fromList(value).asUnmodifiableView(),
    characteristic: characteristic,
  );
  @override
  BackendRequest<void> write(
    BleCharacteristic characteristic,
    Uint8List value,
    bool withResponse,
  ) => _request(
    withResponse ? 42 : 43,
    'write',
    _empty,
    characteristic: characteristic,
    value: value,
  );
  @override
  BackendRequest<void> subscribe(BleCharacteristic characteristic) =>
      _request(44, 'subscribe', _empty, characteristic: characteristic);
  @override
  bool get supportsNotificationCompatibility =>
      Platform.isAndroid || Platform.isIOS || Platform.isMacOS;

  @override
  BackendRequest<void> setNotificationsWithCompatibility(
    BleCharacteristic characteristic,
    bool enabled,
    BleNotificationSetupMode setupMode,
  ) => _request(
    enabled ? 44 : 45,
    enabled ? 'subscribe' : 'unsubscribe',
    _empty,
    characteristic: characteristic,
    value: Uint8List.fromList([
      switch (setupMode) {
        BleNotificationSetupMode.standard => 0,
        BleNotificationSetupMode.compat => 1,
      },
    ]),
  );

  @override
  BackendRequest<void> unsubscribe(BleCharacteristic characteristic) =>
      _request(45, 'unsubscribe', _empty, characteristic: characteristic);
  @override
  BackendRequest<Uint8List> readDescriptor(BleDescriptor descriptor) =>
      _request(
        46,
        'readDescriptor',
        (value) => Uint8List.fromList(value).asUnmodifiableView(),
        descriptor: descriptor,
      );
  @override
  BackendRequest<void> writeDescriptor(
    BleDescriptor descriptor,
    Uint8List value,
  ) => _request(
    47,
    'writeDescriptor',
    _empty,
    descriptor: descriptor,
    value: value,
  );
  @override
  BackendRequest<int> readRssi() => _request(48, 'readRssi', (value) {
    final reader = NativeReader(value);
    final rssi = reader.i16();
    reader.finish();
    return rssi;
  });
  @override
  BackendRequest<int> getMtu() => _request(49, 'getMtu', (value) {
    final reader = NativeReader(value);
    final mtu = reader.u16();
    reader.finish();
    if (mtu < 23 || mtu > 517) {
      throw const FormatException('Invalid negotiated MTU');
    }
    return mtu;
  });
  @override
  BackendRequest<int> requestMtu(int mtu) => _request(
    50,
    'mtu.request',
    (value) {
      final reader = NativeReader(value);
      final result = reader.u16();
      reader.finish();
      if (result < 23 || result > 517) {
        throw const FormatException('Invalid negotiated MTU');
      }
      return result;
    },
    value: Uint8List.fromList([mtu & 255, mtu >> 8]),
    timeout: _timeouts.write,
  );
  @override
  BackendRequest<void> requestConnectionPriority(
    BleConnectionPriority priority,
  ) => _request(
    51,
    'connection.priority',
    _empty,
    value: Uint8List.fromList([priority.index]),
    timeout: _timeouts.write,
  );
  @override
  BackendRequest<void> disconnect() {
    if (_disconnecting != null) return _disconnecting!;
    if (_closed) return BackendRequest(Future.value(), () {});
    final writer = NativeWriter()..u64(generation);
    final request = _bridge.submit(
      31,
      writer.finish(),
      timeout: _timeouts.disconnect,
      operationName: 'disconnect',
    );
    invalidate();
    return _disconnecting = BackendRequest(
      request.result.then(_empty),
      request.cancel,
    );
  }

  void notificationError(BleException error) {
    if (_closed) return;
    _notifications.addError(
      BleException(
        error.code,
        error.message,
        context: BleErrorContext(
          platform: error.context.platform,
          operation: 'notification',
          deviceId: deviceId,
          connectionGeneration: generation,
          nativeCode: error.context.nativeCode,
          nativeMessage: error.context.nativeMessage,
        ),
      ),
    );
  }

  void notification(Uint8List payload) {
    if (_closed) return;
    final reader = NativeReader(payload);
    final service = reader.uuid();
    final characteristic = reader.uuid();
    _notifications.add(
      BackendNotification(
        generation,
        '$service/$characteristic',
        reader.take(reader.remaining),
      ),
    );
  }

  void invalidate() {
    if (_closed) return;
    _closed = true;
    _disconnected.add(null);
    unawaited(_disconnected.close());
    unawaited(_notifications.close());
  }
}

void _empty(Uint8List payload) {
  if (payload.isNotEmpty) {
    throw const FormatException('Unexpected native response payload');
  }
}
