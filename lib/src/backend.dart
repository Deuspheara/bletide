import 'dart:typed_data';

import 'models.dart';

/// Internal backends use cancellation-aware requests. The façade never wraps
/// an uncancellable platform Future in Future.timeout.
final class BackendRequest<T> {
  BackendRequest(this.result, this.cancel);
  final Future<T> result;
  final void Function() cancel;
}

abstract interface class BleBackend {
  Future<void> get ready;
  BleCapabilities get capabilities;
  BleAdapterState get currentAdapterState;
  Stream<BleAdapterState> get adapterState;
  Stream<BleAdvertisement> get advertisements;
  BackendRequest<BleAdvertisement> requestDevice(BleDeviceRequest options);
  BackendRequest<void> startScan();
  BackendRequest<void> stopScan();
  BackendRequest<BackendConnection> connect(BleDeviceId deviceId);
  Future<void> close();
}

abstract interface class BackendConnection {
  BleDeviceId get deviceId;
  int get generation;
  Stream<void> get disconnected;
  Stream<BackendNotification> get notifications;
  BackendRequest<List<BleService>> discoverServices();
  BackendRequest<Uint8List> read(BleCharacteristic characteristic);
  BackendRequest<void> write(
    BleCharacteristic characteristic,
    Uint8List value,
    bool withResponse,
  );
  BackendRequest<Uint8List> readDescriptor(BleDescriptor descriptor);
  BackendRequest<void> writeDescriptor(
    BleDescriptor descriptor,
    Uint8List value,
  );
  BackendRequest<void> subscribe(BleCharacteristic characteristic);
  BackendRequest<void> unsubscribe(BleCharacteristic characteristic);
  BackendRequest<int> readRssi();
  BackendRequest<int> getMtu();
  BackendRequest<int> requestMtu(int mtu);
  BackendRequest<void> requestConnectionPriority(
    BleConnectionPriority priority,
  );
  BackendRequest<void> disconnect();
}

final class BackendNotification {
  BackendNotification(this.generation, this.characteristicKey, Uint8List value)
    : value = Uint8List.fromList(value).asUnmodifiableView();
  final int generation;
  final String characteristicKey;
  final Uint8List value;
}

/// Optional native compatibility extension; ordinary backends remain unchanged.
abstract interface class BackendNotificationCompatibility {
  bool get supportsNotificationCompatibility;
  BackendRequest<void> setNotificationsWithCompatibility(
    BleCharacteristic characteristic,
    bool enabled,
    BleNotificationSetupMode setupMode,
  );
}
