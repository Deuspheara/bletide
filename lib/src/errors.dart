import 'models.dart';

enum BleErrorCode {
  adapterUnavailable,
  adapterDisabled,
  permissionDenied,
  deviceNotFound,
  alreadyConnected,
  connectInProgress,
  connectFailed,
  disconnected,
  timeout,
  cancelled,
  notSupported,
  serviceNotFound,
  characteristicNotFound,
  descriptorNotFound,
  gattFailure,
  invalidState,
  disposed,
  internal,
  unknown,
}

final class BleErrorContext {
  const BleErrorContext({
    this.platform,
    this.operation,
    this.deviceId,
    this.serviceUuid,
    this.characteristicUuid,
    this.descriptorUuid,
    this.nativeCode,
    this.nativeMessage,
    this.connectionGeneration,
  });
  final String? platform;
  final String? operation;
  final BleDeviceId? deviceId;
  final BleUuid? serviceUuid;
  final BleUuid? characteristicUuid;
  final BleUuid? descriptorUuid;
  final String? nativeCode;
  final String? nativeMessage;
  final int? connectionGeneration;
}

final class BleException implements Exception {
  const BleException(
    this.code,
    this.message, {
    this.context = const BleErrorContext(),
  });
  final BleErrorCode code;
  final String message;
  final BleErrorContext context;
  @override
  String toString() => 'BleException(${code.name}): $message';
}
