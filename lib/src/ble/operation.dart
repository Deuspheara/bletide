part of '../ble.dart';

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
