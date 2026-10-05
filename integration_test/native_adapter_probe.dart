// Explicit OS integration probe, outside the hardware-free unit test suite.
// Run only in an isolated environment with a deliberately unavailable adapter.
import 'dart:async';
import 'dart:ffi';
import 'dart:isolate';
import 'dart:io';
import 'dart:typed_data';

import 'package:bletide/src/native/wire.dart';

Future<void> main(List<String> arguments) async {
  if (arguments.length != 2) {
    throw ArgumentError(
      'Usage: native_adapter_probe.dart LIBRARY EXPECTED_CODE',
    );
  }
  final library = DynamicLibrary.open(arguments[0]);
  final expected = int.parse(arguments[1]);
  final version = library.lookupFunction<Int64 Function(), int Function()>(
    'bletide_abi_version',
  );
  final open = library
      .lookupFunction<
        Int64 Function(Int64, Pointer<Void>),
        int Function(int, Pointer<Void>)
      >('bletide_open');
  final command = library
      .lookupFunction<
        Int64 Function(Uint64, Uint32, Pointer<Uint8>, Uint32, Uint32),
        int Function(int, int, Pointer<Uint8>, int, int)
      >('bletide_command');
  final close = library
      .lookupFunction<Int64 Function(Uint64), int Function(int)>(
        'bletide_close',
      );
  if (version() != 2) throw StateError('Unexpected ABI version');
  for (var cycle = 0; cycle < 10; cycle++) {
    final events = StreamController<Uint8List>.broadcast();
    final port = RawReceivePort((dynamic message) {
      if (message is Uint8List) events.add(message);
    });
    final engine = open(port.sendPort.nativePort, NativeApi.postCObject.cast());
    if (engine <= 0) {
      port.close();
      await events.close();
      throw StateError('Engine open failed: $engine');
    }
    Future<Uint8List> response(int kind, int request) => events.stream
        .firstWhere((event) {
          if (event.length < 16) return false;
          final header = ByteData.sublistView(event);
          return header.getUint32(0, Endian.little) == kind &&
              header.getUint64(4, Endian.little) == request;
        })
        .timeout(const Duration(seconds: 10));
    try {
      final request = command(engine, 10, nullptr, 0, 5000);
      if (request <= 0) throw StateError('Initialize rejected: $request');
      final event = await response(1, request);
      final encodedCode = ByteData.sublistView(event)
          .getUint32(12, Endian.little);
      final code = encodedCode & 0x7fffffff;
      final error = decodeNativeError(
        encodedCode,
        Uint8List.sublistView(event, 16),
        platform: Platform.operatingSystem,
        operation: 'initialize',
      );
      if (cycle == 0) {
        stdout.writeln('Adapter initialization: code=$code, $error');
      }
      if (code != expected) {
        throw StateError('Expected adapter error $expected, received $code');
      }
    } finally {
      try {
        final request = close(engine);
        if (request <= 0) throw StateError('Close rejected: $request');
        final event = await response(2, request);
        if (ByteData.sublistView(event).getUint32(12, Endian.little) != 0) {
          throw StateError('Native shutdown failed');
        }
      } finally {
        port.close();
        await events.close();
      }
      if (close(engine) != 0) {
        throw StateError('Repeated close retained an engine');
      }
    }
  }
  stdout.writeln(
    '10 real adapter initialization/acknowledged-close cycles passed',
  );
}
