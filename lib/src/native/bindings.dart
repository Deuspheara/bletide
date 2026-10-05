import 'dart:ffi';

@Native<Int64 Function()>(symbol: 'bletide_abi_version')
external int abiVersion();

@Native<Int64 Function(Int64, Pointer<Void>)>(symbol: 'bletide_open')
external int openEngine(int port, Pointer<Void> postCObject);

@Native<Int64 Function(Uint64, Uint32, Pointer<Uint8>, Uint32, Uint32)>(
  symbol: 'bletide_command',
)
external int command(
  int engine,
  int operation,
  Pointer<Uint8> payload,
  int length,
  int timeoutMs,
);

@Native<Int64 Function(Uint64, Uint64)>(symbol: 'bletide_cancel')
external int cancel(int engine, int request);

@Native<Int64 Function(Uint64)>(symbol: 'bletide_close')
external int closeEngine(int engine);
