# Architecture

One Dart package and one Rust crate. The public façade manages logical stream
ownership, immutable models, typed errors, cancellation, and one FIFO GATT queue
per connection. Backends implement platform calls, not business protocols.
The [organization guide](code-organization.md) maps source locations, mutable
state and the reasons for retaining or separating each substantial file.

Native calls follow `Ble → NativeBleBackend → NativeEventBridge → dart:ffi →
Rust engine → btleplug`. The small audited ABI is in `src/bletide.h`; matching Dart
bindings use `@Native` and the build-hook asset ID `src/native/bindings.dart`.

One process-wide Tokio runtime hosts a supervisor. The supervisor owns a JoinSet
of engine tasks. Each engine owns its request tasks, request registry, stop signal,
and port sink. One adapter worker owns its Manager, adapter, central event stream
and logical scan intent; it serializes scan setup/stop and joins cleanup. On
Android a process coordinator owns the shared physical scanner and grants weakly
registered engine lease tokens. The adapter worker owns a
JoinSet of connection workers plus a generation registry. Each connection worker
owns its peripheral, discovered service cache, command queue, notification stream
and active subscription keys. Shutdown acknowledgement is sent only after all
workers have joined and the engine task has terminated and its registry entry has been removed.

There is one native event transport: NativeApi.postCObject sends ordinary copied
Uint8 typed data to a ReceivePort. Each event is little endian: kind:u32,
request:u64, errorCode:u32, followed by binary payload or UTF-8 error diagnostics.
The VM copies bytes synchronously. No event pointers, native allocations, Rust
objects, or Dart callback pointers cross an asynchronous boundary.

ABI v2 uses positive monotonically increasing handles and negative portable
error codes for immediate command rejection. Zero is an invalid handle; a zero
close result means the engine registry has already retired the engine, and Dart
still waits for its terminal port event. Async requests
return a request ID immediately. Payloads are copied before the command returns.
ABI mismatch fails before allocating an engine. Error events keep the 16-byte
header. Bit 31 of the status marks optional native-code metadata: a little-endian
u32 UTF-8 byte length and native-code bytes precede the remaining message bytes.
The low 31 bits contain the portable BLE classification. Errors without native
metadata keep their original message-only payload. The shared decoder checks
lengths/UTF-8 before exposing context; malformed metadata retires the transport.
Platform codes are distinct from portable classifications, without parsing text.

The separate Web backend is selected with a conditional import; browser builds
never import dart:ffi. It implements the browser chooser, connection and GATT
operations through modern JS interop. Chrome contract tests and web compilation
provide deterministic/build evidence; physical browser BLE verification remains
pending.

## Following a write

1. [`BleConnection.write`](../lib/src/ble/connection.dart) copies the caller's bytes and
   enqueues cancellable work in that generation's FIFO queue.
2. [`NativeBleConnection.write`](../lib/src/native/native_connection.dart) selects
   operation 42 (with response) or 43 (without response). Its request encoder
   adds the generation, service UUID, characteristic UUID and copied bytes.
3. [`NativeEventBridge.submit`](../lib/src/native/event_bridge.dart) allocates a
   temporary input buffer, calls the [C binding](../lib/src/native/bindings.dart)
   and frees that buffer in `finally`. The call returns a request ID immediately.
4. [`bletide_command`](../rust/src/ffi.rs) copies the bytes before returning.
   The [engine](../rust/src/engine.rs) registers the deadline/cancellation and
   routes work through the [adapter worker](../rust/src/scan.rs) to the matching
   connection generation.
5. The [connection worker](../rust/src/connection.rs) serializes the operation;
   its [native driver](../rust/src/connection/native.rs) resolves the cached characteristic, validates the write
   property and awaits `btleplug::Peripheral::write` with the selected write type.
6. The terminal result travels through the [copied port event](../rust/src/event.rs)
   to the bridge's pending request, then through backend generation validation
   to the public future. Cancellation/timeout yields one result and retires
   interrupted physical work; late completion cannot revive the generation.

The Web path instead awaits the browser write promise in
[`WebBleConnection`](../lib/src/web/web_connection.dart), behind the same public
FIFO coordinator. It never passes through native bindings.

The Linux transport remains bluez-async through btleplug. Its pinned constructor
extension returns the unspawned D-Bus I/O future; a shared session owner holds
the actual Tokio task handle. The manager is retained before adapter enumeration,
and explicit shutdown aborts and joins that task. Message-stream Drop retires
local callbacks synchronously; bounded remote removals run inside that same
owned future. The worker drops event registrations before transport shutdown.
Recorded cleanup errors survive cancellation. Source manifests and reviewed
patches for both crates live in `rust/vendor`; build-hook dependencies include
that entire directory. No separate BlueZ implementation is introduced.

The Dart FIFO is shared by native and browser façades and owns logical result/
delivery rules. The Rust queue also protects direct FFI requests and owns physical
work retirement; both queues retain their distinct lifecycle responsibilities.
Writes are copied at the caller, encoded into a bounded request and copied at the
synchronous FFI entry before temporary Dart storage is freed. Diagnostic metadata
has no payload field, and Bletide installs no default Rust logger. The shared
write regression checks both modes and payload-free diagnostics/Dart output;
upstream logging configured by an application is a separate channel.
