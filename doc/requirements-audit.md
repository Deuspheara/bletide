# Code review and release checklist

Use this checklist for changes to the public API, native workers and vendored
platform backends. A deterministic assertion proves its controlled scenario;
it does not prove every OS callback ordering or physical resource lifetime.

## Reviewed design boundaries

- The public API owns immutable snapshots, operation deadlines and logical subscriptions.
- Native FFI copies command bytes synchronously and sends copied VM-port events.
- The C ABI checks its version before engine allocation and bounds payload sizes.
- Each native engine owns its tasks; connection generations reject stale work.
- Cancellation retires physical work and joins cleanup before native reuse.
- Notification readiness waits for setup acknowledgment; final-owner teardown is awaited.
- Connection and adapter workers guard polling/disposal panics at inspected boundaries.
- Linux transport ownership retains and joins its D-Bus resource task.
- Android scanner and peripheral leases coordinate shared process resources.
- Windows callback retirement and Apple reply ownership have controlled regressions.
- Structured native errors retain platform causes without parsing human-readable messages.

The tests in [testing](testing.md) and [mandatory races](mandatory-races.md)
provide assertion-level pointers. Root strict Clippy does not execute dependency
tests; vendored suites must run separately.

## Review every change

- Keep deadline, cancellation and success races single-result and observable.
- Retain setup/teardown owners until the corresponding acknowledgment or terminal failure.
- Reject queued work and late callbacks from a retired connection generation.
- Preserve original platform errors when later cleanup also fails.
- Bound queues, buffers and retained retry state; report overflow explicitly.
- Keep unsafe code at justified foreign boundaries with documented preconditions.
- Keep BLE payload bytes out of library diagnostics and default output.
- Add regression coverage for a reproduced bug or meaningful lifecycle invariant.
- Refresh vendored patches, source hashes and license inventories together.
- Rebuild affected consumers and run their bundle verifiers after native changes.

## Remaining verification

Current Windows loading/WinRT failure injection, healthy Linux/BlueZ execution,
current Android API/ABI runtime probes and signed iOS execution are incomplete.
No physical peripheral/platform combination has completed the hardware suite.
Arbitrary upstream destructor failures, Objective-C exceptions and independent
OS object retention are not established by Rust counters or fake backends.

Before a stable release, review remaining foreign/unsafe boundaries, upstream
caches, logging and actual scan/GATT/notification teardown on each claimed
platform. Track concrete reproducible failures as issues; keep the public
[status](implementation-status.md) aligned with the verified revision.
