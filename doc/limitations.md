# Limitations

Bletide is experimental. Deterministic tests, cross-compilation and bundle
inspection do not establish radio behavior or independent OS resource release.
See [status](implementation-status.md) for remaining runtime/hardware gates.

## Lifetimes and cancellation

- Close `Ble` and disconnect connections explicitly. Connections identify one generation.
- GATT operations run FIFO with deadlines. Native cancellation retires interrupted
  physical work and waits for bounded cleanup before device reuse. Failed cleanup
  can retire the engine; use a fresh engine after an internal terminal failure.
- Web Bluetooth promises cannot be aborted. Late results are discarded and late
  connections disconnected. A retry waits for old physical connect cleanup;
  a browser promise that never settles can prevent physical reuse.
- Cancelling or timing out a browser chooser cannot dismiss its modal.
- Linux socket opening is synchronous and cannot be preempted by Tokio cancellation.
  Authentication and Hello registration are awaitable, but no full socket-opening
  deadline guarantee is established.
- Controlled Rust ownership counters do not count every OS object. Arbitrary
  upstream destructor failures and Objective-C exceptions remain unverified.

## Identity and attributes

Device IDs are platform-opaque. Known-device reconnect uses platform resolution
where available; Chrome is restricted to granted handles. No implicit rescan or
MAC-to-CoreBluetooth UUID conversion is performed.

Characteristic identity includes its service UUID; descriptor identity includes
its service and characteristic UUID. Observable duplicates within a scope fail
as ambiguous. Upstream set-based models can collapse identical physical instances
before Bletide sees them; the API cannot address every duplicate instance.
Release all notification owners before calling `discoverServices` again. The
façade and native worker reject rediscovery while subscriptions are owned, so
attribute replacement cannot silently strand callbacks on older OS objects.

## Link information

`getMtu()` is a snapshot. Apple infers it from the maximum write-without-response
length plus three; Linux can default to 23 when metadata is absent.
`getWritePayloadLimit()` returns `min(MTU - 3, 512)`, a conservative single ATT
write budget, not a promise of long-write support. Web exposes neither value.

Explicit MTU negotiation and balanced/high/lowPower priority hints are Android
operations. Android 14+ may return an existing negotiation. Priority ACK means
Android accepted a hint, not that the peer negotiated a particular interval.
Fresh connected RSSI is unavailable on Windows/Linux/Web; advertisement RSSI
may also be absent.

## Notifications and buffering

`enableNotifications` awaits setup ACK and returns an owner with `values` and
idempotent `cancel()`. Each owner buffers at most 256 undelivered values during
setup or paused consumption. Overflow reports `gattFailure` and releases that
owner. Ended generations discard buffered values. Final-owner teardown is awaited
before replacement setup; other owners remain subscribed.

Linux D-Bus signal queues hold at most 256 messages per active match. Overflow
terminates the shared transport explicitly. This bounds message count, not byte
size, active-match count or BlueZ's device cache.

## Notification compatibility

Every subscription defaults to `BleNotificationSetupMode.standard`. There is no
production UUID allowlist or automatic device detection for CCCD workarounds.
Applications can select `compat` per notify-capable characteristic:

- Android still requires successful local routing but skips CCCD lookup/write.
- iOS/macOS still submit `setNotifyValue` and await its callback. Only
  `CBATTErrorDomain` code 10 on the owning opted-in request is tolerated.
- Linux, Windows and Web reject this policy with `notSupported` before setup.

Concurrent owners must agree on mode; mixed policies return `invalidState`.
Teardown retains the policy. Await final-owner cancellation before changing it.
Setup acknowledgment does not prove notification delivery from a physical peer.

## Diagnostics and privacy

Diagnostic fields omit BLE payloads, but device identities and native messages
can contain private information. Review/redact logs and hardware reports before
sharing. Bletide installs no default Rust logger; application-configured upstream
logging is a separate channel.
