# API contracts and troubleshooting

`Ble()` owns an engine. `ready` confirms initialization, not that the adapter is
powered or a device is present; observe `adapterState` as well. Close the engine
in `finally`. Capabilities describe available operations, not permission grants.

| Operation | Ownership / result |
|---|---|
| `scan` | Listening acquires a shared native scan lease; cancel it to await stop. Stream errors end that lease. Use Web's chooser instead. |
| `connect` | Returns one generation. Its deadline includes initialization. Duplicate connects fail. Reconnect explicitly and rediscover. |
| `discoverServices` | Returns scoped attribute identities. Release notification owners before rediscovery. |
| `read` / `write` | FIFO per connection, deadline includes queue time. Reads and write inputs are copied. No protocol framing or automatic chunking. |
| `enableNotifications` | Awaits setup ACK; returns a single-listener owner. Values can precede ACK and are buffered. Handle stream errors. |
| `subscribe` | Lazy stream convenience; each listener owns a share. Setup failures arrive as stream errors. |
| `cancel` / `disconnect` / `close` | Await cleanup. Idempotent ownership release; errors remain observable. Never reuse an ended connection object. |

Cancellation and deadlines establish one logical result. Cancelling running GATT
can disconnect the generation; browser promises may remain physically pending.
The cancellation token passed to `enableNotifications` covers acquisition only;
after success use the owner's `cancel()`. A write timeout does not prove the peer
received nothing. Decide retry/idempotency rules in your peripheral protocol.
See [limitations](limitations.md) for payload budgets, duplicate UUIDs and buffering.

## Deterministic application tests

Import `package:bletide/testing.dart` and inject `FakeBleBackend` into `Ble`.
Start an operation, obtain its pending ACK with `waitFor<T>()`, then `complete`
or `fail` it. Awaiting an operation before providing its fake ACK will hang until
its deadline. `completeConnect` creates the fake physical generation; use its
notification and remote-disconnect methods to drive events. Close in teardown.

The executable [recipe tests](../example/test/recipes_test.dart) demonstrate this
pattern including writes and cleanup. They test application orchestration;
compiled ABI and real-platform checks remain necessary for backend changes.

## Troubleshooting

| Symptom | Check / next action |
|---|---|
| `permissionDenied` or unauthorized adapter | Declare and request permissions before BLE use. Review the [platform setup](platforms.md). |
| No scan result | Verify advertising, adapter power and Android location settings on older OS versions. Service filters match advertised UUIDs, not every GATT service. Cancel a timed-out scan lease. |
| Web chooser / service unavailable | Use HTTPS or localhost and a supporting browser. Invoke chooser directly in a button handler; grant needed services with filters or `optionalServices`. |
| `deviceNotFound` on reconnect | Use the discovered opaque ID; native OS resolution and browser grants differ. Rescan/reselect if the OS no longer knows it. |
| `invalidState` during rediscovery | Await cancellation of all notification owners before replacing discovered attributes. |
| `notSupported` | Check capability and characteristic properties. MTU requests/priority hints are Android-only; Web cannot report write budgets. |
| Timeout / disconnect after a write | Observe connection states and native cause fields. Reconnect/rediscover before retry; avoid replaying non-idempotent commands blindly. |
| Notifications stop or overflow | Consume values promptly, handle `onError`/`onDone`, and await owner cancellation. Standard CCCD setup is the default; compat is an explicit peripheral-specific exception. |
| Native build/load failure | Install pinned Rust plus host SDK tools. Inspect the consumer bundle using [testing commands](testing.md); compilation alone does not prove loading. |

For bug reports include revision, OS/SDK versions, fake versus physical reproduction,
and redacted `BleException.context` fields. Diagnostics omit payload fields but
identities and native messages may be private. Avoid sharing raw explorer output.
