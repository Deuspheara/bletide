# Lifecycle and ownership

Applications explicitly await `Ble.close()`. Garbage collection is not the
cleanup mechanism. `close()` and connection `disconnect()` are idempotent.

The façade owns the backend, adapter/advertisement subscriptions, diagnostics,
connection registry, and pending connects. Duplicate connects return
`connectInProgress` while an attempt is running and `alreadyConnected` after it
succeeds. `Ble.disconnect(deviceId)` can cancel a connecting attempt.
Connection initialization has one readiness observer for the façade. Cancelled
attempts are removed from its active registry; retrying while initialization is
pending does not add another readiness continuation. Initialization failure is
preserved for later attempts.

Each scan stream owns one logical scanner reference while it has a listener.
The first owner starts one physical scan; the last owner stops it. Distinct
filters share an unrestricted physical scan and apply portable post-filtering.
Cancellation during startup still schedules a stop after startup resolves.
Before backend initialization finishes, scan ownership remains in the listener
registry. The façade's single readiness observer starts only remaining owners.
Cancelling these listeners or closing the engine does not await readiness; late
readiness cannot start a retired scanner. Initialization failures reach active
and later scan listeners through stream errors.

A BleConnection is permanently bound to one physical generation. Disconnect,
remote loss, and adapter loss invalidate it and cancel queued/running work.
Reconnect returns a new object. Old objects reject operations. Notification
events carry a generation and stale generations are discarded.

Each connection owns a FIFO queue. A request has one terminal completer, a timer,
and an optional cancellation listener. Timeout/cancellation completes once,
propagates cancellation to the backend, cancels the timer, and releases the queue.
The backend must invalidate late physical completions and own their cleanup.

One broadcast notification stream per characteristic shares physical setup.
Only the final listener triggers teardown. Cancellation during setup waits for
setup and compensating unsubscribe; a replacement listener can retain pending
setup. The departing owner's `cancel()` waits for its teardown acknowledgement
and reports stop errors. Replacement setup after a pending unsubscribe does not
delay that acknowledgement. A replacement awaiting teardown receives its original
failure, including the backend cause, before generation invalidation. A failed unsubscribe invalidates the generation and
attempts disconnect; disconnect failure is diagnosed and remains observable
through the connection's idempotent `disconnect()` future. Remote loss or engine
close releases pending cancellations and prevents further delivery. The façade
copies write buffers at enqueue time and copies read values. Notification payloads
are copied, read-only bytes. A paused listener retains values while connected,
but delivery checks this connection generation again on resume and discards
buffered values after disconnect or engine close, including after a fresh connect.
The awaitable notification owner buffers at most 256 undelivered values, including
while unlistened or paused. Overflow reports `gattFailure` with operation
`notification.buffer`, discards its undelivered values and releases that owner;
other owners and the physical connection remain active. Backend transport overflow can still retire the
entire generation as described below. Explicit owner cancellation also discards
its undelivered values.

Rust requests transition queued → running → completed/failed/cancelled/timedOut. The
engine registry bounds pending requests at 1024 and input payloads at 1 MiB.
The native process admits at most 128 engines across isolates. Its start queue is
also bounded at 128, and admission uses a nonblocking enqueue under the registry
lock. Excess opens return `invalidState`; failed enqueues roll back registration.
Closing engines retain their slot until their task joins and the registry retires
them. Await close before replacing an engine at capacity. These bounds apply to
library registrations/queued starts, not independent OS objects or caller-owned
transient allocations.
The engine cancels and joins work before shutdown acknowledgement. Dart then
closes its ReceivePort and completes any remaining futures. Positive shutdown
acknowledgements must match the close request. Request zero is the supervisor
terminal event and is also valid when termination races an explicit close. A
zero return from native close means the registry is already retired; Dart still
waits for the terminal event so cleanup failure is not discarded. Unsolicited
terminal failures are reported on the event stream and cached for later close
callers. A dead port safely
rejects messages; failed delivery requests engine shutdown. An engine-owned
one-second timer sends an internal port-liveness probe even while idle. A failed
probe follows the same joined shutdown path, so a quiet engine is not retained
forever after isolate teardown. The timer stops with the engine; no BLE scan is
started by the probe and Dart does not expose it as a diagnostic event. Cleanup
may take additional time up to the platform cleanup deadlines. No Dart callback
pointer is retained, including across isolate teardown/hot restart.

The native adapter worker owns its scan intent and event stream. Android
process-wide scanner leases share the upstream global adapter: the first lease
starts scanning and only the last release calls OS stop. Cancelling a start
while waiting for coordination never acquires a lease. Interrupted OS calls
retain uncertain state until compensating cleanup; failed cleanup is reported
and a later owner must recover the scanner before starting it. Lease tokens
do not retain closed engines. Cancellation/timeout during startup
triggers compensating stop before another scan command runs. Engine shutdown
joins scanner cleanup; cleanup failures reach close() as typed errors. Dart
subscriptions still close when native cleanup fails. Each native connection worker owns its generation and serializes platform calls.
Cancellation/timeout of a running OS call invalidates the generation and joins
disconnect cleanup; queued cancellation skips the OS call. The adapter registry
retains a closing slot until its worker joins, preventing a replacement physical
connection from overlapping cleanup. Old-generation delivery is discarded in Dart
and service caches start empty for each new generation.

On Linux, the manager, adapters and peripherals share one session task owner.
The pinned bluez-async constructor extension returns the unspawned D-Bus resource;
btleplug owns its actual Tokio handle. Bletide retains the manager before awaiting
adapter enumeration, including when enumeration fails or initialization is
cancelled. Shutdown aborts and joins the transport after connection workers join;
a cancelled shutdown retains the handle for retry. Last-owner Drop aborts as a
fallback. Explicit close preserves transport/task failures. Portable ownership
regressions and Linux compilation cover this implementation; healthy BlueZ and
independent D-Bus connection release still need Linux runtime verification.

BlueZ event streams retire local D-Bus callbacks synchronously when dropped.
Remote match removals are driven inside the owned transport future, with at most
64 queued and 16 running futures. Shutdown drops adapter events before closing
that transport, and joining transport cancellation drops queued/running removals.
Already recorded removal/overflow errors survive cancellation and reach close.
No separate match-removal task is spawned. This establishes library task ownership
through controlled tests, not independent D-Bus daemon resource counts.

The pinned btleplug Apple source includes an explicit adapter shutdown extension.
Bletide calls it after connection workers join. It signals an independent stop
channel for the CoreBluetooth thread, aborts and joins adapter/peripheral event
tasks, clears cached peripheral handles and joins the OS-object thread. The thread
uses a channel executor rather than another Tokio runtime. Dropping an unfinished
bootstrap owner also signals and joins its thread. Completed event-task handles
are reaped during registration. Once registration is closed, incoming futures
are dropped synchronously without spawning or retaining another task. Native delegates are detached and peripheral
waiters fail when the thread's CoreBluetooth state drops. Buffered commands that
have not reached a peripheral queue also receive a shutdown error; the command
receiver is closed first so no producer can add more commands during its drain. Controlled tests and 100 opt-in real adapter shutdown cycles prove the tested
ownership/join behavior. Shutdown closes the delegate receiver and drains the serial
native callback queue after detaching delegates. A separate opt-in regression
passes 100 callbacks blocked on a full event channel; removing the queue barrier
makes it fail. Loaded peripheral GATT teardown remains verification work.

The callback dispatch queue's raw creation reference has an RAII owner and is
released when CoreBluetooth state drops, as required by Apple's
[dispatch_release ownership rule](https://developer.apple.com/documentation/dispatch/dispatch_release).
Before releasing that reference, shutdown waits for a synchronous queue barrier.
The receiver is closed first so blocked callback sends cannot deadlock that barrier.

A GATT result reporting adapter unavailable, adapter disabled, permission denied,
or disconnected also retires its physical generation before further queued calls
can reach the OS. The failing request retains its original code; queued requests
fail as disconnected. This does not depend on a later adapter-state callback.
Ordinary GATT failures keep the queue usable.


Android adapter observation retains a loss revision and last unavailable state.
A brief loss followed by recovery cannot disappear into a latest-ready snapshot:
existing engines see loss first, then current recovery. Physical scanner leases
are invalidated before loss publication. A new lease recovers the OS scanner;
subsequent cleanup from a pre-loss engine cannot stop the new scan. Neither
connections nor scans restart automatically after adapter recovery.


Android's cached upstream peripheral is leased process-wide. A competing engine
receives `connectInProgress` while another engine owns that peripheral, including
its cleanup phase; it makes no OS disconnect call. Same-engine duplicate rules
remain described above. OS disconnect acknowledgement marks the lease clean;
release occurs when the generation worker drops its driver and notification
stream. Failed, cancelled, or panicked cleanup leaves a weak recovery record.
The next owner must acknowledge an OS disconnect before connecting. Records do
not retain engines or peripherals and are capped at 1024; capacity exhaustion
returns `invalidState` for a new device, while existing devices can still recover.


Android's bundled notification stream is explicitly closed on disconnect and
before its replacement is registered. Closure discards queued payloads and wakes
the pending JNI poller, releasing its native waker. One generation retains at
most 1024 queued notifications; overflow terminates the stream and therefore the
connection generation through joined Rust disconnect cleanup. A payload already
removed by poll remains owned by its poll result even if close races retrieval.
Remote loss closes its GATT object without depending on an active command.
Android disconnect cancels the owned retry, terminates active/queued Java
command futures and closes the local GATT client without waiting for an OS
callback. Command-future identity prevents late retries or completions from
advancing a newer command. A failed close retains the client for recovery and
reports an error. The acknowledgement concerns this client's disposal, not
whether another application's shared radio link remains connected. Actual
Android radio teardown and permission-loss recovery remain unverified.


Apple events carrying retrieval/clear replies own a terminal reply guard until the
adapter consumes them. Receiver loss or discarding a queued event completes the
reply with an error. A second guard covers cancellation of a pending channel send,
including a send that enqueued the event before its flush completed. Successful
send releases this second guard; successful consumption completes the event's
guard. The shared reply state accepts one completion, so late event consumption
cannot overwrite a cancellation error or resurrect an already-polled reply.


Android JNI notification errors and Apple/Windows broadcast notification overflow
emit one terminal error through the pinned upstream result-bearing stream. The
connection worker forwards the original cause as a notification error, closes its
generation, rejects queued operations and joins disconnect cleanup before releasing
resources. Later source values are never polled. A running GATT request receives
the original error; queued requests receive disconnected. Dart notification errors
include device/generation context and native message, and notification.failed
diagnostics retain the cause. The upstream value-only API remains available for
other consumers and terminates at an error rather than resuming after data loss.

The connection worker also polls notifications during a pending GATT operation.
A terminal stream interrupts that operation as disconnected, skips queued work
and joins cleanup. Existing subscribed values are delivered without waiting for
the GATT result. Values for a subscription being set up wait for its success
acknowledgement; failure discards them. This setup buffer holds at most 1024
values, and overflow retires the generation through the same joined cleanup.

The Dart native backend checks a connection event's generation before decoding
its body or error. Late malformed disconnect/notification frames from a retired
generation cannot close or report into a fresh connection. A truncated header,
or malformed body for a current generation, reports a typed internal error and
closes the backend. Logical disposal rejects new requests immediately; transport
close runs after leaving the event callback, so synchronous transports are safe.
Automatic shutdown owns its failed future, reports cleanup errors before streams
close and retains the failed close future for callers. Failed initialization also
starts owned cleanup while preserving the original ready error.

Android discovery cache retention belongs to initialized engines sharing the
process-wide adapter. Closing one engine preserves other engines' cached
peripherals. Closing the last engine detaches the cache; subsequent scan
callbacks cannot retain new entries until another engine initializes. Cache
values are destroyed outside the ownership lock. Connection workers and scanner
leases retire before normal engine cache release; abandoned initialization also
releases its cache owner. This adds no public cache-management API.
