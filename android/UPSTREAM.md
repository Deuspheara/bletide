Java sources under com/nonpolynomial and io/github/gedgygedgy originate from
btleplug 0.13.3, src/droidplug/java/src/main/java. Cargo.lock pins the matching
Rust code. UPSTREAM-SOURCES.sha256 records the original published source bytes.

Bundled sources have ten reviewed local compatibility patches:
patches/stale-gatt.patch rejects stale BluetoothGatt identities in all GATT
callbacks, extending upstream's existing connection-state identity check.
Notification payloads are copied only after checking the active GATT identity.
patches/notification-lifetime.patch closes and clears the previous stream when
opening the next generation and on disconnect, releases the GATT object on
remote loss, and limits notification backlogs to 1024 items. Overflow terminates
the stream, causing the Rust generation worker to disconnect. QueueStream adds
explicit close and bounded construction; its original constructor and JNI
interface remain compatible. Items are removed under the queue lock when polled,
so close cannot invalidate a payload already handed to a poll result.
patches/cancellation-lifetime.patch makes disconnect terminate active/queued
commands and cancel the owned retry before disconnecting and closing the GATT
client. Cancelled retry tasks are removed from the process scheduler. Command
future identity guards reject late retries/completions, and SimpleFuture retains
its first terminal result while releasing its pending native waker. Disconnect
acknowledges local GATT-client closure; it does not assert the shared radio link
is unused by other applications. Close failures remain errors and retain the
client for recovery. This follows Android's BluetoothGatt disconnect/close APIs:
https://developer.android.com/reference/android/bluetooth/BluetoothGatt#close()
patches/attribute-identity.patch rejects duplicate characteristic and descriptor
UUID matches rather than selecting the first live GATT object.
patches/service-identity.patch extends the Rust/Java JNI signatures to include
service UUIDs for characteristic read/write, notification setup/teardown and
descriptor read/write. Lookup rejects duplicate services or attributes inside the
requested service, while the same characteristic UUID in different services is
addressable. Reply callbacks check service identity as well as attribute UUIDs.
Notification snapshots copy the payload and attach an independent service identity;
Rust reads this identity rather than searching cached services by characteristic UUID.
The corresponding Rust patch is in rust/vendor/btleplug.patch. These signatures
must ship with the bundled Java sources; the published unpatched Java API differs.
patches/callback-failure.patch contains exceptions from remote-loss GATT close
and adapter event dispatch on the Binder callback thread. A failed remote close
logs its error, retains the GATT client for explicit disconnect/recovery, and
still publishes the disconnect event after closing the notification stream.
Adapter event exceptions are logged through the existing callback dispatch
boundary. Neither failure is treated as successful delivery or successful close.
patches/api33-values.patch implements the API 33 characteristic read, descriptor
read and notification overloads using their supplied event values. Read results
and notifications copy the value before handing it to an asynchronous consumer;
they do not read the mutable GATT attribute value. Both legacy and modern callbacks
reject stale GATT identities before touching attributes or routing command replies.
The legacy callbacks remain available on API 24–32. The newer value contract is
documented at https://developer.android.com/reference/android/bluetooth/BluetoothGattCallback.
patches/notification-overflow-cause.patch emits one FutureException containing
the queue overflow cause before ending the stream. Overflow discards queued
payloads, releases its pending waker and retains the cause across close until
the error is polled. Rust's JSendStream captures and clears Java exceptions
inside the JNI environment, preserving their text in its terminal error item.
The matching Rust source change is in rust/vendor/btleplug.patch.
These preserve the upstream BLE implementation. The patches and
resulting source hashes are recorded in LOCAL-PATCHES.sha256; all other bundled
upstream files remain unchanged. Run `python3 tool/android_sources.py` from the
repository root to verify the complete inventory.

See THIRD_PARTY_NOTICES and LICENSE.btleplug.md for upstream license terms.
BletidePlugin.java is original initialization/adapter-state glue; no BLE
operation uses a Flutter MethodChannel. JVM tests use JUnit/Mockito only as
development dependencies; neither is packaged in consumer applications.

patches/notification-compatibility.patch adds an explicit per-request boolean
through the service-aware JNI signature. Standard setup writes the CCCD and
awaits its callback. Opted-in notify setup/teardown enables local routing and
skips CCCD writes, awaiting local acceptance; it preserves local routing errors
and rejects non-notify characteristics. No device UUID selects this behavior.

patches/scan-failure.patch gives each physical scan attempt its own callback,
reports onScanFailed with its generation/native code, caches synchronous failure
for startup observers and rejects advertisements/failures from retired attempts.
Stop retires the callback only after successful OS cleanup. Rust consumes these
errors without poisoning the process-wide adapter; duplicate engine observers do
not invalidate a new physical attempt.
