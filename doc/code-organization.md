# Code organization and maintenance

Source review: 2026-10-05. This document describes boundaries and their costs,
not platform maturity. See [testing](testing.md) for verification commands and
[implementation status](implementation-status.md) for runtime evidence limits.
File sizes below are navigation aids, not acceptance limits.

## Boundaries implemented

Previously, `lib/src/ble.dart` contained 1,201 lines covering engine readiness,
scan leases, connection admission, operation deadlines, FIFO work and notification
owners. A scan change required navigating past connection-specific buffering and
teardown; a notification change required finding its helpers among engine code.
The same Dart library now has three source locations:

- [ble.dart](../lib/src/ble.dart): engine readiness, scans, connection admission,
  diagnostics and engine close (415 lines).
- [ble/connection.dart](../lib/src/ble/connection.dart): a connection generation,
  FIFO work, notification owners and disconnect (667 lines).
- [ble/operation.dart](../lib/src/ble/operation.dart): cancellation, terminal
  result arbitration, timers and error-context enrichment (126 lines).

These are `part` files. They preserve private constructors, engine invalidation
and the existing public import/export surface. They improve navigation but do
not provide independent privacy between parts. Turning each part into a separate
library would require exposing engine internals or adding coordination callbacks.
That extra boundary has no demonstrated testing benefit here: public contracts
already inject the backend and exercise both engine and connection retirement.

Previously, Rust's adapter, connection and engine files mixed production logic
with large controlled regression suites. `scan.rs` was 2,223 lines, `connection.rs`
2,031 and `engine.rs` 1,047. The worker loops were difficult to locate, and the
connection's production notification-policy decoder even followed its main test
module. The layout now separates:

- [scan.rs](../rust/src/scan.rs) (383 lines) and
  [connection.rs](../rust/src/connection.rs) (364 lines): driver contracts,
  lifecycle workers, cancellation and cleanup barriers.
- [scan/native.rs](../rust/src/scan/native.rs) and
  [connection/native.rs](../rust/src/connection/native.rs) (349 and 347 lines):
  btleplug implementations, OS-specific behavior, attribute/event translation.
- [engine.rs](../rust/src/engine.rs) (627 lines): request registry, runtime
  supervisor and engine ownership.
- Child regression modules: [scan/tests.rs](../rust/src/scan/tests.rs)
  (1,498 lines), [connection/tests.rs](../rust/src/connection/tests.rs)
  (1,352), [engine/tests.rs](../rust/src/engine/tests.rs) (427).

The native drivers already implemented the worker traits. The extraction uses
that existing seam; it adds no new dispatch layer. A platform attribute change
can now be reviewed separately from the cancellation loop. The service encoder
remains available through `connection::encode_services` for the compiled native
fixture. Driver state, futures, scopes and Drop order stay the same. Regression
modules retain private access and assertion names; the moved binary-fixture path
still resolves to the shared Dart/Rust fixture. Small policy tests remain inline.

Rustfmt passes some densely packed `tokio::select!` bodies unchanged. The GATT
payload guard, setup identity decoding, notification error handling and engine
command snapshot were expanded manually. The first-party Android bootstrap now
puts callbacks and cleanup steps on separate lines. Unrelated vendored formatting
is deliberately outside these changes.

## Source layout and ownership review

Each row identifies state, independently changing concerns, the maintenance
tradeoff and the decision. “Retain” means there is a concrete reason to avoid a
new boundary, rather than an assertion that a file is cohesive enough.

### Dart API and coordination

| Source | Responsibilities and mutable state | Independent changes; coupling, tests and decision |
|---|---|---|
| [ble.dart](../lib/src/ble.dart) | Initialization result, pending connects, active connections, scan controllers/filters, physical scan flag/transition, chooser, diagnostics, disposed/closing state and backend stream subscriptions. | Scan reconciliation and connect admission can evolve independently of GATT buffering. **Split implemented** above. Keep readiness and close with admission: an unresolved `ready` must not retain cancelled attempts; adapter loss invalidates existing generations; close owns backend cleanup. `backend_contract_test.dart`, `chooser_test.dart` and `capabilities_test.dart` exercise these interactions. |
| [ble/connection.dart](../lib/src/ble/connection.dart) | FIFO queue/running work, drain flag, connection state, backend listeners, disconnect future; notification entries own desired/enabled status, owner count, transition/setup result and stream; individual owners hold bounded buffers and cancellation futures. | Ordinary GATT forwarding changes independently of notification ownership. **Retain after extraction**: notification enable/disable itself occupies the same FIFO, rediscovery must reject retained owners, invalidation aborts queued/running work and detaches the generation before awaiting cleanup. Moving notification state into a new manager would need callbacks for all three and obscure the teardown barrier. The shared GATT suite and readiness/overflow/replacement tests cover these contracts. Reconsider only if a separately testable notification policy emerges. |
| [ble/operation.dart](../lib/src/ble/operation.dart) | `BleCancellation` listener set/cancel flag; `_Work` backend request, timer, cancellation-removal callback and terminal completer. Builds contextual errors and disposes late results through the supplied callback. | Deadline arbitration changes independently of scanning and GATT. **Extracted**, retaining one implementation for both. Do not create per-operation wrappers: queue time counts toward the deadline, pre-cancelled work never dispatches, late connects must be physically discarded, and result completion is single-shot. Existing queue/readiness/cancellation tests exercise the actual coordinator. |
| [models.dart](../lib/src/models.dart) (214 lines) | UUID normalization, identities, capabilities, deadlines, filters, immutable advertisement/attribute snapshots and diagnostic values. No library-owned mutable lifecycle state. | New model fields and filtering policy can evolve separately. **Retain**: these types describe the small public vocabulary and are commonly navigated together. A file per model would multiply imports without reducing ownership coupling. `models_test.dart` verifies normalization/filtering and byte/list snapshots. |
| [errors.dart](../lib/src/errors.dart) | Portable error enum, immutable context and exception. No mutable state. | Error vocabulary changes separately from model fields. **Retain existing boundary**. Numeric wire classifications must still match Rust/native mappings; changing language enums independently needs wire tests. |
| [backend.dart](../lib/src/backend.dart) | Backend/connection contracts, cancellable requests, copied notification envelope and optional notification compatibility extension. No lifecycle owner. | Optional platform features change without forcing every backend to implement them. **Retain**: one short contract lets reviewers compare native/web/fake behavior. Separate request/notification files would add little. The backend interfaces are exported by the public library for injection; they cannot simply be relocated into an inaccessible internal API. |
| [fake_backend.dart](../lib/src/fake_backend.dart) (346 lines) | Controlled operation completers, pending/history registries, fake generation counter, scan/close state, per-connection subscription keys and streams. | Fixture convenience methods can evolve separately from production behavior. **Retain**, exposed through `lib/testing.dart`; moving the implementation out of `lib` would break consumer testing imports. Explicitly controlled operation completion is essential to cancellation/race tests; do not replace it with implicit successful futures. |
| [backend_factory.dart](../lib/src/backend_factory.dart), [bletide.dart](../lib/bletide.dart), [testing.dart](../lib/testing.dart) | Conditional backend selection and export surfaces. No mutable state. | **Retain** small entry points. Conditional imports enforce the web/FFI boundary at compilation; `src` alone is a convention, not Dart access control. Public compatibility is defined by the exports, not by moving files into additional folders. |

### Native Dart and browser integration

| Source | Responsibilities and mutable state | Independent changes; coupling, tests and decision |
|---|---|---|
| [native/event_bridge.dart](../lib/src/native/event_bridge.dart) (313 lines) | ReceivePort/subscription, engine handle, pending request completers, close request/result and terminal flag; synchronous input allocation/copy/free and terminal event correlation. | Buffer allocation and event classification can change independently. **Retain**: both rely on the same pending registry and terminal-port lifetime. Splitting would need a second owner to coordinate malformed-event shutdown and correlated close ACK. `native_abi_test.dart` executes the compiled ABI, including late events, bounds and repeated close; transport fakes supplement this evidence. |
| [native/native_backend.dart](../lib/src/native/native_backend.dart) (300 lines) | Initialization, capabilities/adapter state, advertisements, connection-generation registry, transport listener, close/error retirement. | Advertisement decoding is independent and already in `wire.dart`. **Retain** routing with generation admission: stale events cannot close a replacement connection and unsolicited native shutdown must release all logical owners. `native_backend_test.dart` covers malformed/stale/error/cleanup paths. |
| [native/native_connection.dart](../lib/src/native/native_connection.dart) (320 lines) | Generation-scoped command encoding, contextual errors, disconnect/notification streams and closed flag. | Adding an operation or changing error context is independent of engine routing. **Retain existing split**: methods are uniform wrappers around one transport and a generation. Further extraction would hide operation numbers without changing state ownership. Native wire and ABI contracts cover both copied payloads and error context. |
| [native/wire.dart](../lib/src/native/wire.dart) (201 lines) | Bounded reader offset, writer byte builder, advertisement/service/error decoding and UUID encoding. Temporary codec state only. | Service and advertisement formats can evolve separately, but share primitives and fixtures. **Retain**: splitting every codec would make bounds enforcement harder to review as a whole. Tests reject each truncated service payload, invalid counts/flags and malformed native error metadata. |
| [native/bindings.dart](../lib/src/native/bindings.dart), [C header](../src/bletide.h) | Five versioned native declarations and native asset identity. No mutable state. | **Retain** the existing asset path; moving it requires hook and consumer-manifest changes for no navigation gain. C/Dart declarations are intentionally duplicated across the ABI and checked by compiled ABI/layout tests. |
| [web/web_backend.dart](../lib/src/web/web_backend.dart) (561 lines) | Availability listener/timer/revision and readiness, selected-device cache, chooser reservation/completer, active/pending/retrying connections, process-wide leases/retiring streams and generation counter, closed flag and retained cleanup failure. | Chooser option construction can change independently of connect/retry. **Retain for now**: selected devices feed admission, availability aborts both pending and active work, and non-abortable browser promises retain process reservations until settlement. A chooser manager would need to share device capacity and close state; a lease manager would need callbacks for late connect cleanup. Chrome tests cover the shared-browser races. Documented complexity remains in `_retryConnect` and `connect`; splitting lexical locations alone would not make those races independent. |
| [web/web_connection.dart](../lib/src/web/web_connection.dart) (545 lines) | Browser error classification, physical-generation FIFO, active/pending/queued jobs, attribute caches, JS listeners, closed/cleanup state and disconnect delivery. | Stateless error conversion can change independently, but is already one function shared by backend and connection. **Retain**: another small file is optional navigation preference. Do not abstract browser request retirement into `_Work`: a late JS promise can reconnect/change an OS object, so it must hold its physical reservation after logical timeout. Existing Chrome discovery/late-write/notification/disconnect tests exercise real JS interop with controlled promises. |
| [web/bluetooth.dart](../lib/src/web/bluetooth.dart) | Minimal JS IDL extension types. No Dart-owned mutable state; underlying browser objects are foreign state. | Browser API declarations evolve independently of coordination. **Retain existing boundary**, and keep byte copies in the connection adapter. Extending the IDL needs Chrome execution, not just analyzer success. |

### Rust workers, ABI and platform integration

| Source | Responsibilities and mutable state | Independent changes; coupling, tests and decision |
|---|---|---|
| [engine.rs](../rust/src/engine.rs) | Global ID/host initialization; runtime and supervisor handles; engine/request registries; atomic request state/close ID; command channels, stop signals, resource counters and sticky cleanup error; owned JoinSets and liveness timer. | Request admission and supervisor scheduling can evolve independently. **Extract tests; retain production**: retirement removes the registry entry only after joined worker cleanup, then sends terminal ACK. Separating registries from supervisor would create a cross-module publication protocol without independent ownership. Tests cover close after retirement, pending queues, fatal worker/payload failures and repeated cycles. |
| [scan.rs](../rust/src/scan.rs) | Driver trait, adapter worker's event stream, state/scanning/uncertain flags, generation-slot registry, connection JoinSet, admission/retry routing, cleanup and pending disconnect ACKs. | OS bootstrap/event translation changes independently of connection routing. **Split native implementation and tests**. Retain retry waits in the adapter owner: it must join the old worker before reusing the physical device. Driver injection already gives portable tests without exposing production state. |
| [scan/native.rs](../rust/src/scan/native.rs) | Manager/adapter options, Android scanner token/generation, OS initialization/start/stop/shutdown/peripheral lookup, error mapping and advertisement encoding. | Platform integration and advertisement fields can evolve separately. **Retain within extracted driver**: encoding is stateless but only translates these upstream events, with direct fixture tests. Retain manager ownership before any await that can fail; Android lease Drop fallback and explicit shutdown remain paired. A separate platform tree would duplicate the tree already owned by btleplug. |
| [connection.rs](../rust/src/connection.rs) | Driver/Device contracts, slot command/stop/state channels, worker's notification stream, subscription map/resource guards, queued commands, setup backlog and interruption/cleanup state. | Attribute lookup and link hints change independently of worker arbitration. **Split native implementation and tests**. Keep initialization/GATT polling and future disposal under the same guarded worker; moving cleanup to detached tasks would change shutdown evidence and Drop ordering. Tests cover FIFO errors, pending setup notifications, rediscovery, disposal panics and original-cause preservation. |
| [connection/native.rs](../rust/src/connection/native.rs) | Peripheral, discovered service cache and Android lease; service/characteristic/descriptor identity checks, GATT dispatch, property checks, link hints and service encoder. | New platform operations need not change the worker loop. **Extracted** behind existing trait. Cache replacement is now rejected while subscriptions are live by the worker; ambiguous UUIDs fail explicitly. Keep decoder/property/attribute selection adjacent so reviewers can verify which bytes select which foreign object. |
| [shared_scan.rs](../rust/src/shared_scan.rs) (448 lines) | Android process scanner's weak owner tokens, physical state, revision, serialized transition lock and watch state. | Error classification can evolve separately from ownership, but generation admission and stop/start arbitration cannot. **Retain**: splitting states and transitions would obscure the lock scopes preventing old-owner stop from terminating a fresh scan. Approximately half the file is focused ownership tests; unlike the former worker files, production is readily navigable. |
| [peripheral_lease.rs](../rust/src/peripheral_lease.rs) (148 lines) | Bounded weak-token registry, lease clean/recover flags, tombstones and identity-checked release. | Capacity policy is independently changeable. **Retain**: tombstones are intentional retry safety, not a cache to clear on timeout. Its four tests cover conflict, 100 recovery cycles, cleanup uncertainty and capacity recovery. |
| [adapter_state.rs](../rust/src/adapter_state.rs) (123 lines) | Android state/revision watch channel and invalidation publication. | Notification representation changes separately from scanner policy. **Retain existing boundary**: scanner invalidation must precede publishing the new adapter state. Tests check that order. |
| [android.rs](../rust/src/android.rs) (63 lines) | JNI initialization atomic, state singleton, class-loader/bootstrap and adapter-state exports. | **Retain** narrow bootstrap; BLE operations remain FFI commands. Foreign exceptions use the existing guarded policy. Host JNI tests exercise bundled utility classes/policy; Android build/runtime remains a separate gate. |
| [codec.rs](../rust/src/codec.rs) (153 lines) | Portable/native error representation, bounded temporary reader offset and event encoding. No asynchronous owner. | Wire shape and error classification can evolve separately. **Retain** short shared codec, alongside mirrored Dart decoding and shared binary fixtures. Do not generate foreign error policy from message text. |
| [ffi.rs](../rust/src/ffi.rs) (146 lines) | C exports, panic guard, synchronous payload validation/copy and optional test/debug exports. No additional registry. | ABI changes are separate from worker internals. **Retain** the small unsafe boundary; checks precede slice construction, and inputs are copied before return. Compiled Dart ABI tests and malformed-pointer/bounds Rust tests remain necessary. |
| [event.rs](../rust/src/event.rs) (107 lines) | CObject layout, immutable port/function identity and synchronous copied VM event sink. No event pointer lifetime beyond the call. | **Retain separate transport**. Layout tests and actual VM-port contracts check this foreign boundary; a mock EventSink cannot replace them. |
| [resources.rs](../rust/src/resources.rs) (65 lines) | Conditional global/per-engine atomic counters and RAII decrement guards. | **Retain** diagnostics separate from ownership. Counters retain only storage, and release builds omit internal exports. Counter balance does not prove OS resource release. |
| [fixture.rs](../rust/src/fixture.rs) (318 lines) | Feature-gated native adapter/device/driver fixture; controlled event/notification channels, connect/discovery barriers, atomic connected state, copied attribute values and discovered flag. | Fixture controls can evolve independently of public ABI. **Retain** in the crate under `test-support`: native Dart contracts must exercise actual worker/ABI/VM-port transport. Moving it into a separate test crate would bypass the production feature/build-hook path. Release bundle verifiers reject fixture exports. |
| [lib.rs](../rust/src/lib.rs), [build.rs](../rust/build.rs) | Module/feature selection and Android build/link setup. No new asynchronous owner. | **Retain** simple entry points. Shared callback-boundary code is intentionally included from inspected vendor sources, avoiding a second panic-disposal implementation. This unusual path is explicit and provenance-checked. |
| [BletidePlugin.java](../android/src/main/java/dev/bletide/BletidePlugin.java) | Engine Context, activity binding and adapter BroadcastReceiver; JNI bootstrap, permission-driven state updates and listener registration/cleanup. | Permission checks and activity attachment can change separately. **Simplify formatting; retain** the small bootstrap. Extracting a permission manager would not create independently owned resources. Numeric adapter values intentionally mirror native state. JVM tests/builds supplement, but do not prove live Flutter engine attachment or radio behavior. |

Android's `com/nonpolynomial` and `io/github/gedgygedgy` namespaces are vendored,
with explicit local patches; they are not first-party organizational targets.
Their regression tests are first-party and stay in matching Java package paths
to reach package-private callback state. The historical fixture JNI namespace is
also intentional: changing a native symbol for cosmetic consistency adds risk.

### Tests, example, build hooks and tooling

| Source | Responsibilities and mutable state | Independent changes; coupling, tests and decision |
|---|---|---|
| [backend_contract_test.dart](../test/backend_contract_test.dart) (953 lines) | Fake scan/connect/GATT lifecycle assertions; per-test controlled operations/listeners/timers. | **Retain** suites together with the common fake harness: splitting by operation would duplicate readiness and teardown setup. Each test owns its backend; no global production owner. Shared GATT assertions already live in support. |
| [capabilities_test.dart](../test/capabilities_test.dart) (748 lines) | Capability rejection, counted readiness observation and notification setup/buffering/replacement races; per-test fixture state. | **Retain**: test names document the independent contracts and all use the same controlled physical connection helper. If readiness contracts grow separately, that suite is a viable future boundary, not a current correctness defect. |
| [native_backend_test.dart](../test/native_backend_test.dart) (1,023 lines) | Transport request/event fixture, routing/context, malformed payloads, generation safety and notifications. | **Retain**: the many malformed/stale cases depend on one transport fixture. A file split should extract a reusable fixture only when another suite needs it; promoting it now increases test API surface. These transport tests supplement compiled ABI tests. |
| [native_abi_test.dart](../test/native_abi_test.dart) (830 lines) | Compiled symbol lookup, real FFI/ReceivePort contracts, counts, generations, cancellation and teardown. State is per-test handles/ports, with actual native global registries. | **Retain** this distinct evidence boundary. Splitting into independently scheduled files could introduce concurrent tests against process counters unless fixtures/serialization are redesigned. No test names or resource assertions were removed. |
| [web_gatt_test.dart](../test/web_gatt_test.dart) (987 lines), [web_chooser_test.dart](../test/web_chooser_test.dart) (193) | Actual JS interop objects with controlled promises/listeners; browser-generation GATT/late results versus gesture/chooser contracts. | **Retain existing split**: chooser and GATT fixtures differ. The GATT fixture owns listener registries and promise barriers; a second file currently needs no reusable fixture. Both run on Chrome, separately from VM tests. |
| [support/gatt_contract.dart](../test/support/gatt_contract.dart) (372 lines) | Reusable assertions for discovered identity, copies, writes, context, notification sharing and disconnect. State is fixture/listener ownership per contract. | **Retain shared assertions**; this is useful consolidation rather than platform simulation. Each backend supplies a different fixture, so actual ABI/JS calls still execute. |
| [support/native_fixture_contracts.dart](../test/support/native_fixture_contracts.dart) (615 lines), [support/fake_gatt_fixture.dart](../test/support/fake_gatt_fixture.dart) (84) | Native counter/control/barrier cases versus fake attribute operation fixture. | **Retain distinct adapters**: merging them would obscure which calls execute native code. Native helper assertions belong beside compiled fixtures; fake values and pending operations belong beside fake fixture construction. |
| [native_wire_test.dart](../test/native_wire_test.dart), [models_test.dart](../test/models_test.dart), [chooser_test.dart](../test/chooser_test.dart) | Focused codec/model/chooser regressions; local byte buffers and fake completers. | **Retain** existing focused files, with no new helper abstraction needed. |
| [hardware_config_test.dart](../test/hardware_config_test.dart) (98), [hardware_scenario_test.dart](../test/hardware_scenario_test.dart) (307) | Configuration validation/privacy and harness cleanup; local fake fixtures/recorded report events. | **Retain** config/scenario boundary. Tests verify harness behavior, not actual hardware execution. |
| [Rust child tests](../rust/src/scan/tests.rs) | Controlled channel drivers, sinks, panic/drop injections, watch states and harness handles for each worker. | **Moved** for production navigation, retaining private child-module access. The 1,498/1,352-line suites remain large because their timing fixtures are shared; splitting each race would duplicate the controlled state machine. Small tests for ABI/codec/leases remain inline. |
| [example/lib/main.dart](../example/lib/main.dart) (586 lines) | Flutter entry point/testbench, widget engine, text controllers, scan/adapter/connection/diagnostic subscriptions, device/service snapshots, setup-key set, cancellation token, UI readiness/close/dispose flags and bounded result log. | Layout changes independently of lifecycle actions. **Retain**: the widget tests already inject an engine and exercise late setup/disposal. A new application controller would duplicate library ownership and require a disposal API. Stateless service-row extraction is optional if UI growth makes widget nesting harder; no generic app architecture is warranted. |
| [example/lib/recipes.dart](../example/lib/recipes.dart) (95), [example/test/recipes_test.dart](../example/test/recipes_test.dart) (102) | Task-oriented discover/select/read/write/notify/reconnect examples and their controlled fixture operations. Owners are scoped local connections/subscriptions. | **Keep separate executable recipes** so tutorial policy can change without modifying the explorer. Tests verify resource ownership and both write modes. Recipes do not add automatic protocol retries/chunking. |
| [example/test/testbench_test.dart](../example/test/testbench_test.dart) (752) | Widget controls, stale generation UI, notification ACK/teardown, connect cancellation and disposal; test-local fake physical operations. | **Retain** one injectable testbench fixture, with independent test cases. Splitting lifecycle from layout assertions would duplicate widget initialization without changing ownership. |
| [PeripheralGenerationTest.java](../android/src/test/java/com/nonpolynomial/btleplug/android/impl/PeripheralGenerationTest.java) (217) | Callback/generation regression fixtures, old/current GATT objects, completion counters, latches and test-local executor for connection-state publication. | **Retain** together: callback identity and monitored publication protect the same generation. Package placement permits inspected internal access without adding public Java test APIs. The new monitor-blocking assertion checks real bundled Java synchronization, not radio behavior. |
| [PeripheralIdentityTest.java](../android/src/test/java/com/nonpolynomial/btleplug/android/impl/PeripheralIdentityTest.java) (232) | Mock GATT service/characteristic/descriptor trees, callback values and notification state; scoped identity, duplicate UUID, API33 copies and compatibility assertions. | **Retain** the separate attribute suite. Shared service-building helpers keep repeated UUID setup explicit. Compact Java test formatting is an optional style improvement; splitting each case would duplicate reflection/setup. |
| [PeripheralCancellationTest.java](../android/src/test/java/com/nonpolynomial/btleplug/android/impl/PeripheralCancellationTest.java) (130) | Pending Java futures, queue operations, close/retry GATT objects and counted wakers. | **Retain**: cancellation/cleanup tests depend on the same controlled pending queue. Small reflection helpers are repeated intentionally between these independently runnable Java suites rather than promoted into a generic private-field test framework. |
| [AdapterScanFailureTest.java](../android/src/test/java/com/nonpolynomial/btleplug/android/impl/AdapterScanFailureTest.java) (57), [QueueStreamLifetimeTest.java](../android/src/test/java/io/github/gedgygedgy/rust/stream/QueueStreamLifetimeTest.java) (76) | Per-attempt callback/generation counters versus queue/poll/waker lifetime fixtures. | **Retain separate suites**: scanner callback admission and utility queue retirement own different state. Matching package paths expose the appropriate internal source boundaries; both execute actual bundled classes with controlled OS inputs. |
| [Example MainActivity](../example/android/app/src/main/kotlin/com/example/bletide/MainActivity.kt) (20) | Platform-version permission selection and a local missing-permission list. No retained BLE owner. | **Retain** the thin example activity. Permission prompting belongs to the consumer; moving it into the package bootstrap would change the documented ownership contract. Android release compilation checks this integration, while actual permission/radio execution remains pending. |
| [integration_test/hardware_config.dart](../integration_test/hardware_config.dart) (132), [hardware_scenario.dart](../integration_test/hardware_scenario.dart) (188), [native_adapter_probe.dart](../integration_test/native_adapter_probe.dart) (95) | Immutable validated private settings; scenario with local owned connections/subscriptions/reports; actual native loading and adapter-error probe with handles/ports. | **Retain separate boundaries**: configuration never starts BLE, scenario orchestrates safe configured work, native probe verifies actual load/error cleanup. Thin example integration/driver files launch these and remain in Flutter's required directories. |
| [hook/build.dart](../hook/build.dart) (70) | Build-hook dependency inventory, test feature selection, native asset identity and Android API24 compiler environment. Temporary output/config state only. | **Retain**: Android compiler selection is a short pure helper adjacent to its only caller. Changes to ABI identity must update bindings and bundle expectations; dependency inclusion must continue to cover vendor sources omitted by Cargo dep-info. New first-party Rust modules are discovered by Cargo. |
| [tool/dbus_async_setup_probe.py](../tool/dbus_async_setup_probe.py) (167), [fixtures/dbus_async_setup.rs](../tool/fixtures/dbus_async_setup.rs) (480) | Isolated checksum-verified temporary Cargo project/alternate-driver reconstruction/report; private Unix-peer/socket/watch/JoinHandle test state and actual client setup. | **Retain probe/fixture split**. Alternate-driver options change independently of production setup; their temporary source copies must never alter vendor files. The fixture's peer tasks are joined/aborted explicitly. Python manifest construction is dense but local; introducing a build framework would add more maintenance than it removes. |
| [tool/patch_replay.py](../tool/patch_replay.py) (57), [test_patch_replay.py](../tool/test_patch_replay.py) | Shared offline inventory/patch execution and negative controls. Temporary directories only. | **Consolidated** Rust reverse/forward replay and Android ordered replay here. Source hashes alone could authenticate internally inconsistent patches; replay verifies they restore/reproduce the intended bytes. Tests deliberately use mismatched origins/additions. |
| [upstream_sources.py](../tool/upstream_sources.py), [android_sources.py](../tool/android_sources.py) | Complete pinned inventories, patch hashes and ordered/reversible provenance. Temporary inventory maps only. | **Retain language-specific manifest policy** with shared replay mechanism. Android order matters; Rust originals/additions have a different inventory format. A generic provenance schema would hide those differences. |
| [repository_check.py](../tool/repository_check.py) (95), [documentation_check.py](../tool/documentation_check.py) (26) | Publication file-set/privacy/link checks; temporary README Dart files analyzed against resolved API. | **Retain** two independent checks: hygiene works before dependency resolution, documentation analysis requires the package config. Report matched rule/path rather than credential values; generated snippets remain in ignored storage. |
| [android_apks.py](../tool/android_apks.py) (95), [apple_bundle.py](../tool/apple_bundle.py) (98), [linux_bundle.py](../tool/linux_bundle.py) (56), [windows_bundle.py](../tool/windows_bundle.py) (121) | APK/ELF, Mach-O, ELF bundle and PE parsers plus architecture/export/mapping/license assertions. Temporary byte buffers/files only. | **Retain platform entry points** used by CI. Repeated five-export names and notice comparisons are small, intentional cross-format assertions. A shared bundle framework would obscure different APK alignment, Apple deployment and PE RVA rules. If the ABI grows, derive symbol expectations from the header rather than add another handwritten generic schema. |
| [event_layout.py](../tool/event_layout.py) (128), [native_licenses.py](../tool/native_licenses.py) (112), [jni_host_tests.py](../tool/jni_host_tests.py) (25), [fixtures/jni](../tool/fixtures/jni) | Temporary compiled C/Rust layout probes; locked graph/license inventories; bundled Java compilation/JAR and actual upstream JNI execution. | **Retain separate evidence tools**: layout, redistribution and Java class-loader behavior fail for different reasons and have different prerequisites. No long-lived production state. Temporary output paths must remain outside the public source inventory. |
| [CI](../.github/workflows/ci.yml), [Android build](../android/build.gradle), example platform/build files | Target/toolchain policy, conditional validation and generated-consumer templates; no BLE lifecycle owner. | **Retain native platform directories** expected by Flutter. CI run blocks now explicitly use fail-fast Bash on every OS; a failure probe verifies a later successful command cannot mask a native failure. First-party organization does not justify reformatting generated platform templates wholesale. |

## Folder boundaries and duplication

`lib/bletide.dart` is the consumer entry point; `lib/testing.dart` adds controlled
test fixtures. `lib/src/ble` holds logical coordination, `native` holds ABI/event
transport and `web` holds JS integration. Models/errors/backend contracts remain
flat because there are few of them and all three backends share them. Conditional
imports provide the important platform separation. Dart parts preserve private
library access, while Rust child modules restrict native helper visibility to
their parent (and its tests). Neither language gains independent ownership from
a folder name alone.

Rust's `ffi`, `event` and `codec` are already separate from engine/worker state.
Android Java bootstrap and Rust JNI stay in their platform integration paths.
Build hooks and standalone inspection commands stay outside runtime source.
Tests/support contain shared contract fixtures; hardware settings are validated
by integration helpers and kept private. Flutter platform folders and Gradle
wrapper locations are required consumer structure. Documentation belongs in
`doc`, fixtures under their owning test/probe suite, and pinned upstream crates,
patches and metadata under `rust/vendor` and Android provenance files.

The Dart FIFO owns logical deadlines/results for native and browser backends.
The Rust FIFO owns physical requests, including direct FFI callers. The browser
FIFO protects direct backend calls and late non-abortable promises. Consolidating
these queues would remove a different safety layer on each platform. Wire enums,
UUID encoding, adapter state and ABI declarations are intentionally duplicated
across foreign boundaries and checked through fixtures/native compilation.
Byte snapshots at caller, encoder and FFI entry protect different lifetimes.

## Prioritized remaining findings

| Priority/type | Evidence and impact | Follow-up |
|---|---|---|
| P1 verification gap | [Implementation status](implementation-status.md) records no completed peripheral/platform hardware suite. Compiled counters, fake/JS fixtures and APK/framework inspection cannot establish independent OS retention or callback order. | Execute the configured hardware suite on each claimed platform; include healthy BlueZ, loaded Windows/WinRT failures, current Android API/ABI runtime and signed iOS execution. Keep experimental wording until evidence exists. |
| P2 maintainability | [scan.rs](../rust/src/scan.rs) `worker` still coordinates routing, retired-device waits, event handling and teardown in one select loop; [connection.rs](../rust/src/connection.rs) `worker` nests setup-notification arbitration inside GATT polling. Rustfmt does not expand all macro tokens. | Some dense guards were expanded. Further edits should isolate a demonstrated independently testable routing/encoding policy, preserving biased-select order, guarded future destruction and worker joins. A generic async state-machine framework is not justified. |
| P2 maintainability | [web_backend.dart](../lib/src/web/web_backend.dart) has instance maps plus static lease/retirement maps. `release`, `abort`, `failed` and late-promise success share captured state. `_retryConnect` is coupled to terminal retirement delivery. | Keep explicit identity checks and Chrome late-result tests. If browser transport grows, extract physical lease ownership only with a testable terminal-retirement contract; do not release leases on logical cancellation. |
| P2 maintenance/process | [Repository check](../tool/repository_check.py) searches selected credential patterns and public files; it is not exhaustive secret/history scanning. Upstream replay verifies consistency with pinned manifests, not independent authenticity. | Owner must review history/settings and independently authenticate upstream when changing pins. Preserve patches, licenses and manifest checks. |
| P3 stylistic preference | [Example main](../example/lib/main.dart) has a large widget tree; native bundle scripts repeat short symbol/notice sets. Existing tests and format checks pass. | Optional stateless widget helpers or header-derived symbols if future changes demonstrate churn. No current behavior defect or need for more dependencies. |

The continuation fixed confirmed defects separately from these concerns: public
GATT capability checks now reject unsupported attribute work before dispatch;
rediscovery rejects live notification owners in Dart and the actual Rust worker;
the vendored Java connection getter now acquires the callback-state monitor and has a publication regression;
provenance replay and explicit CI shell failure handling strengthen existing
checks. These changes have direct source/assertion evidence; they are not
justified by file length or a preferred architectural style.

## Inspected upstream patterns

See [library comparison](library-comparison.md) for source links and decisions.
The applied pattern is a small public facade with visible ownership units and
platform implementations behind existing contracts. Bletide does not need a
federated package hierarchy, global singleton facade, device-object reconnection
model or runtime scheduler architecture merely because another library uses one.
