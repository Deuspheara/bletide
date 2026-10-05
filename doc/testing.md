# Testing

Deterministic tests run without a BLE peripheral. Native tests require the host
build toolchain; JNI tests additionally require JDK 17. Rustup selects the pinned
compiler from `rust/rust-toolchain.toml`. See [status](implementation-status.md)
for the difference between local checks, packaging and physical validation.

## Package and example

From the repository root:

```sh
flutter pub get
dart format --output=none --set-exit-if-changed lib hook test integration_test example/lib example/integration_test example/test_driver example/test
dart analyze
python3 tool/documentation_check.py
flutter test
cd example
flutter pub get
dart analyze
flutter test test/testbench_test.dart test/recipes_test.dart
```

Browser contracts require Chrome:

```sh
dart test --platform chrome test/web_chooser_test.dart test/web_gatt_test.dart
```

## Rust and platform dependencies

From `rust`:

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
cargo test --locked --manifest-path vendor/btleplug/Cargo.toml --target-dir target/upstream-tests --lib
```

Root Cargo tests do not execute dependency tests. OS-dependent backend tests are
ignored by default. Run native tests on the relevant OS to validate its code.
On Linux, install D-Bus development headers and pkg-config, then also run:

```sh
cargo test --locked --manifest-path vendor/bluez-async/Cargo.toml --target-dir target/bluez-tests --lib
cargo test --locked --manifest-path vendor/dbus/Cargo.toml --target-dir target/dbus-tests --features futures --lib openble_reply_ownership_tests
python3 ../tool/dbus_async_setup_probe.py --report ../build/dbus-setup.json
```

The private-peer tests exercise registration/cancellation without a system bus.
They do not validate the full engine with healthy BlueZ. Socket opening remains
synchronous; see [limitations](limitations.md).

On macOS, from the repository root, with JDK 17 on PATH:

```sh
python3 tool/jni_host_tests.py
```

Android JVM tests use the committed Gradle wrapper. Resolve the example first:

```sh
cd example
flutter pub get
cd android
./gradlew :bletide:testDebugUnitTest --console=plain
```

Use `gradlew.bat` on Windows. Android unit tests use controlled Java callbacks;
they do not establish physical GATT behavior.

## Repository, provenance and licenses

From the root:

```sh
python3 tool/repository_check.py
python3 tool/test_patch_replay.py
python3 tool/upstream_sources.py
python3 tool/android_sources.py
python3 tool/event_layout.py
python3 tool/native_licenses.py --check
```

The repository check scans tracked and eligible untracked files for common
credential formats, private workstation paths, generated outputs and broken
relative documentation links. It is a targeted check, not a guarantee that all
sensitive data is detected. Inspect reports and commit metadata before publishing.

The source verifiers compare complete inventories and patch hashes. Rust patches
must reverse to the original inventory and reproduce the reviewed sources;
Android's ordered patches must reproduce the bundled Java from pinned upstream.
This is offline consistency evidence, not independent authentication of upstream.
The negative controls reject mismatched origins even when source hashes agree.
The ABI
layout verifier compares native definitions with the installed Dart C header.
The license verifier checks the locked dependency graph. Advisory checks need
network access and the CI-pinned cargo-deny version:

```sh
cd rust
cargo deny --locked check advisories licenses sources
```

## Consumer artifacts

Build the example on the target OS, then inspect its bundled assets:

```sh
python3 tool/android_apks.py --toolchain /absolute/path/to/ndk/toolchains/llvm/prebuilt/host/bin
python3 tool/linux_bundle.py --bundle example/build/linux/x64/release/bundle
python3 tool/windows_bundle.py --bundle example/build/windows/x64/runner/Release
python3 tool/apple_bundle.py --platform macos --app example/build/macos/Build/Products/Release/bletide_example.app
python3 tool/apple_bundle.py --platform ios --app example/build/ios/iphoneos/Runner.app
python3 tool/apple_bundle.py --platform simulator --app example/build/ios/iphonesimulator/Runner.app
```

The verifiers check architecture, native mappings, production exports and
redistribution notices. Android also checks API24 linkage/JNI and alignment;
Apple checks deployment metadata and device/simulator slice identity.
Inspection does not establish runtime loading or radio behavior. The full build
sequence is maintained in [CI](../.github/workflows/ci.yml).

## Hardware scenario

Copy `integration_test/hardware.example.json` to
`integration_test/hardware.local.json`, which is ignored. Replace placeholders
with a controlled peripheral's identity, UUIDs and explicitly safe payloads.
Grant platform permissions first. From `example`:

```sh
flutter drive --driver=test_driver/hardware.dart --target=integration_test/hardware_test.dart --dart-define-from-file=../integration_test/hardware.local.json -d DEVICE_ID
```

The native harness records hardware/firmware identity, versions, checks and
cleanup. Reports and configuration can contain private device identifiers;
review and redact them before sharing. The native widget harness skips Web:
browser discovery must start from a user gesture.

Optional settings are `BLE_NOTIFY_SETUP_MODE` (`standard` or explicit `compat`),
`BLE_REQUEST_MTU` (23–517), and `BLE_CONNECTION_PRIORITY` (`balanced`, `high`,
`lowPower`). Setup is reported only after ACK; unsupported link hints are recorded
without issuing platform calls. No physical suite has completed yet.

## Coverage map

| Sources | Covered contracts |
|---|---|
| `test/support/gatt_contract.dart` | Shared fake/native/browser GATT behavior |
| `test/backend_contract_test.dart` | Scan ownership, adapter loss, deadlines and retry |
| `test/native_abi_test.dart` | Actual compiled ABI/VM-port copying, bounds and cleanup |
| `test/native_backend_test.dart`, `test/native_wire_test.dart` | Wire decoding, causes and stale generations |
| `test/capabilities_test.dart` | Capability gates, notification readiness and bounded buffering |
| `test/web_chooser_test.dart`, `test/web_gatt_test.dart` | Controlled browser promises and late-result cleanup |
| `example/test/testbench_test.dart` | Explorer controls and lifecycle ownership |
| `rust/src/{engine,connection,scan}/tests.rs` | Owned workers, cleanup barriers and panic containment through private child-module access |
| Vendored backend and D-Bus suites | Platform reply/callback ownership and transport regressions |

See [mandatory races](mandatory-races.md) for assertion names and
[review checklist](requirements-audit.md) for unresolved native boundaries.
The [organization guide](code-organization.md) explains production/test and
native-driver boundaries; moving a suite must preserve assertions and fixture paths.
The repeated panic-payload destructor test deliberately aborts its child process.
Run negative controls only in isolated source copies with separate target folders.
