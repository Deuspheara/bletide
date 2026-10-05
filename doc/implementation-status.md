# Implementation status

Bletide is experimental and unreleased. The native backend uses Rust/btleplug;
the browser backend uses Web Bluetooth. API and lifecycle contracts are tested,
but no platform/peripheral combination has completed the hardware suite.

## Implemented

- Adapter state, discovery or browser chooser, connect/disconnect and GATT discovery.
- Characteristic/descriptor reads, writes and owned notification subscriptions.
- FIFO operations, deadlines, cancellation, connection generations and explicit cleanup.
- Capability-gated RSSI/MTU snapshots, write budgets and Android link hints.
- Typed errors with native causes, diagnostics without BLE payload fields and fake backends.
- Native asset hooks, Android JNI/R8 packaging, example explorer and hardware harness.
- Pinned vendored sources, reviewed patches and redistribution license inventories.

See [architecture](architecture.md), [lifecycle](lifecycle.md),
[platforms](platforms.md) and [limitations](limitations.md).

## Verification snapshot

Local checks on macOS, 2026-10-05, after the audit continuation and source reorganization:

| Check | Result |
|---|---|
| Package tests | 202 passed, including compiled native ABI/VM-port contracts |
| Example widget tests | 16 passed (explorer and executable recipes) |
| Root Rust tests | 99 passed |
| Browser JavaScript contracts | 38 passed |
| README examples | Type-checked |
| Browser example build | Passed |
| Vendored backend host suite | 203 passed, 8 OS-dependent tests ignored |
| macOS release consumer | Rebuilt as Bletide; bundle verifier passed |
| Actual bundled Java/JNI suite | 258 passed, 8 OS-dependent tests ignored |
| Android JVM callback/lifecycle tests | Passed |
| Android release consumers | ARMv7/ARM64/x64 rebuilt; APK verifiers passed |
| Dart analysis and formatting | Passed for package and example |
| Rust formatting and strict Clippy | Passed with all targets/features on the host |
| Upstream Rust/Android source inventories | Passed |
| Native redistribution license inventory | Passed |
| Fresh locked dependency advisories/licenses/sources | Passed with CI-pinned cargo-deny 0.20.2 |
| Native event/header layout | Passed for the host |
| Isolated checkout with public uncommitted overlay | Formatting, analysis, root/example tests and provenance checks passed; no local config/build outputs copied |
| pub.dev archive dry-run | Completed with one expected uncommitted-files warning; nothing published |

The current Bletide checks above refresh macOS and Android packaging. Historical
checks also cover unsigned iOS device/simulator builds.
Before this uncommitted continuation, GitHub Actions also built Windows and Linux release consumers
and passed their bundle verifiers. Windows x64/ARM64 target checks passed.
The revised uncommitted tree has not run the GitHub Actions matrix. These are
build/artifact results, not hardware validation.
Browser contract tests use controlled JavaScript; browser compilation does not
prove physical Bluetooth behavior.

The [CI workflow](../.github/workflows/ci.yml) defines macOS, Linux and Windows
jobs, including Android, Apple and browser packaging. Consult the
[Actions results](https://github.com/Deuspheara/bletide/actions/workflows/ci.yml)
for the complete result on a specific commit. Reproducible commands
are in [testing](testing.md). Local temporary logs are not public attestations.

## Release gates

Before publishing an experimental GitHub release:

- Verify a fresh checkout, including source inventories and required Gradle wrapper files.
- Run the complete GitHub Actions matrix and fix failures.
- Inspect the public file list and commit history for credentials and private metadata.
- Set repository/issue links and document the supported toolchains.

Before claiming stable platform support:

- Exercise Windows DLL loading and WinRT error paths on physical adapters.
- Execute current Linux consumers with healthy BlueZ/D-Bus, including ARM64.
- Validate current Android API/ABI combinations and signed iOS device execution.
- Run controlled-peripheral discovery, GATT, notification, reconnect and stress scenarios.
- Finish foreign-boundary, OS resource-retention and diagnostics review.

The [review checklist](requirements-audit.md) and
[race coverage map](mandatory-races.md) identify the remaining evidence limits.
