# Contributing

Bletide is experimental. Bug reports, focused fixes, documentation improvements
and platform validation are welcome. Read the [architecture](doc/architecture.md)
and [lifecycle](doc/lifecycle.md) before changing cancellation or resource ownership.
Use the [organization guide](doc/code-organization.md) to locate owners and native
drivers. State a concrete maintenance problem before splitting a file; preserve
test access, guarded disposal and joined cleanup rather than applying line limits.

## Development setup

Install Flutter 3.47.2 or later (Dart 3.13.2 or later), Rustup and the native
build tools for your host. The Rust compiler is pinned in
`rust/rust-toolchain.toml`. Android requires JDK 17, the Android SDK/NDK and API24+.
Apple builds require Xcode. Linux builds need D-Bus development headers,
pkg-config, Clang, CMake, Ninja and GTK development packages.

Run `flutter pub get` in the root and `example`. Follow [testing](doc/testing.md)
for analysis, formatting, package/Rust tests, browser contracts and platform checks.

## Pull requests

Explain the concrete failure or behavior change, its platform scope and how it
was verified. Keep changes focused. Add a regression for a reproduced bug or a
meaningful contract; do not replace native evidence with a fake pass. Keep the
[implementation status](doc/implementation-status.md) accurate when support changes.

Run relevant format, analysis and tests before submitting. Native backend edits
need the affected vendored suite and consumer verification in addition to root tests.
A cross-compile is useful evidence but does not establish runtime support.

## Vendored dependencies

`rust/vendor` and the bundled Android Java sources are intentional. They contain
reviewed lifecycle fixes and extensions unavailable in upstream releases. Preserve
upstream licenses, source metadata and original attribution.

For a vendor change, update the reviewed patch and its source/hash inventory,
run `tool/upstream_sources.py` or `tool/android_sources.py`, and review the diff.
Update redistribution notices with `tool/native_licenses.py` when dependencies
change. Do not reformat unrelated upstream code or bypass hash checks.
See `android/UPSTREAM.md` for the Java patch sequence.

## Reports and privacy

Provide OS/toolchain versions, reproduction steps and a minimal example. Redact
private device names/addresses, payloads, credentials and local paths from logs.
Keep hardware settings in the ignored `integration_test/hardware.local.json`.
Use [SECURITY.md](SECURITY.md) for vulnerabilities. Contributions are MIT licensed;
vendored code retains its original licenses.
