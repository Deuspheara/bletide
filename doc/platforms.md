# Platforms

Features below describe implemented behavior, not completed hardware validation.
No platform/peripheral combination has completed the physical suite. Current
verification scope is recorded in [implementation status](implementation-status.md).

| Feature | Android | iOS / macOS | Windows | Linux | Web |
|---|---|---|---|---|---|
| Adapter state | Native | Native | Native | Native | Browser availability |
| Discovery | Scan | Scan | Scan | Scan | Chooser |
| Connections and GATT | Implemented | Implemented | Implemented | Implemented | Browser-permitted attributes |
| Descriptor access | Implemented | Implemented | Implemented | Implemented | Browser-permitted descriptors |
| Notifications with awaitable setup | Implemented | Implemented | Implemented | Implemented | Browser promise |
| Fresh connected RSSI | Supported | Supported | Unsupported | Unsupported | Unsupported |
| MTU snapshot / write budget | Supported | Inferred snapshot | Snapshot | Snapshot / fallback | Unsupported |
| Explicit MTU request | Supported | Unsupported | Unsupported | Unsupported | Unsupported |
| Connection-priority hint | Supported | Unsupported | Unsupported | Unsupported | Unsupported |
| Nonstandard CCCD opt-in | Local routing | Narrow callback tolerance | Unsupported | Unsupported | Unsupported |

Use `ble.capabilities` to check operations. An unsupported request returns
`BleErrorCode.notSupported` rather than fabricated data. See
[limitations](limitations.md) for MTU, identity and cancellation semantics.

## Android

API24 or later, JDK 17 and the Android SDK/NDK are required. Native BLE operations
use Rust FFI; the small Flutter plugin bootstraps JNI and observes adapter state.
The package bundles pinned upstream Java classes and consumer R8 keep rules.
Applications must declare/request permissions before scanning or connecting.
The example's manifest and activity show the required version-dependent setup.

The library manifest merges legacy Bluetooth and location declarations (through
API30), plus `BLUETOOTH_SCAN` with `neverForLocation` and `BLUETOOTH_CONNECT`.
Request SCAN/CONNECT at runtime on API31+; request fine location on API24–30 and
check system location settings if discovery is empty. The package does not
display runtime permission prompts. Apps that derive location must review the
merged manifest and the `neverForLocation` policy; it may filter some beacons.
See [Android's permission guide](https://developer.android.com/develop/connectivity/bluetooth/bt-permissions)
and the [example activity](../example/android/app/src/main/kotlin/com/example/bletide/MainActivity.kt).
Use compileSdk 36 and NDK 28.2.13676358 for the tested example; minSdk is 24.

Android 14+ can return the existing MTU negotiation. Priority requests are hints:
an acknowledgment does not prove a peer-negotiated interval. Compat notification
setup enables local routing while skipping the CCCD write.

## Apple

Build with Xcode. Consuming apps need Bluetooth usage descriptions; sandboxed
macOS apps also need Bluetooth entitlements. See the example's `Info.plist` and
entitlement files. Unsigned device packaging does not prove signed iOS execution.
Add `NSBluetoothAlwaysUsageDescription` with an app-specific explanation to
`Info.plist`; the example also includes `NSBluetoothPeripheralUsageDescription`.
For sandboxed macOS enable `com.apple.security.device.bluetooth` in both debug
and release entitlements. The example targets iOS15+ and macOS12+; review
consuming-app deployment targets separately.
CoreBluetooth identities are opaque UUIDs, not interchangeable with MAC addresses.

Compat notifications still call `setNotifyValue` and await the native callback.
Only the owning opted-in request's missing-CCCD error is tolerated.

## Windows

Build on Windows with the Flutter desktop prerequisites and MSVC toolchain.
Native x64/ARM64 libraries have cross-link evidence; Flutter consumer loading,
WinRT fault injection and runtime redistribution of the required C runtime remain
unverified. Execute the consumer and hardware suite before claiming stable support.

## Linux

Compilation needs D-Bus development headers and pkg-config. Flutter desktop
builds also need Clang, CMake, Ninja and GTK development packages. Runtime needs
a healthy BlueZ service and D-Bus permissions. `DBUS_SYSTEM_BUS_ADDRESS` can set
a custom system bus address; the default is `/run/dbus/system_bus_socket`.

Current native cross-link checks do not establish current consumer execution,
healthy-bus cleanup or ARM64 Flutter runtime. Synchronous socket opening remains
a cancellation/deadline limitation.

## Web

Web Bluetooth requires a supporting browser, a secure context and a chooser
started directly from a user gesture. Filters use OR clauses; `optionalServices`
grants service access beyond the selected filter. Chooser results are identities,
not scan advertisements; unavailable RSSI/service metadata remains absent.

Availability represents browser support/access, not proof of a powered adapter.
Continuous native scanning is unsupported. Wasm runtime and physical
browser/peripheral validation remain unverified.
