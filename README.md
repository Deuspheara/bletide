# Bletide

Experimental BLE **central/client** transport for Dart and Flutter. Bletide uses direct FFI
into Rust, btleplug and Tokio on native platforms, and Web Bluetooth in browsers.

**Experimental, unreleased.** Deterministic API/lifecycle tests pass and native
consumer builds have been inspected. Physical GATT, reconnect and resource-stress
validation remain incomplete. See the [verification status](doc/implementation-status.md).

## What it provides

- Discovery, connections, service/characteristic/descriptor access, reads and writes.
- Awaitable notification setup with explicit subscription ownership and cleanup.
- Typed errors, cancellation, deadlines and connection generations.
- Capability checks, payload-free diagnostic fields and an injectable fake backend.
- Rust native-assets build hooks and an interactive Flutter explorer.

Bletide is a transport library. Applications own runtime permissions and their
peripheral protocols.

Choose Bletide for experiments where you want to inspect a Rust-backed transport,
control connection/subscription ownership, or test application logic with injected
backends. Expect to validate your own platform and peripheral. For production
adoption, compare established libraries and their current licenses first; see
[API/documentation comparison](doc/library-comparison.md).

Bluetooth Classic, peripheral/server mode, automatic reconnect, bonding control
and background restoration are outside the current API. Applications handle
reconnect policy and rediscover attributes for each new connection generation.

## Get started

Clone [the repository](https://github.com/Deuspheara/bletide):

```sh
git clone https://github.com/Deuspheara/bletide.git
```

Until a package release is available, clone this repository and use a local path
dependency in your Flutter app:

```yaml
dependencies:
  bletide:
    path: ../bletide
```

Or use a Git dependency pinned to a reviewed commit (this is the audit baseline):

```yaml
dependencies:
  bletide:
    git:
      url: https://github.com/Deuspheara/bletide.git
      ref: 6f8e080aca327be2f60540dbc37bd9a6038284a1
```

Run `flutter pub get` in the consuming app. Upgrade the pinned revision deliberately;
the unreleased API can change. Keep the application's lockfile under version control.

Use Flutter >=3.47.2 / Dart >=3.13.2, Rustup and the compiler pinned in
`rust/rust-toolchain.toml`. Native builds require the host platform's development
tools. The build hook compiles and bundles Rust; no manual library loader is needed.

This native example scans for the standard Battery Service and reads Battery Level:

```dart
import 'package:bletide/bletide.dart';

Future<int> readBatteryLevel() async {
  final ble = Ble();
  try {
    await ble.ready;
    final device = await ble
        .scan(filter: BleScanFilter(serviceUuids: [BleUuid('180f')]))
        .first
        .timeout(const Duration(seconds: 15));
    final connection = await ble.connect(device.deviceId);
    try {
      final services = await connection.discoverServices();
      final battery = services.firstWhere((s) => s.uuid == BleUuid('180f'));
      final level = battery.characteristics.firstWhere(
        (c) => c.uuid == BleUuid('2a19') && c.properties.read,
      );
      final bytes = await connection.read(level);
      if (bytes.isEmpty) throw StateError('Empty battery level');
      return bytes.first;
    } finally {
      await connection.disconnect();
    }
  } finally {
    await ble.close();
  }
}
```

Grant platform permissions first. The peripheral must advertise the service and
expose a readable Battery Level characteristic. Device IDs are opaque; use the
identity returned by discovery.

## Notifications

Retain the owner returned after setup acknowledgment. Cancel it explicitly when
finished; cancelling its values listener also releases that owner's subscription.

```dart
import 'dart:typed_data';
import 'package:bletide/bletide.dart';

Future<void> observeForFiveSeconds(
  BleConnection connection,
  BleCharacteristic characteristic,
  void Function(Uint8List) onValue,
  void Function(Object) onError,
) async {
  final owner = await connection.enableNotifications(characteristic);
  final listener = owner.values.listen(onValue, onError: onError);
  try {
    await Future<void>.delayed(const Duration(seconds: 5));
  } finally {
    await listener.cancel();
    await owner.cancel();
  }
}
```

Standard CCCD behavior is the default. A known nonstandard peripheral can opt in
per characteristic with `setupMode: BleNotificationSetupMode.compat` on Android
or Apple platforms. Other backends return `notSupported`. See
[notification compatibility](doc/limitations.md#notification-compatibility).

## Writes and a complete session

Use a discovered characteristic with the matching write property. Send only
payloads defined by your peripheral's protocol; Bletide does not split writes.
With-response completion confirms ATT acceptance, not a protocol-level reply.

```dart
import 'dart:typed_data';
import 'package:bletide/bletide.dart';

Future<void> sendCommand(
  BleConnection connection,
  BleCharacteristic command,
  Uint8List protocolPayload,
) async {
  if (!command.properties.write) throw StateError('Write with response required');
  final budget = await connection.getWritePayloadLimit(); // native platforms
  if (protocolPayload.length > budget) throw StateError('Exceeds single-write budget');
  await connection.write(command, protocolPayload);
}
```

[Runnable recipes](example/lib/recipes.dart) combine scan/chooser selection,
connect, discovery, read, notification setup, write and cleanup. Import them into
the example app or adapt them to your app's protocol; all UUIDs and write bytes
are caller-supplied. Run their deterministic example with
`cd example && flutter test test/recipes_test.dart`. For interactive hardware
use, run `flutter run -d macos` from `example`, or select your configured device.
The explorer requests no protocol-specific write automatically.

## Browser discovery

Call `ble.requestDevice()` directly from a user gesture such as a button handler.
Use `BleDeviceRequest(optionalServices: [BleUuid('180f')])` to grant access to
services beyond the chooser filters. Connect using the returned `deviceId`.
Continuous scanning, RSSI and MTU reporting are unavailable on Web. Browser
promises and the chooser cannot be physically aborted; late results are discarded.

## Platforms

| Platform | Integration | Requirements / limits |
|---|---|---|
| Android | Rust + bundled JNI/Java | API24+, app-owned Bluetooth permissions |
| iOS / macOS | Rust + CoreBluetooth | Xcode, Bluetooth usage descriptions; macOS entitlements |
| Windows | Rust + WinRT | Windows C++ build tools; Flutter loading/runtime validation pending |
| Linux | Rust + BlueZ/D-Bus | D-Bus development libraries for builds; healthy BlueZ/access at runtime |
| Web | Browser Web Bluetooth | Supporting browser, secure context, user gesture and service grants |

Check `ble.capabilities` before platform-specific operations. Android supports
explicit MTU requests and connection-priority hints. `getWritePayloadLimit()`
returns a conservative single-write budget where supported.
See [platform details](doc/platforms.md) and [limitations](doc/limitations.md).

## Development

Run the explorer from `example` with `flutter run -d macos` or your configured
target. Inject `FakeBleBackend` from `package:bletide/testing.dart` in deterministic
application tests. Fake operations complete when the test resolves their ACKs.

- [Contributing](CONTRIBUTING.md) and [testing](doc/testing.md)
- [Architecture](doc/architecture.md), [source organization](doc/code-organization.md) and [lifecycle](doc/lifecycle.md)
- [API contracts and troubleshooting](doc/api-guide.md)
- [Release checklist](doc/releasing.md) and [security reporting](SECURITY.md)

Library diagnostics omit BLE payload fields. Device IDs and native error messages
can still contain private identifiers; redact shared reports and logs.

## License

MIT. Vendored dependencies retain their original licenses and attribution.
`THIRD_PARTY_NOTICES` describes native modifications; `THIRD_PARTY_LICENSES`
contains the redistribution inventory. Both are Flutter package assets; the
example registers them with `LicenseRegistry` and exposes a Licenses button.
