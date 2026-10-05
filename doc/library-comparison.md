# Learning from other BLE libraries

Documentation/API review on 2026-10-05; this is not a hardware performance
benchmark or a feature-parity claim. Follow the source links for current details.
No implementation code was copied.

| Project and official source | Useful practice | Bletide decision |
|---|---|---|
| [FlutterBluePlus README](https://github.com/chipweinberger/flutter_blue_plus/blob/master/packages/flutter_blue_plus/README.md) | Task-oriented examples, cleanup helpers, platform table, mocking and common-problem guidance | Add executable recipes, an API ownership table and troubleshooting; keep explicit error/stream termination semantics. |
| [Flutter Reactive BLE README](https://github.com/PhilipsHue/flutter_reactive_ble/blob/master/README.md) | Scoped characteristic identities, connection status streams, distinction between ATT write acknowledgment and protocol response | Document generation-scoped rediscovery, application retry policy and write acknowledgment. Keep awaitable notification setup alongside the stream convenience API. |
| [btleplug README](https://github.com/deviceplug/btleplug/blob/master/README.md) | Explicit central-mode scope, platform prerequisites and controlled-peripheral integration instructions | Preserve attribution and reviewed patches, label build evidence separately from physical verification, publish reproducible hardware steps. |
| [Bluest README](https://github.com/alexmoon/bluest) | Rust API organized around adapter/device/GATT objects and documented platform/runtime caveats | Keep backend/platform details behind the Dart façade; document ownership instead of introducing another transport dependency. |

FlutterBluePlus offers device/attribute methods and cleanup helpers; Reactive BLE
uses a controller with qualified characteristics and connection streams. Bletide
uses an owned engine, one connection object per generation and explicit
subscription owners. These are ergonomic choices, not evidence that Bletide has
equivalent runtime maturity, background support or peripheral compatibility.

Licenses differ: inspect [FlutterBluePlus's current license](https://github.com/chipweinberger/flutter_blue_plus/blob/master/packages/flutter_blue_plus/LICENSE.md)
(including its commercial-use conditions), [Reactive BLE's BSD license](https://github.com/PhilipsHue/flutter_reactive_ble/blob/master/LICENSE),
[btleplug's BSD/upstream notices](https://github.com/deviceplug/btleplug/blob/master/LICENSE.md)
and Bluest's [BSD-2-Clause](https://github.com/alexmoon/bluest/blob/main/LICENSE-BSD)
and [Apache-2.0](https://github.com/alexmoon/bluest/blob/main/LICENSE-APACHE) texts.
Bletide remains MIT, with native dependencies retaining their own terms.

## Actual source layouts inspected

Source-layout review: 2026-10-05. These are the inspected upstream branch
sources, not pinned Bletide dependencies or guarantees about future upstream
revisions. Bletide's actual pins remain in its provenance manifests.

| Project / inspected source | Observed boundary | Fits Bletide | Does not fit this library |
|---|---|---|---|
| Flutter Reactive BLE: [package tree](https://github.com/PhilipsHue/flutter_reactive_ble/tree/master/packages), [Dart tree](https://github.com/PhilipsHue/flutter_reactive_ble/tree/master/packages/flutter_reactive_ble/lib/src), [facade](https://github.com/PhilipsHue/flutter_reactive_ble/blob/master/packages/flutter_reactive_ble/lib/src/reactive_ble.dart), [connector](https://github.com/PhilipsHue/flutter_reactive_ble/blob/master/packages/flutter_reactive_ble/lib/src/device_connector.dart) | Facade delegates to scanner, connector and connected-device operations; mobile implementation/platform interface are separate packages. Connector composes streams and cancellation cleanup. | Give scanning/admission and generation operations clear source locations; retain backend injection and qualified identities. | Federation adds package/version coordination to one Rust ABI and one browser backend. Its singleton facade and connection-stream ownership differ from Bletide's explicitly closed engines and awaitable generation objects. |
| btleplug: [source tree](https://github.com/deviceplug/btleplug/tree/master/src), [API traits](https://github.com/deviceplug/btleplug/blob/master/src/api/mod.rs), [platform selector](https://github.com/deviceplug/btleplug/blob/master/src/platform.rs) | Common central/peripheral API; separate BlueZ, CoreBluetooth, Android and WinRT implementations selected by target configuration. | Keep workers behind existing Driver/Device contracts and isolate OS integration in native child modules. | Recreating the OS tree would duplicate the vendored layer. Bletide's workers additionally own request generations, VM events and cleanup barriers; these must stay visible. |
| Tokio: [source tree](https://github.com/tokio-rs/tokio/tree/master/tokio/src), [runtime modules](https://github.com/tokio-rs/tokio/tree/master/tokio/src/runtime), [runtime entry](https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/mod.rs), [JoinSet](https://github.com/tokio-rs/tokio/blob/master/tokio/src/task/join_set.rs) | Named subsystems; runtime separates scheduling, task machinery and public entry points. JoinSet explicitly owns tasks and exposes join/shutdown semantics. | Keep engine/adapter/connection owners and joins visible. Child test modules separate large suites while retaining private access. | A scheduler's many submodules/configuration layers are unnecessary. Task abort alone is not Bletide cleanup acknowledgement; worker cleanup and registry retirement must finish. |
| FlutterBluePlus: [library entry](https://github.com/chipweinberger/flutter_blue_plus/blob/master/packages/flutter_blue_plus/lib/flutter_blue_plus.dart), [device operations](https://github.com/chipweinberger/flutter_blue_plus/blob/master/packages/flutter_blue_plus/lib/src/bluetooth_device.dart), [library tree](https://github.com/chipweinberger/flutter_blue_plus/tree/master/packages/flutter_blue_plus/lib) | One Dart library assembles device/service/characteristic/descriptor and utility parts. Device methods access facade-owned connection/subscription registries. | Parts improve navigation while preserving private lifecycle access and public imports. Bletide uses two new parts, rather than a file per model. | Parts do not enforce independent module privacy. Global registries or reconnectable device objects would change engine/generation ownership contracts. |

These are design comparisons derived from the inspected code; no implementation
was copied. The [per-file organization review](code-organization.md) records
actual maintenance problems, mutable state and retain/split decisions. Similar
folders do not establish equivalent cleanup or hardware behavior.
