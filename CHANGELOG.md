## 0.0.1 (unreleased)

- Begin bletide native ABI, lifecycle, immutable models, and deterministic tests.
- Share notification setup, await final teardown, and discard buffered values from retired connections.
- Default to standard notification setup; expose per-subscription opt-in nonstandard CCCD compatibility on Android and Apple platforms, with explicit unsupported errors elsewhere.
- Name the explicit notification setup API `BleNotificationSetupMode.standard` / `.compat`, selected with the optional `setupMode` argument.
- Preserve structured native error metadata for Android scan codes, retained Windows HRESULTs, typed Linux D-Bus names and Apple notification setup errors.
- Preserve Apple GATT/RSSI native error metadata and prevent failed RSSI callbacks from publishing invalid values.
- Acknowledge confirmed Apple disconnects and failed-connect cancellation cleanup; retain connection and primary discovery native causes.
- Fail Apple characteristic/descriptor discovery on native errors while preserving cached attributes, pending reads and explicit retry readiness.
