# Bletide testbench

Run `flutter run -d macos` (or another configured platform) from this directory.
The explorer displays adapter state, capabilities, advertisements, connection
generations, GATT services/characteristics/descriptors, binary read/write,
notifications, and recent diagnostics. Controls follow backend capabilities.

The native transport implementation is still in progress; see the package's
platform matrix before interpreting a build as device support. Request Android
runtime Bluetooth permissions before scanning; app-level permission UI is not a
BLE transport feature.
