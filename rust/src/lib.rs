#![deny(unsafe_op_in_unsafe_fn)]
#[cfg(any(target_os = "android", test))]
mod adapter_state;
#[cfg(target_os = "android")]
mod android;
mod codec;
mod connection;
mod engine;
mod event;
mod ffi;
#[cfg(feature = "test-support")]
mod fixture;
#[cfg(any(target_os = "android", test))]
mod peripheral_lease;
mod resources;
mod scan;
#[cfg(any(target_os = "android", test))]
mod shared_scan;

// Exercise the actual Windows publication barrier on every host test runner.
#[cfg(test)]
#[path = "../vendor/btleplug/src/winrtble/ble/callback_gate.rs"]
mod windows_callback_gate;
#[cfg(test)]
use windows_callback_gate as callback_gate;

#[cfg(test)]
#[path = "../vendor/btleplug/src/winrtble/ble/callback_failure.rs"]
mod windows_callback_failure;

#[path = "../vendor/btleplug/src/winrtble/ble/callback_boundary.rs"]
mod callback_boundary;

#[cfg(target_os = "android")]
#[path = "../vendor/btleplug/src/droidplug/jni_utils/error_policy.rs"]
mod android_error_policy;
