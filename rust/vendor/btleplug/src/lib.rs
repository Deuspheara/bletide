// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.
//
// Some portions of this file are taken and/or modified from Rumble
// (https://github.com/mwylde/rumble), using a dual MIT/Apache License under the
// following copyright:
//
// Copyright (c) 2014 The Rust Project Developers

//! btleplug is a Bluetooth Low Energy (BLE) central module library for Rust.
//! It currently supports Windows 10, macOS (and possibly iOS) and Linux
//! (BlueZ). Android support is planned for the future.
//!
//! ## Usage
//!
//! An example of how to use the library to control some BLE smart lights:
//!
//! ```rust,no_run
//! use btleplug::api::{bleuuid::uuid_from_u16, Central, Manager as _, Peripheral as _, ScanFilter, WriteType};
//! use btleplug::platform::{Adapter, Manager, Peripheral};
//! use rand::{RngExt, rng};
//! use std::error::Error;
//! use std::thread;
//! use std::time::Duration;
//! use tokio::time;
//! use uuid::Uuid;
//!
//! const LIGHT_CHARACTERISTIC_UUID: Uuid = uuid_from_u16(0xFFE9);
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     let manager = Manager::new().await.unwrap();
//!
//!     // get the first bluetooth adapter
//!     let adapters = manager.adapters().await?;
//!     let central = adapters.into_iter().nth(0).unwrap();
//!
//!     // start scanning for devices
//!     central.start_scan(ScanFilter::default()).await?;
//!     // instead of waiting, you can use central.events() to get a stream which will
//!     // notify you of new devices, for an example of that see examples/event_driven_discovery.rs
//!     time::sleep(Duration::from_secs(2)).await;
//!
//!     // find the device we're interested in
//!     let light = find_light(&central).await.unwrap();
//!
//!     // connect to the device
//!     light.connect().await?;
//!
//!     // discover services and characteristics
//!     light.discover_services().await?;
//!
//!     // find the characteristic we want
//!     let chars = light.characteristics();
//!     let cmd_char = chars.iter().find(|c| c.uuid == LIGHT_CHARACTERISTIC_UUID).unwrap();
//!
//!     // dance party
//!     let mut rng = rng();
//!     for _ in 0..20 {
//!         let color_cmd = vec![0x56, rng.random(), rng.random(), rng.random(), 0x00, 0xF0, 0xAA];
//!         light.write(&cmd_char, &color_cmd, WriteType::WithoutResponse).await?;
//!         time::sleep(Duration::from_millis(200)).await;
//!     }
//!     Ok(())
//! }
//!
//! async fn find_light(central: &Adapter) -> Option<Peripheral> {
//!     for p in central.peripherals().await.unwrap() {
//!         if p.properties()
//!             .await
//!             .unwrap()
//!             .unwrap()
//!             .local_name
//!             .iter()
//!             .any(|name| name.contains("LEDBlue"))
//!         {
//!             return Some(p);
//!         }
//!     }
//!     None
//! }
//! ```

use crate::api::ParseBDAddrError;
use std::result;
use std::time::Duration;

#[cfg(any(target_os = "android", target_os = "windows", test))]
mod advertisement;
pub mod api;
#[cfg(target_os = "linux")]
mod bluez;
#[cfg(not(target_os = "linux"))]
mod common;

#[cfg(any(target_os = "linux", test))]
#[path = "bluez/attribute_map.rs"]
mod attribute_map;
#[cfg(test)]
#[path = "../../bluez-async/src/cleanup.rs"]
mod bluez_cleanup_tests;
#[cfg(test)]
#[path = "../../bluez-async/src/node_name.rs"]
mod bluez_node_name_tests;
#[cfg(test)]
#[path = "../../bluez-async/src/object_path.rs"]
mod bluez_object_path_tests;
#[cfg(test)]
#[path = "../../bluez-async/src/signal_queue.rs"]
mod bluez_signal_queue_tests;
#[cfg(test)]
mod bluez_signal_failure_tests {
    #[tokio::test]
    async fn overflow_reaches_owned_transport_and_adapter_monitor() {
        use futures::StreamExt;
        let (cleanup_sender, cleanup) = super::bluez_cleanup_tests::queue();
        let publisher = cleanup_sender.clone();
        let (mut signals, receiver) = super::bluez_signal_queue_tests::queue(move || {
            publisher.record_failure("D-Bus signal event queue overflow");
        });
        let task = super::session_task::SessionTask::spawn(async move {
            super::bluez_cleanup_tests::drive(std::future::pending::<()>(), cleanup)
                .await
                .map(|_| ())
        });
        let mut events = Box::pin(task.monitor(futures::stream::pending::<u8>()));
        // No consumer is reading; a burst must retire the owned transport.
        for value in 0..300_u16 {
            signals.push(value);
        }
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
                .await
                .unwrap(),
            Some(Err("D-Bus signal event queue overflow".into()))
        );
        assert_eq!(events.next().await, None);
        assert_eq!(
            cleanup_sender.failure().as_deref(),
            Some("D-Bus signal event queue overflow")
        );
        assert!(
            matches!(task.close().await, Err(crate::Error::RuntimeError(message)) if message == "D-Bus signal event queue overflow")
        );
        drop(receiver);
    }
}

#[cfg(any(target_os = "linux", test))]
#[path = "bluez/lookup_error.rs"]
mod bluez_lookup_error;
#[cfg(target_vendor = "apple")]
mod corebluetooth;
#[cfg(target_os = "android")]
mod droidplug;
#[cfg(any(target_os = "linux", test))]
#[path = "bluez/session_task.rs"]
mod session_task;
#[cfg(all(not(target_os = "android"), feature = "jni-host-tests"))]
#[allow(dead_code)]
mod droidplug {
    mod jni_utils;
}
pub mod platform;
#[cfg(feature = "serde")]
pub mod serde;
#[cfg(target_os = "windows")]
mod winrtble;

/// The main error type returned by most methods in btleplug.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Permission denied")]
    PermissionDenied,

    #[error("Device not found")]
    DeviceNotFound,

    #[error("Not connected")]
    NotConnected,

    #[error("Unexpected callback")]
    UnexpectedCallback,

    #[error("Unexpected characteristic")]
    UnexpectedCharacteristic,

    #[error("No such characteristic")]
    NoSuchCharacteristic,

    #[error("No Bluetooth adapter available")]
    NoAdapterAvailable,

    #[error("The operation is not supported: {}", _0)]
    NotSupported(String),

    #[error("Timed out after {:?}", _0)]
    TimedOut(Duration),

    #[error("Error parsing UUID: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("Invalid Bluetooth address: {0}")]
    InvalidBDAddr(#[from] ParseBDAddrError),

    #[error("Runtime Error: {}", _0)]
    RuntimeError(String),

    #[error("{}", _0)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(target_os = "linux")]
#[path = "bluez/native_error.rs"]
mod bluez_native_error;

#[cfg(any(target_os = "windows", test))]
#[path = "winrtble/gatt_status.rs"]
mod windows_gatt_status;

#[cfg(any(target_os = "windows", test))]
#[path = "winrtble/retrieval_match.rs"]
mod windows_retrieval_match;

#[cfg(any(target_os = "windows", test))]
#[path = "winrtble/ble/setup_cleanup.rs"]
mod windows_setup_cleanup;

impl Error {
    /// Original platform code when this error retains a typed native cause.
    /// This local extension never infers a code from formatted error text.
    pub fn native_code(&self) -> Option<String> {
        #[cfg(target_os = "windows")]
        if let Self::Other(error) = self {
            return error
                .downcast_ref::<windows::core::Error>()
                .map(|error| windows_native_code(error.code().0))
                .or_else(|| {
                    error
                        .downcast_ref::<windows_gatt_status::GattStatusError>()
                        .map(windows_gatt_status::GattStatusError::native_code)
                });
        }
        #[cfg(target_os = "linux")]
        if let Self::Other(error) = self {
            return bluez_native_error::native_code(error.as_ref());
        }
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        if let Self::Other(error) = self {
            return error
                .downcast_ref::<corebluetooth::native_error::NativeError>()
                .and_then(|error| error.native_code.clone());
        }
        None
    }

    /// Whether this error represents denied access, including retained WinRT
    /// E_ACCESSDENIED or D-Bus AccessDenied causes. Native diagnostics are retained.
    pub fn is_permission_denied(&self) -> bool {
        if matches!(self, Self::PermissionDenied) {
            return true;
        }
        #[cfg(target_os = "windows")]
        if let Self::Other(error) = self {
            return error
                .downcast_ref::<windows::core::Error>()
                .is_some_and(|error| error.code().0 == 0x80070005_u32 as i32);
        }
        #[cfg(target_os = "linux")]
        if let Self::Other(error) = self {
            return bluez_native_error::is_permission_denied(error.as_ref());
        }
        false
    }
}

#[cfg(any(target_os = "windows", test))]
fn windows_native_code(code: i32) -> String {
    // HRESULT is a signed i32; its diagnostic identity is the same 32 bits.
    format!("0x{:08X}", code as u32)
}

#[cfg(test)]
mod native_code_tests {
    use super::*;
    #[test]
    fn hresult_format_preserves_sign_bit_and_all_eight_hex_digits() {
        for (bits, expected) in [
            (0x80070005_u32, "0x80070005"),
            (0x80004005, "0x80004005"),
            (0x8000ffff, "0x8000FFFF"),
            (0xffffffff, "0xFFFFFFFF"),
            (1, "0x00000001"),
            (0, "0x00000000"),
        ] {
            assert_eq!(windows_native_code(bits as i32), expected);
        }
    }
    #[test]
    fn formatted_text_and_portable_errors_do_not_invent_native_codes() {
        for error in [
            Error::Other("permission denied 0x80070005".into()),
            Error::RuntimeError("HRESULT 80004005".into()),
            Error::PermissionDenied,
        ] {
            assert_eq!(error.native_code(), None);
        }
    }
}

/// Convert [`PoisonError`] to [`Error`] for replace `unwrap` to `map_err`
impl<T: std::fmt::Debug> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Self::Other(format!("{:?}", e).into())
    }
}

/// Convenience type for a result using the btleplug [`Error`] type.
pub type Result<T> = result::Result<T, Error>;
