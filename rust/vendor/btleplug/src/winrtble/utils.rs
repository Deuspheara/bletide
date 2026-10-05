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

use crate::{Result, api::CharPropFlags};
use uuid::Uuid;
use windows::core::GUID;
use windows::{
    Devices::Bluetooth::GenericAttributeProfile::{
        GattCharacteristicProperties, GattClientCharacteristicConfigurationDescriptorValue,
        GattCommunicationStatus,
    },
    Storage::Streams::{DataReader, IBuffer},
};

pub(crate) fn callback_result_reported(
    callback: impl FnOnce() -> windows::core::Result<()>,
    report_error: impl FnOnce(&windows::core::Error),
) -> windows::core::Result<()> {
    super::ble::callback_failure::invoke_reported(
        callback,
        || {
            windows::core::Error::new(
                windows::core::HRESULT(0x8000ffff_u32 as i32),
                "BLE callback panicked",
            )
        },
        report_error,
    )
}

pub fn to_error(status: GattCommunicationStatus) -> Result<()> {
    crate::windows_gatt_status::to_error(status.0)
}

pub fn to_descriptor_value(
    properties: GattCharacteristicProperties,
) -> GattClientCharacteristicConfigurationDescriptorValue {
    let notify = GattCharacteristicProperties::Notify;
    let indicate = GattCharacteristicProperties::Indicate;
    if properties & indicate == indicate {
        GattClientCharacteristicConfigurationDescriptorValue::Indicate
    } else if properties & notify == notify {
        GattClientCharacteristicConfigurationDescriptorValue::Notify
    } else {
        GattClientCharacteristicConfigurationDescriptorValue::None
    }
}

pub fn to_uuid(uuid: &GUID) -> Uuid {
    Uuid::from_fields(uuid.data1, uuid.data2, uuid.data3, &uuid.data4)
}

pub fn to_vec(buffer: &IBuffer) -> windows::core::Result<Vec<u8>> {
    let reader = DataReader::FromBuffer(buffer)?;
    let len = reader.UnconsumedBufferLength()? as usize;
    let mut data = vec![0u8; len];
    reader.ReadBytes(&mut data)?;
    Ok(data)
}

#[allow(dead_code)]
pub fn to_guid(uuid: &Uuid) -> GUID {
    let (data1, data2, data3, data4) = uuid.as_fields();
    GUID::from_values(data1, data2, data3, data4.to_owned())
}

pub fn to_char_props(props: &GattCharacteristicProperties) -> CharPropFlags {
    let mut flags = CharPropFlags::default();
    if *props & GattCharacteristicProperties::Broadcast != GattCharacteristicProperties::None {
        flags |= CharPropFlags::BROADCAST;
    }
    if *props & GattCharacteristicProperties::Read != GattCharacteristicProperties::None {
        flags |= CharPropFlags::READ;
    }
    if *props & GattCharacteristicProperties::WriteWithoutResponse
        != GattCharacteristicProperties::None
    {
        flags |= CharPropFlags::WRITE_WITHOUT_RESPONSE;
    }
    if *props & GattCharacteristicProperties::Write != GattCharacteristicProperties::None {
        flags |= CharPropFlags::WRITE;
    }
    if *props & GattCharacteristicProperties::Notify != GattCharacteristicProperties::None {
        flags |= CharPropFlags::NOTIFY;
    }
    if *props & GattCharacteristicProperties::Indicate != GattCharacteristicProperties::None {
        flags |= CharPropFlags::INDICATE;
    }
    if *props & GattCharacteristicProperties::AuthenticatedSignedWrites
        != GattCharacteristicProperties::None
    {
        flags |= CharPropFlags::AUTHENTICATED_SIGNED_WRITES;
    }
    if *props & GattCharacteristicProperties::ExtendedProperties
        != GattCharacteristicProperties::None
    {
        flags |= CharPropFlags::EXTENDED_PROPERTIES;
    }
    flags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use std::str::FromStr;

    #[test]
    fn null_device_projection_is_absent_only_after_successful_completion() {
        use windows::Devices::Bluetooth::BluetoothLEDevice;
        use windows_future::AsyncStatus;
        // A null interface is the documented not-found result. No pointer is dereferenced
        // and no COM object or Bluetooth API is created by this conversion.
        let missing = unsafe {
            <BluetoothLEDevice as windows::core::Type<BluetoothLEDevice>>::from_abi(
                std::ptr::null_mut(),
            )
        }
        .unwrap_err();
        assert_eq!(missing.code().0, 0);
        assert!(crate::windows_retrieval_match::is_missing_result(
            missing.code().0,
            Some(AsyncStatus::Completed.0),
            Some(0),
        ));
        for status in [
            AsyncStatus::Started,
            AsyncStatus::Canceled,
            AsyncStatus::Error,
        ] {
            assert!(!crate::windows_retrieval_match::is_missing_result(
                missing.code().0,
                Some(status.0),
                Some(0),
            ));
        }
        let denied =
            windows::core::Error::from_hresult(windows::core::HRESULT(0x80070005_u32 as i32));
        assert!(!crate::windows_retrieval_match::is_missing_result(
            denied.code().0,
            Some(AsyncStatus::Completed.0),
            Some(0),
        ));
    }

    #[test]
    fn retained_native_error_classifies_access_denied_without_parsing_text() {
        for (code, denied) in [(0x80070005_u32, true), (0x80004005_u32, false)] {
            let native = windows::core::Error::from_hresult(windows::core::HRESULT(code as i32));
            let error = Error::from(native);
            assert_eq!(error.is_permission_denied(), denied);
            assert_eq!(
                error.native_code().as_deref(),
                Some(format!("0x{code:08X}").as_str())
            );
            assert!(error.to_string().contains(&format!("{code:08X}")));
        }
        let opaque = Error::Other("permission denied 80070005".into());
        assert_eq!(opaque.native_code(), None);
        assert!(
            !opaque.is_permission_denied(),
            "Text must not impersonate a native cause"
        );
    }

    #[test]
    fn callback_boundary_preserves_hresult_and_maps_panic() {
        let code = windows::core::HRESULT(0x80070005_u32 as i32);
        let error =
            callback_result_reported(|| Err(windows::core::Error::from_hresult(code)), |_| {})
                .unwrap_err();
        assert_eq!(error.code(), code);
        let error = callback_result_reported(|| panic!("injected Windows callback panic"), |_| {})
            .unwrap_err();
        assert_eq!(error.code(), windows::core::HRESULT(0x8000ffff_u32 as i32));
    }

    #[test]
    fn protocol_failure_is_not_an_unsupported_capability() {
        assert!(matches!(
            to_error(GattCommunicationStatus::ProtocolError),
            Err(Error::Other(_))
        ));
        assert!(matches!(
            to_error(GattCommunicationStatus::AccessDenied),
            Err(Error::PermissionDenied)
        ));
        assert!(matches!(
            to_error(GattCommunicationStatus::Unreachable),
            Err(Error::NotConnected)
        ));
        assert!(to_error(GattCommunicationStatus::Success).is_ok());
        let protocol = to_error(GattCommunicationStatus::ProtocolError).unwrap_err();
        assert_eq!(
            protocol.native_code().as_deref(),
            Some("GattCommunicationStatus:2")
        );
        assert_eq!(protocol.to_string(), "GATT protocol error");
        let unknown = to_error(GattCommunicationStatus(-7)).unwrap_err();
        assert_eq!(
            unknown.native_code().as_deref(),
            Some("GattCommunicationStatus:-7")
        );
        assert_eq!(unknown.to_string(), "Communication Error:");
    }

    #[test]
    fn check_uuid_to_guid_conversion() {
        let uuid_str = "10B201FF-5B3B-45A1-9508-CF3EFCD7BBAF";
        let uuid = Uuid::from_str(uuid_str).unwrap();

        let guid_converted = to_guid(&uuid);

        let guid_expected = GUID::try_from(uuid_str).unwrap();
        assert_eq!(guid_converted, guid_expected);
    }

    #[test]
    fn check_guid_to_uuid_conversion() {
        let uuid_str = "10B201FF-5B3B-45A1-9508-CF3EFCD7BBAF";
        let guid = GUID::try_from(uuid_str).unwrap();

        let uuid_converted = to_uuid(&guid);

        let uuid_expected = Uuid::from_str(uuid_str).unwrap();
        assert_eq!(uuid_converted, uuid_expected);
    }
}
