//! Checked WinRT GATT statuses retain portable classification and diagnostics.
use crate::{Error, Result};
use std::fmt;

#[derive(Debug)]
pub(crate) struct GattStatusError(i32);

impl GattStatusError {
    pub(crate) fn native_code(&self) -> String {
        format!("GattCommunicationStatus:{}", self.0)
    }
}

impl fmt::Display for GattStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 2 {
            formatter.write_str("GATT protocol error")
        } else {
            formatter.write_str("Communication Error:")
        }
    }
}

impl std::error::Error for GattStatusError {}

pub(crate) fn to_error(status: i32) -> Result<()> {
    // Values are defined by the pinned WinRT GattCommunicationStatus binding.
    match status {
        0 => Ok(()),
        1 => Err(Error::NotConnected),
        3 => Err(Error::PermissionDenied),
        status => Err(Error::Other(Box::new(GattStatusError(status)))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_preserve_success_permission_and_connection_classification() {
        assert!(to_error(0).is_ok());
        assert!(matches!(to_error(1), Err(Error::NotConnected)));
        assert!(matches!(to_error(3), Err(Error::PermissionDenied)));
    }

    #[test]
    fn protocol_and_unknown_statuses_keep_owned_codes_without_claiming_unsupported() {
        for status in [2, 4, 99, -1, i32::MIN, i32::MAX] {
            let error = to_error(status).unwrap_err();
            assert!(!error.is_permission_denied());
            let Error::Other(cause) = error else {
                panic!("GATT status lost its failure class")
            };
            let cause = cause.downcast_ref::<GattStatusError>().unwrap();
            let code = cause.native_code();
            assert_eq!(code, format!("GattCommunicationStatus:{status}"));
            assert_eq!(
                cause.to_string(),
                if status == 2 {
                    "GATT protocol error"
                } else {
                    "Communication Error:"
                }
            );
            assert_eq!(
                std::thread::spawn(move || code).join().unwrap(),
                format!("GattCommunicationStatus:{status}")
            );
        }
    }
}
