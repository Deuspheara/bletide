use dbus::Path;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

use crate::DeviceId;

/// Opaque identifier for a GATT service on a Bluetooth device.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ServiceId {
    #[serde(with = "crate::serde_path")]
    pub(crate) object_path: Path<'static>,
}

impl ServiceId {
    pub(crate) fn new(object_path: &str) -> Self {
        Self {
            object_path: object_path.to_owned().into(),
        }
    }

    /// Get the ID of the device on which this service was advertised.
    pub fn device(&self) -> DeviceId {
        DeviceId::new(crate::object_path::parent(&self.object_path))
    }
}

impl From<ServiceId> for Path<'static> {
    fn from(id: ServiceId) -> Self {
        id.object_path
    }
}

impl Display for ServiceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", crate::object_path::display(&self.object_path))
    }
}

/// Information about a GATT service on a Bluetooth device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceInfo {
    /// An opaque identifier for the service on the device, including a reference to which adapter
    /// it was discovered on.
    pub id: ServiceId,
    /// The 128-bit UUID of the service.
    pub uuid: Uuid,
    /// Whether this GATT service is a primary service.
    pub primary: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shallow_and_opaque_paths_format_without_panicking() {
        for path in ["/", "/node", "/custom/adapter/device"] {
            let id = ServiceId::new(path);
            assert_eq!(id.to_string(), path);
            assert_eq!(
                id.device(),
                DeviceId::new(if path == "/custom/adapter/device" {
                    "/custom/adapter"
                } else {
                    "/"
                })
            );
        }
    }

    #[test]
    fn service_device() {
        let device_id = DeviceId::new("/org/bluez/hci0/dev_11_22_33_44_55_66");
        let service_id = ServiceId::new("/org/bluez/hci0/dev_11_22_33_44_55_66/service0022");
        assert_eq!(service_id.device(), device_id);
    }

    #[test]
    fn to_string() {
        let service_id = ServiceId::new("/org/bluez/hci0/dev_11_22_33_44_55_66/service0022");
        assert_eq!(
            service_id.to_string(),
            "hci0/dev_11_22_33_44_55_66/service0022"
        );
    }
}
