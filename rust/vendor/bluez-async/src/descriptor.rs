use dbus::Path;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

use crate::CharacteristicId;

/// Opaque identifier for a GATT characteristic descriptor on a Bluetooth device.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct DescriptorId {
    #[serde(with = "crate::serde_path")]
    pub(crate) object_path: Path<'static>,
}

impl DescriptorId {
    #[cfg(test)]
    pub(crate) fn new(object_path: &str) -> Self {
        Self {
            object_path: object_path.to_owned().into(),
        }
    }

    /// Get the ID of the characteristic on which this descriptor was advertised.
    pub fn characteristic(&self) -> CharacteristicId {
        CharacteristicId::new(crate::object_path::parent(&self.object_path))
    }
}

impl From<DescriptorId> for Path<'static> {
    fn from(id: DescriptorId) -> Self {
        id.object_path
    }
}

impl Display for DescriptorId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", crate::object_path::display(&self.object_path))
    }
}

/// Information about a GATT descriptor on a Bluetooth device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorInfo {
    /// An opaque identifier for the descriptor on the device, including a reference to which
    /// adapter it was discovered on.
    pub id: DescriptorId,
    /// The 128-bit UUID of the descriptor.
    pub uuid: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shallow_and_opaque_paths_format_without_panicking() {
        for path in ["/", "/node", "/custom/adapter/device"] {
            let id = DescriptorId::new(path);
            assert_eq!(id.to_string(), path);
            assert_eq!(
                id.characteristic(),
                CharacteristicId::new(if path == "/custom/adapter/device" {
                    "/custom/adapter"
                } else {
                    "/"
                })
            );
        }
    }

    #[test]
    fn descriptor_characteristic() {
        let characteristic_id =
            CharacteristicId::new("/org/bluez/hci0/dev_11_22_33_44_55_66/service0022/char0033");
        let descriptor_id = DescriptorId::new(
            "/org/bluez/hci0/dev_11_22_33_44_55_66/service0022/char0033/desc0034",
        );
        assert_eq!(descriptor_id.characteristic(), characteristic_id);
    }

    #[test]
    fn to_string() {
        let descriptor_id = DescriptorId::new(
            "/org/bluez/hci0/dev_11_22_33_44_55_66/service0022/char0033/desc0034",
        );
        assert_eq!(
            descriptor_id.to_string(),
            "hci0/dev_11_22_33_44_55_66/service0022/char0033/desc0034"
        );
    }
}
