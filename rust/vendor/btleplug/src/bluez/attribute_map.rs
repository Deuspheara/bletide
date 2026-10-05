//! Stage UUID-addressed discovery before publishing its cache.
use crate::{Error, Result};
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) fn unique_attributes<T>(
    attributes: Result<impl IntoIterator<Item = (Uuid, T)>>,
    kind: &str,
) -> Result<HashMap<Uuid, T>> {
    let mut result = HashMap::new();
    for (uuid, value) in attributes? {
        if result.insert(uuid, value).is_some() {
            return Err(Error::NotSupported(format!("Ambiguous {kind} UUID {uuid}")));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_rejects_duplicate_identity_at_each_attribute_scope() {
        let uuid = Uuid::from_u128(123);
        for scope in ["service", "characteristic", "descriptor"] {
            let result = unique_attributes(Ok([(uuid, 1), (uuid, 2)]), scope);
            assert!(matches!(result, Err(Error::NotSupported(message)) if message.contains(scope)));
        }
    }

    #[test]
    fn descriptor_failure_is_preserved_instead_of_an_empty_discovery() {
        for error in [
            Error::PermissionDenied,
            Error::RuntimeError("BlueZ descriptor fetch failed".into()),
        ] {
            let permission = matches!(error, Error::PermissionDenied);
            let result = unique_attributes::<u8>(Err::<Vec<(Uuid, u8)>, _>(error), "descriptor");
            if permission {
                assert!(matches!(result, Err(Error::PermissionDenied)));
            } else {
                assert!(
                    matches!(result, Err(Error::RuntimeError(message)) if message == "BlueZ descriptor fetch failed")
                );
            }
        }
    }

    #[test]
    fn distinct_attributes_and_empty_descriptors_are_valid() {
        let a = Uuid::from_u128(123);
        let b = Uuid::from_u128(456);
        let result = unique_attributes(Ok([(a, 1), (b, 2)]), "characteristic").unwrap();
        assert_eq!(result[&a], 1);
        assert_eq!(result[&b], 2);
        // Separate parent scopes can legitimately reuse the same UUID.
        let other = unique_attributes(Ok([(a, 3)]), "characteristic").unwrap();
        assert_eq!(other[&a], 3);
        assert_eq!(result[&a], 1);
        assert!(
            unique_attributes::<u8>(Ok([]), "descriptor")
                .unwrap()
                .is_empty()
        );
    }
}
