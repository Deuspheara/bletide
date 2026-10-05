//! A D-Bus introspection child has a nonempty relative object path.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NameError {
    Missing,
    Invalid,
}

pub(crate) fn validate(name: Option<&str>) -> Result<&str, NameError> {
    let name = name.ok_or(NameError::Missing)?;
    if !name.split('/').all(|component| {
        !component.is_empty()
            && component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }) {
        return Err(NameError::Invalid);
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_child_name_is_a_controlled_error() {
        assert_eq!(validate(None), Err(NameError::Missing));
    }

    #[test]
    fn invalid_child_names_are_rejected_before_path_construction() {
        for prefix in ["service", "char", "desc"] {
            for suffix in ["//child", "/", "-1", ".1", " child", "é", ":1", "\0"] {
                let name = format!("{prefix}{suffix}");
                assert_eq!(validate(Some(&name)), Err(NameError::Invalid), "{name:?}");
            }
        }
        for name in ["", "/service0001", "../char0001"] {
            assert_eq!(validate(Some(name)), Err(NameError::Invalid));
        }
    }

    #[test]
    fn valid_names_including_unrelated_siblings_are_preserved() {
        for name in [
            "service0001",
            "char0002",
            "desc0003",
            "other_1",
            "_",
            "123",
            "service0001/child_2",
        ] {
            assert_eq!(validate(Some(name)), Ok(name));
        }
    }
}
