//! Pure path handling for already validated D-Bus paths. Root/shallow paths
//! must never create an empty path or make opaque-ID formatting fail.

pub(crate) fn parent(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some(("", _)) | None => "/",
        Some((parent, _)) => parent,
    }
}

pub(crate) fn display(path: &str) -> &str {
    path.strip_prefix("/org/bluez/").unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_and_shallow_parents_remain_valid_paths() {
        assert_eq!(parent("/"), "/");
        assert_eq!(parent("/node"), "/");
        assert_eq!(parent("/custom/node"), "/custom");
    }

    #[test]
    fn normal_bluez_identity_and_hierarchy_are_unchanged() {
        let path = "/org/bluez/hci0/dev_11_22_33_44_55_66/service0001/char0002/desc0003";
        assert_eq!(
            display(path),
            "hci0/dev_11_22_33_44_55_66/service0001/char0002/desc0003"
        );
        assert_eq!(
            parent(path),
            "/org/bluez/hci0/dev_11_22_33_44_55_66/service0001/char0002"
        );
    }

    #[test]
    fn opaque_paths_outside_expected_prefix_are_preserved() {
        for path in ["/", "/node", "/org/bluez", "/custom/adapter/device"] {
            assert_eq!(display(path), path);
        }
    }
}
