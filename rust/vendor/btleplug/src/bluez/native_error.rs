//! Preserve structured D-Bus names without parsing diagnostic messages.
use bluez_async::BluetoothError;
use std::error::Error;

fn name<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a str> {
    if let Some(BluetoothError::DbusSetupError(error)) = error.downcast_ref::<BluetoothError>() {
        return error.source().and_then(name);
    }
    if let Some(BluetoothError::DbusError(error)) = error.downcast_ref::<BluetoothError>() {
        return error.name();
    }
    error
        .downcast_ref::<dbus::Error>()
        .and_then(dbus::Error::name)
}

pub(crate) fn native_code(error: &(dyn Error + 'static)) -> Option<String> {
    name(error).map(str::to_owned)
}

pub(crate) fn is_permission_denied(error: &(dyn Error + 'static)) -> bool {
    name(error) == Some("org.freedesktop.DBus.Error.AccessDenied")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_names_survive_owner_drop_and_preserve_unrelated_errors() {
        for (name, denied) in [
            ("org.freedesktop.DBus.Error.AccessDenied", true),
            ("org.freedesktop.DBus.Error.AccessDeniedExtra", false),
            ("org.freedesktop.DBus.Error.UnknownObject", false),
            ("org.bluez.Error.Failed", false),
            ("org.bluez.Error.NotPermitted", false),
        ] {
            for wrapped in 0..3 {
                let native = dbus::Error::new_custom(name, "original platform cause: 100%");
                let error: Box<dyn Error + Send + Sync> = match wrapped {
                    0 => Box::new(native),
                    1 => Box::new(BluetoothError::from(native)),
                    _ => Box::new(BluetoothError::DbusSetupError(native.into())),
                };
                assert_eq!(is_permission_denied(error.as_ref()), denied);
                let code = native_code(error.as_ref()).unwrap();
                assert_eq!(code, name);
                assert!(error.to_string().contains("original platform cause: 100%"));
                drop(error);
                assert_eq!(std::thread::spawn(move || code).join().unwrap(), name);
            }
        }
    }
    #[test]
    fn formatted_messages_and_non_dbus_errors_do_not_impersonate_names() {
        let errors: Vec<Box<dyn Error + Send + Sync>> = vec![
            Box::new(std::io::Error::other(
                "org.freedesktop.DBus.Error.AccessDenied",
            )),
            Box::new(BluetoothError::FlagParseError(
                "org.freedesktop.DBus.Error.AccessDenied".into(),
            )),
            Box::new(BluetoothError::NoBluetoothAdapters),
        ];
        for error in errors {
            assert_eq!(native_code(error.as_ref()), None);
            assert!(!is_permission_denied(error.as_ref()));
        }
    }
}
