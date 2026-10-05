//! Device lookup must not turn bus/permission/metadata failures into absence.
use crate::Error;

/// An object can disappear between a signal and its metadata lookup. Only that
/// absence retires the event; all other causes must reach the adapter consumer.
pub(crate) fn event_lookup_error(error: Error) -> Option<crate::api::CentralEvent> {
    match error {
        Error::DeviceNotFound => None,
        error => Some(crate::api::CentralEvent::AdapterError {
            message: error.to_string(),
        }),
    }
}

pub(crate) fn device_lookup_error(
    error: impl std::error::Error + Send + Sync + 'static,
    dbus_name: Option<&str>,
) -> Error {
    if dbus_name == Some("org.freedesktop.DBus.Error.UnknownObject") {
        Error::DeviceNotFound
    } else {
        Error::Other(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Cause(&'static str);
    impl std::fmt::Display for Cause {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.0)
        }
    }
    impl std::error::Error for Cause {}

    #[test]
    fn stale_object_events_retire_but_other_lookup_failures_are_published() {
        assert!(event_lookup_error(Error::DeviceNotFound).is_none());
        for name in [
            None,
            Some("org.freedesktop.DBus.Error.AccessDenied"),
            Some("org.freedesktop.DBus.Error.Disconnected"),
            Some("org.freedesktop.DBus.Error.UnknownMethod"),
        ] {
            let error = device_lookup_error(Cause("native event lookup detail"), name);
            assert!(matches!(event_lookup_error(error),
                Some(crate::api::CentralEvent::AdapterError { message })
                if message.contains("native event lookup detail")));
        }
        assert!(
            event_lookup_error(device_lookup_error(
                Cause("removed"),
                Some("org.freedesktop.DBus.Error.UnknownObject")
            ))
            .is_none()
        );
    }

    #[test]
    fn only_unknown_object_means_device_not_found() {
        assert!(matches!(
            device_lookup_error(
                Cause("object removed"),
                Some("org.freedesktop.DBus.Error.UnknownObject")
            ),
            Error::DeviceNotFound
        ));
    }

    #[test]
    fn bus_permission_method_and_interface_failures_preserve_original_cause() {
        for name in [
            "NoReply",
            "Disconnected",
            "AccessDenied",
            "ServiceUnknown",
            "UnknownMethod",
            "UnknownInterface",
        ] {
            let name = format!("org.freedesktop.DBus.Error.{name}");
            let error = device_lookup_error(Cause("original native detail"), Some(&name));
            match error {
                Error::Other(error) => {
                    assert_eq!(error.to_string(), "original native detail");
                    assert_eq!(
                        error.downcast_ref::<Cause>().unwrap().0,
                        "original native detail"
                    );
                }
                error => panic!("lookup lost the cause: {error:?}"),
            }
        }
    }

    #[test]
    fn metadata_and_lookalike_errors_are_not_misclassified() {
        for name in [
            None,
            Some("UnknownObject"),
            Some("org.example.UnknownObject"),
            Some("org.freedesktop.DBus.Error.UnknownObjectExtra"),
        ] {
            assert!(
                matches!(device_lookup_error(Cause("metadata detail"), name),
                Error::Other(error) if error.to_string() == "metadata detail")
            );
        }
    }
}
