//! Owned CoreBluetooth diagnostics; no NSError escapes its delegate callback.
use objc2_foundation::NSError;
use std::fmt;

#[derive(Clone, Debug)]
pub(crate) struct NativeError {
    pub(crate) message: String,
    pub(crate) native_code: Option<String>,
}

impl NativeError {
    pub(crate) fn local(message: String) -> Self {
        Self {
            message,
            native_code: None,
        }
    }

    pub(crate) fn copy(error: &NSError) -> Self {
        Self {
            message: error.localizedDescription().to_string(),
            native_code: Some(format!("{}:{}", error.domain(), error.code())),
        }
    }
}

impl fmt::Display for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Preserve the previous RuntimeError display text.
        write!(f, "Runtime Error: {}", self.message)
    }
}
impl std::error::Error for NativeError {}

#[cfg(test)]
mod tests {
    use super::*;
    use objc2_foundation::NSString;

    #[test]
    fn copied_domain_code_and_message_outlive_nserror() {
        for (domain, code) in [
            ("CBATTErrorDomain", 10),
            ("OtherDomain", 10),
            ("CBErrorDomain", -1),
            ("ControlledDomain", isize::MAX),
        ] {
            let native = NSError::new(code, &NSString::from_str(domain));
            let message = native.localizedDescription().to_string();
            let copied = NativeError::copy(&native);
            drop(native);
            let error = crate::Error::Other(Box::new(copied));
            let expected = format!("{domain}:{code}");
            assert_eq!(error.native_code().as_deref(), Some(expected.as_str()));
            assert_eq!(error.to_string(), format!("Runtime Error: {message}"));
            assert!(!error.is_permission_denied());
            let owned = error.native_code().unwrap();
            drop(error);
            assert_eq!(std::thread::spawn(move || owned).join().unwrap(), expected);
        }
    }

    #[test]
    fn formatted_apple_codes_do_not_impersonate_native_causes() {
        for error in [
            crate::Error::RuntimeError("CBATTErrorDomain:10".into()),
            crate::Error::Other("CBATTErrorDomain:10".into()),
            crate::Error::Other(Box::new(NativeError::local("CBATTErrorDomain:10".into()))),
        ] {
            assert_eq!(error.native_code(), None);
        }
    }
}
