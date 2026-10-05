//! Versioned little-endian binary wire format, never JSON.
//! Event: kind:u32, request:u64, code:u32, payload:remaining bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Error {
    pub code: u32,
    pub message: String,
    pub native_code: Option<String>,
}

impl Error {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            native_code: None,
        }
    }
    pub fn with_native_code(mut self, code: impl Into<String>) -> Self {
        let code = code.into();
        if !code.is_empty() {
            self.native_code = Some(code);
        }
        self
    }
}

impl From<btleplug::Error> for Error {
    fn from(error: btleplug::Error) -> Self {
        let code = match &error {
            _ if error.is_permission_denied() => 3,
            btleplug::Error::NoAdapterAvailable => 1,
            btleplug::Error::DeviceNotFound => 4,
            btleplug::Error::NotConnected => 8,
            btleplug::Error::TimedOut(_) => 9,
            btleplug::Error::NotSupported(_) => 11,
            btleplug::Error::NoSuchCharacteristic => 13,
            _ => 15,
        };
        let native_code = error.native_code();
        let mapped = Self::new(code, error.to_string());
        match native_code {
            Some(native_code) => mapped.with_native_code(native_code),
            None => mapped,
        }
    }
}

pub(crate) fn event(kind: u32, request: u64, result: Result<Vec<u8>, Error>) -> Vec<u8> {
    let (code, payload) = match result {
        Ok(bytes) => (0, bytes),
        Err(error) => match error.native_code {
            None => (error.code, error.message.into_bytes()),
            Some(native_code) => {
                let mut payload = Vec::new();
                let Ok(length) = u32::try_from(native_code.len()) else {
                    return event(
                        kind,
                        request,
                        Err(Error::new(18, "Native error code exceeds wire limit")),
                    );
                };
                payload.extend_from_slice(&length.to_le_bytes());
                payload.extend_from_slice(native_code.as_bytes());
                payload.extend_from_slice(error.message.as_bytes());
                (error.code | 0x8000_0000, payload)
            }
        },
    };
    let mut bytes = Vec::with_capacity(16 + payload.len());
    bytes.extend_from_slice(&kind.to_le_bytes());
    bytes.extend_from_slice(&request.to_le_bytes());
    bytes.extend_from_slice(&code.to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes
}

/// Reject malformed commands before looking up or touching a platform object.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    pub fn take(&mut self, count: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Error::new(16, "Truncated native command"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
    pub fn u64(&mut self) -> Result<u64, Error> {
        let mut value = [0; 8];
        value.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(value))
    }
    pub fn uuid(&mut self) -> Result<uuid::Uuid, Error> {
        uuid::Uuid::from_slice(self.take(16)?).map_err(|_| Error::new(16, "Invalid command UUID"))
    }
    pub fn remaining(&mut self) -> &'a [u8] {
        let value = &self.bytes[self.offset..];
        self.offset = self.bytes.len();
        value
    }
    pub fn finish(&self) -> Result<(), Error> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::new(16, "Unexpected command payload"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arbitrary_binary_is_preserved() {
        for len in 0..1024 {
            let bytes = (0..len).map(|n| n as u8).collect::<Vec<_>>();
            assert_eq!(&event(1, 42, Ok(bytes.clone()))[16..], bytes);
        }
    }
    #[test]
    fn error_mapping_is_portable() {
        assert_eq!(Error::from(btleplug::Error::PermissionDenied).code, 3);
        assert_eq!(Error::from(btleplug::Error::NotConnected).code, 8);
    }
}

#[cfg(test)]
mod native_error_tests {
    use super::*;
    #[test]
    fn native_code_error_matches_shared_fixture_and_plain_errors_keep_layout() {
        let encoded = event(
            1,
            41,
            Err(Error::new(19, "Controlled native platform failure").with_native_code("133")),
        );
        assert_eq!(
            encoded,
            include_bytes!("../../test/fixtures/native_error.bin")
        );
        let plain = event(1, 41, Err(Error::new(19, "original cause")));
        assert_eq!(&plain[12..16], &19u32.to_le_bytes());
        assert_eq!(&plain[16..], b"original cause");
    }
}
