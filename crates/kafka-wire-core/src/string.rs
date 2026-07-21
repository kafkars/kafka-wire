//! Owned, byte-backed, validated Kafka protocol string.
//!
//! The value keeps decoded `Bytes` zero-copy while exposing only UTF-8 views.
//! It deliberately owns no length-prefix policy; encoders and decoders decide
//! which Kafka string regime surrounds these bytes.

use std::{fmt, ops::Deref, str::Utf8Error};

use bytes::Bytes;

/// Owned UTF-8 bytes used by generated Kafka messages.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StrBytes(Bytes);

impl StrBytes {
    /// Returns the validated value as `str`.
    pub fn as_str(&self) -> &str {
        let Ok(value) = std::str::from_utf8(&self.0) else {
            unreachable!("StrBytes construction validates UTF-8")
        };
        value
    }

    /// Returns the UTF-8 bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the number of UTF-8 bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the string is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes the value and returns an owned `String`.
    pub fn into_string(self) -> String {
        self.as_str().to_owned()
    }

    /// Consumes the value and returns its validated byte storage.
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl TryFrom<Bytes> for StrBytes {
    type Error = Utf8Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(&value)?;
        Ok(Self(value))
    }
}

impl From<&str> for StrBytes {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<String> for StrBytes {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl AsRef<str> for StrBytes {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for StrBytes {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for StrBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}
