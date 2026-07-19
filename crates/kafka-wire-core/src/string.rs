//! Owned validated Kafka protocol string.
//!
//! The public abstraction is intentionally opaque so its storage can evolve
//! independently from generated DTOs.

use std::{fmt, ops::Deref};

/// Owned UTF-8 string used by generated Kafka messages.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StrBytes(String);

impl StrBytes {
    /// Returns the value as `str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the UTF-8 bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns the number of UTF-8 bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the string is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes the value and returns its owned `String`.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<&str> for StrBytes {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for StrBytes {
    fn from(value: String) -> Self {
        Self(value)
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
        self.0.fmt(formatter)
    }
}
