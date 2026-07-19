//! Owned 16-byte Kafka UUID.
//!
//! This module owns the fixed-width UUID value carried as sixteen big-endian
//! bytes and its all-zero sentinel. It deliberately adds no textual parsing,
//! formatting, or version policy beyond the raw bytes; those belong above the
//! wire kernel.

/// A Kafka UUID: sixteen big-endian bytes.
///
/// The wire form is exactly the sixteen bytes in order, so the newtype stores
/// them verbatim rather than a parsed integer pair.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// The all-zero UUID.
    ///
    /// Kafka schemas use this value as the conventional default and as the
    /// "unset" sentinel for many UUID fields, so it is exposed as a constant
    /// rather than reconstructed at each call site.
    pub const ZERO: Self = Self([0; 16]);

    /// Creates a UUID from its sixteen big-endian bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the sixteen big-endian bytes by value.
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Returns a reference to the sixteen big-endian bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns whether this is the all-zero sentinel.
    pub fn is_zero(&self) -> bool {
        self.0 == [0; 16]
    }
}
