//! Fixed-width UUID encoding.
//!
//! This module owns the sixteen-byte big-endian UUID writer. The value is a
//! fixed-width primitive with no length prefix, so it routes straight to the
//! raw-slice path.

use crate::Uuid;

use super::{EncodeError, EncodeTarget, Encoder};

impl<T: EncodeTarget> Encoder<T> {
    /// Writes a UUID as sixteen big-endian bytes.
    #[inline]
    pub fn write_uuid(&mut self, value: Uuid) -> Result<(), EncodeError> {
        self.write_raw_slice(value.as_bytes())
    }
}
