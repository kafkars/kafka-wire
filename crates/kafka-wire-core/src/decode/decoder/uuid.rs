//! Fixed-width UUID decoding.
//!
//! This module owns the sixteen-byte big-endian UUID reader. The value has no
//! length prefix, so it takes exactly sixteen bytes bounded by the remainder and
//! copies them into the owned newtype.

use crate::Uuid;

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads a UUID from sixteen big-endian bytes.
    pub fn read_uuid(&mut self) -> Result<Uuid, DecodeError> {
        let bytes = self.take(16)?;
        let mut raw = [0_u8; 16];
        raw.copy_from_slice(&bytes);
        Ok(Uuid::from_bytes(raw))
    }
}
