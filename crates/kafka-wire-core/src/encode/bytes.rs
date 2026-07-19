//! Legacy and compact byte-string encoding.
//!
//! This module owns the length-prefixed raw-byte writers (`BYTES` and
//! `COMPACT_BYTES`, each with a nullable form). It deliberately owns no payload
//! interpretation: the bytes are written verbatim, and framing above them (for
//! example a record batch) belongs to the caller that supplied the slice.

use super::encoder::compact_length;
use super::{EncodeError, EncodeTarget, Encoder};

impl<T: EncodeTarget> Encoder<T> {
    /// Writes a legacy non-null byte string: an `int32` length then the bytes.
    pub fn write_bytes(&mut self, value: &[u8]) -> Result<(), EncodeError> {
        let length = i32::try_from(value.len()).map_err(|_| EncodeError::LengthOverflow {
            kind: "bytes",
            length: value.len(),
            maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
        })?;
        self.write_i32(length)?;
        self.write_raw_slice(value)
    }

    /// Writes a legacy nullable byte string; `None` emits the `int32` `-1`.
    pub fn write_nullable_bytes(&mut self, value: Option<&[u8]>) -> Result<(), EncodeError> {
        match value {
            Some(value) => self.write_bytes(value),
            None => self.write_i32(-1),
        }
    }

    /// Writes a compact non-null byte string: `unsigned varint(len + 1)` then
    /// the bytes.
    pub fn write_compact_bytes(&mut self, value: &[u8]) -> Result<(), EncodeError> {
        let length = compact_length(value.len(), "compact bytes")?;
        self.write_unsigned_varint(length)?;
        self.write_raw_slice(value)
    }

    /// Writes a compact nullable byte string; `None` emits the varint `0`.
    pub fn write_compact_nullable_bytes(
        &mut self,
        value: Option<&[u8]>,
    ) -> Result<(), EncodeError> {
        match value {
            Some(value) => self.write_compact_bytes(value),
            None => self.write_unsigned_varint(0),
        }
    }
}
