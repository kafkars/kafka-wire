//! Legacy and compact byte-string decoding.
//!
//! This module owns the length-prefixed raw-byte readers (`BYTES` and
//! `COMPACT_BYTES`, each with a nullable form). Each returns a zero-copy
//! `Bytes` slice of the input cursor, so no payload is heap-copied. The claimed
//! length is bounded by the bytes that remain (via `take`) before slicing, so a
//! peer cannot name more bytes than the frame carried. A distinct byte-field
//! budget also prevents a peer from retaining an unexpectedly large slice of a
//! permitted outer frame.

use bytes::Bytes;

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads a legacy non-null byte string: an `int32` length then the bytes.
    pub fn read_bytes(&mut self) -> Result<Bytes, DecodeError> {
        let offset = self.offset();
        let length = self.read_i32()?;
        if length < 0 {
            return Err(DecodeError::NegativeLength {
                kind: "bytes",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "bytes",
            offset,
        })?;
        Self::check_limit("bytes", length, self.limits.max_bytes_bytes, offset)?;
        self.take(length)
    }

    /// Reads a legacy nullable byte string; the `int32` `-1` decodes to `None`.
    pub fn read_nullable_bytes(&mut self) -> Result<Option<Bytes>, DecodeError> {
        let offset = self.offset();
        let length = self.read_i32()?;
        if length == -1 {
            return Ok(None);
        }
        if length < -1 {
            return Err(DecodeError::NegativeLength {
                kind: "nullable bytes",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "nullable bytes",
            offset,
        })?;
        Self::check_limit(
            "nullable bytes",
            length,
            self.limits.max_bytes_bytes,
            offset,
        )?;
        self.take(length).map(Some)
    }

    /// Reads a compact non-null byte string: `unsigned varint(len + 1)` then the
    /// bytes.
    pub fn read_compact_bytes(&mut self) -> Result<Bytes, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Err(DecodeError::NullNotAllowed {
                kind: "compact bytes",
                offset,
            });
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact bytes",
            offset,
        })?;
        Self::check_limit("compact bytes", length, self.limits.max_bytes_bytes, offset)?;
        self.take(length)
    }

    /// Reads a compact nullable byte string; the varint `0` decodes to `None`.
    pub fn read_compact_nullable_bytes(&mut self) -> Result<Option<Bytes>, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Ok(None);
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact nullable bytes",
            offset,
        })?;
        Self::check_limit(
            "compact nullable bytes",
            length,
            self.limits.max_bytes_bytes,
            offset,
        )?;
        self.take(length).map(Some)
    }
}
