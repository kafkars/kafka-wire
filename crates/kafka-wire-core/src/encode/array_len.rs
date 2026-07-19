//! Legacy and compact array-length encoding.
//!
//! This module owns the count prefix that precedes an array's elements, in both
//! the non-null and nullable forms of each encoding. It deliberately owns no
//! element encoding: the caller writes the count here, then the elements. The
//! reader-side counterpart is `decode::decoder::array`.

use super::encoder::compact_length;
use super::{EncodeError, EncodeTarget, Encoder};

impl<T: EncodeTarget> Encoder<T> {
    /// Writes a legacy non-null array length as an `int32`.
    pub fn write_array_len(&mut self, length: usize) -> Result<(), EncodeError> {
        let length = i32::try_from(length).map_err(|_| EncodeError::LengthOverflow {
            kind: "array",
            length,
            maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
        })?;
        self.write_i32(length)
    }

    /// Writes a compact non-null array length as `unsigned varint(len + 1)`.
    pub fn write_compact_array_len(&mut self, length: usize) -> Result<(), EncodeError> {
        let length = compact_length(length, "compact array")?;
        self.write_unsigned_varint(length)
    }

    /// Writes a legacy nullable array length; `None` emits the `int32` `-1`.
    pub fn write_nullable_array_len(&mut self, length: Option<usize>) -> Result<(), EncodeError> {
        match length {
            Some(length) => self.write_array_len(length),
            None => self.write_i32(-1),
        }
    }

    /// Writes a compact nullable array length; `None` emits the varint `0`.
    pub fn write_compact_nullable_array_len(
        &mut self,
        length: Option<usize>,
    ) -> Result<(), EncodeError> {
        match length {
            Some(length) => self.write_compact_array_len(length),
            None => self.write_unsigned_varint(0),
        }
    }
}
