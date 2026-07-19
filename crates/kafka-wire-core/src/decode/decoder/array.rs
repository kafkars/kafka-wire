//! Legacy and compact non-null array length decoding.

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads and validates a legacy non-null array length.
    pub fn read_array_len(&mut self) -> Result<usize, DecodeError> {
        let offset = self.offset();
        let length = self.read_i32()?;
        if length < 0 {
            return Err(DecodeError::NegativeLength {
                kind: "array",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "array",
            offset,
        })?;
        Self::check_limit("array", length, self.limits.max_array_elements, offset)?;
        Ok(length)
    }

    /// Reads and validates a compact non-null array length.
    pub fn read_compact_array_len(&mut self) -> Result<usize, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Err(DecodeError::NullNotAllowed {
                kind: "compact array",
                offset,
            });
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact array",
            offset,
        })?;
        Self::check_limit(
            "compact array",
            length,
            self.limits.max_array_elements,
            offset,
        )?;
        Ok(length)
    }
}
