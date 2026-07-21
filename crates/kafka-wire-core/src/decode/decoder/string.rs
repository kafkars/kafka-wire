//! Legacy and compact UTF-8 string decoding with explicit size limits.

use crate::StrBytes;

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads a legacy non-null string.
    pub fn read_string(&mut self) -> Result<StrBytes, DecodeError> {
        let offset = self.offset();
        let length = self.read_i16()?;
        if length < 0 {
            return Err(DecodeError::NegativeLength {
                kind: "string",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "string",
            offset,
        })?;
        self.read_string_payload(length, offset)
    }

    /// Reads a legacy nullable string.
    pub fn read_nullable_string(&mut self) -> Result<Option<StrBytes>, DecodeError> {
        let offset = self.offset();
        let length = self.read_i16()?;
        if length == -1 {
            return Ok(None);
        }
        if length < -1 {
            return Err(DecodeError::NegativeLength {
                kind: "nullable string",
                length: i64::from(length),
                offset,
            });
        }

        let length = usize::try_from(length).map_err(|_| DecodeError::LengthOverflow {
            kind: "nullable string",
            offset,
        })?;
        self.read_string_payload(length, offset).map(Some)
    }

    /// Reads a compact non-null string.
    pub fn read_compact_string(&mut self) -> Result<StrBytes, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Err(DecodeError::NullNotAllowed {
                kind: "compact string",
                offset,
            });
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact string",
            offset,
        })?;
        self.read_string_payload(length, offset)
    }

    /// Reads a compact nullable string.
    pub fn read_compact_nullable_string(&mut self) -> Result<Option<StrBytes>, DecodeError> {
        let offset = self.offset();
        let encoded = self.read_unsigned_varint()?;
        if encoded == 0 {
            return Ok(None);
        }

        let length = usize::try_from(encoded - 1).map_err(|_| DecodeError::LengthOverflow {
            kind: "compact nullable string",
            offset,
        })?;
        self.read_string_payload(length, offset).map(Some)
    }

    fn read_string_payload(
        &mut self,
        length: usize,
        prefix_offset: usize,
    ) -> Result<StrBytes, DecodeError> {
        Self::check_limit(
            "string",
            length,
            self.limits.max_string_bytes,
            prefix_offset,
        )?;
        let payload_offset = self.offset();
        let bytes = self.take(length)?;
        StrBytes::try_from(bytes).map_err(|error| DecodeError::InvalidUtf8 {
            offset: payload_offset,
            valid_up_to: error.valid_up_to(),
        })
    }
}
