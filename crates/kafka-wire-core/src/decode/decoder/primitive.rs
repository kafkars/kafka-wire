//! Fixed-width primitives and Kafka unsigned-varint decoding.

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads a Kafka boolean.
    #[inline]
    pub fn read_bool(&mut self) -> Result<bool, DecodeError> {
        let offset = self.offset();
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(DecodeError::InvalidBoolean { offset, value }),
        }
    }

    /// Reads a signed 8-bit integer.
    #[inline]
    pub fn read_i8(&mut self) -> Result<i8, DecodeError> {
        let byte = self.read_u8()?;
        Ok(i8::from_be_bytes([byte]))
    }

    /// Reads the marker that introduces a nullable struct, reporting presence.
    ///
    /// A struct carries no length prefix, so nullability cannot be spelled the
    /// way a nullable string's is. It is a marker byte ahead of the body:
    /// negative for absent, non-negative for present.
    ///
    /// **This is a raw `int8` even in a flexible structure.** Every other
    /// length-like quantity in a flexible message is a varint, and this one is
    /// not — Apache Kafka keys the marker on the field's nullability alone,
    /// never on its flexible window. The two spellings are both one byte, so an
    /// implementation that reached for a varint here would round-trip against
    /// itself perfectly and be read by a real broker as *present*, which then
    /// parses the bytes after it as the struct body.
    ///
    /// Any negative value means absent, not `-1` alone. That is what Kafka's
    /// generated reader tests, so accepting only `-1` would reject a frame the
    /// protocol permits.
    #[inline]
    pub fn read_struct_presence(&mut self) -> Result<bool, DecodeError> {
        Ok(self.read_i8()? >= 0)
    }

    /// Reads a signed 16-bit integer.
    #[inline]
    pub fn read_i16(&mut self) -> Result<i16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads an unsigned 16-bit integer.
    #[inline]
    pub fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads a signed 32-bit integer.
    #[inline]
    pub fn read_i32(&mut self) -> Result<i32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads an unsigned 32-bit integer.
    #[inline]
    pub fn read_u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads a signed 64-bit integer.
    #[inline]
    pub fn read_i64(&mut self) -> Result<i64, DecodeError> {
        let bytes = self.take(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Reads an IEEE-754 double from eight big-endian bytes.
    #[inline]
    pub fn read_float64(&mut self) -> Result<f64, DecodeError> {
        let bytes = self.take(8)?;
        Ok(f64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Reads an unsigned Kafka varint.
    #[inline]
    pub fn read_unsigned_varint(&mut self) -> Result<u32, DecodeError> {
        let offset = self.offset();
        let mut value = 0_u32;

        for shift in [0_u32, 7, 14, 21, 28] {
            let byte = self.read_u8()?;
            if shift == 28 && byte & 0xf0 != 0 {
                return Err(DecodeError::MalformedVarint { offset });
            }

            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }

        Err(DecodeError::MalformedVarint { offset })
    }

    #[inline]
    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }
}
