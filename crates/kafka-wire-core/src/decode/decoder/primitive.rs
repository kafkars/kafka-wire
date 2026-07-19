//! Fixed-width primitives and Kafka unsigned-varint decoding.

use super::super::DecodeError;
use super::Decoder;

impl Decoder {
    /// Reads a Kafka boolean.
    pub fn read_bool(&mut self) -> Result<bool, DecodeError> {
        let offset = self.offset();
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(DecodeError::InvalidBoolean { offset, value }),
        }
    }

    /// Reads a signed 8-bit integer.
    pub fn read_i8(&mut self) -> Result<i8, DecodeError> {
        let byte = self.read_u8()?;
        Ok(i8::from_be_bytes([byte]))
    }

    /// Reads a signed 16-bit integer.
    pub fn read_i16(&mut self) -> Result<i16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads an unsigned 16-bit integer.
    pub fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads a signed 32-bit integer.
    pub fn read_i32(&mut self) -> Result<i32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads an unsigned 32-bit integer.
    pub fn read_u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads a signed 64-bit integer.
    pub fn read_i64(&mut self) -> Result<i64, DecodeError> {
        let bytes = self.take(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Reads an unsigned Kafka varint.
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

    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }
}
