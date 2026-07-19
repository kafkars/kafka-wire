//! Primitive Kafka wire encoder.
//!
//! The encoder owns ordering and length-prefix mechanics; generated message code
//! owns field order and version gates.

use bytes::BytesMut;

use crate::{StrBytes, TaggedFields};

use super::{BufferTarget, EncodeError, EncodeTarget, SizeTarget};

/// Stateful Kafka wire encoder over an encoding target.
#[derive(Debug)]
pub struct Encoder<T> {
    target: T,
}

impl<'a> Encoder<BufferTarget<'a>> {
    /// Creates an encoder that appends to `buffer`.
    pub fn new(buffer: &'a mut BytesMut) -> Self {
        Self {
            target: BufferTarget::new(buffer),
        }
    }
}

impl Encoder<SizeTarget> {
    /// Creates an encoder that counts bytes.
    pub const fn sizing() -> Self {
        Self {
            target: SizeTarget::new(),
        }
    }
}

impl<T: EncodeTarget> Encoder<T> {
    /// Returns the number of bytes this encoder has emitted.
    ///
    /// Bytes the target already held when the encoder was created belong to an
    /// earlier frame and are not counted.
    #[inline]
    pub fn len(&self) -> usize {
        self.target.len()
    }

    /// Returns whether this encoder has emitted no bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.target.is_empty()
    }

    /// Writes a Kafka boolean.
    #[inline]
    pub fn write_bool(&mut self, value: bool) -> Result<(), EncodeError> {
        self.target.write_slice(&[u8::from(value)])
    }

    /// Writes a signed 8-bit integer.
    #[inline]
    pub fn write_i8(&mut self, value: i8) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes a signed 16-bit integer.
    #[inline]
    pub fn write_i16(&mut self, value: i16) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes an unsigned 16-bit integer.
    #[inline]
    pub fn write_u16(&mut self, value: u16) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes a signed 32-bit integer.
    #[inline]
    pub fn write_i32(&mut self, value: i32) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes an unsigned 32-bit integer.
    #[inline]
    pub fn write_u32(&mut self, value: u32) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes a signed 64-bit integer.
    #[inline]
    pub fn write_i64(&mut self, value: i64) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes an IEEE-754 double in big-endian byte order.
    #[inline]
    pub fn write_float64(&mut self, value: f64) -> Result<(), EncodeError> {
        self.target.write_slice(&value.to_be_bytes())
    }

    /// Writes a raw byte slice with no length prefix.
    ///
    /// This is the encode-side escape hatch: a downstream primitive (records or
    /// a future codec) that has already framed its own bytes emits them through
    /// here. The encoder adds no length, so the caller owns any prefix the wire
    /// format requires. Every higher-level writer that needs raw output routes
    /// through this method so the sizing and buffer targets stay on one path.
    #[inline]
    pub fn write_raw_slice(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.target.write_slice(bytes)
    }

    /// Writes an unsigned Kafka varint.
    #[inline]
    pub fn write_unsigned_varint(&mut self, mut value: u32) -> Result<(), EncodeError> {
        let mut encoded = [0_u8; 5];
        let mut length = 0_usize;

        loop {
            let low = u8::try_from(value & 0x7f).map_err(|_| EncodeError::LengthOverflow {
                kind: "unsigned varint",
                length: usize::try_from(value).unwrap_or(usize::MAX),
                maximum: usize::MAX,
            })?;
            value >>= 7;
            encoded[length] = if value == 0 { low } else { low | 0x80 };
            length += 1;
            if value == 0 {
                break;
            }
        }

        self.target.write_slice(&encoded[..length])
    }

    /// Writes a legacy non-null string.
    pub fn write_string(&mut self, value: &StrBytes) -> Result<(), EncodeError> {
        let length = i16::try_from(value.len()).map_err(|_| EncodeError::LengthOverflow {
            kind: "string",
            length: value.len(),
            maximum: usize::try_from(i16::MAX).unwrap_or(usize::MAX),
        })?;
        self.write_i16(length)?;
        self.target.write_slice(value.as_bytes())
    }

    /// Writes a legacy nullable string.
    pub fn write_nullable_string(&mut self, value: Option<&StrBytes>) -> Result<(), EncodeError> {
        match value {
            Some(value) => self.write_string(value),
            None => self.write_i16(-1),
        }
    }

    /// Writes a compact non-null string.
    pub fn write_compact_string(&mut self, value: &StrBytes) -> Result<(), EncodeError> {
        let length = compact_length(value.len(), "compact string")?;
        self.write_unsigned_varint(length)?;
        self.target.write_slice(value.as_bytes())
    }

    /// Writes a compact nullable string.
    pub fn write_compact_nullable_string(
        &mut self,
        value: Option<&StrBytes>,
    ) -> Result<(), EncodeError> {
        match value {
            Some(value) => self.write_compact_string(value),
            None => self.write_unsigned_varint(0),
        }
    }

    /// Writes unknown tagged fields in their validated order.
    pub fn write_tagged_fields(&mut self, fields: &TaggedFields) -> Result<(), EncodeError> {
        let count = u32::try_from(fields.len()).map_err(|_| EncodeError::LengthOverflow {
            kind: "tagged field count",
            length: fields.len(),
            maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
        })?;
        self.write_unsigned_varint(count)?;

        for field in fields.iter() {
            self.write_unsigned_varint(field.tag())?;
            let length =
                u32::try_from(field.data().len()).map_err(|_| EncodeError::LengthOverflow {
                    kind: "tagged field",
                    length: field.data().len(),
                    maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
                })?;
            self.write_unsigned_varint(length)?;
            self.target.write_slice(field.data())?;
        }

        Ok(())
    }
}

pub(super) fn compact_length(length: usize, kind: &'static str) -> Result<u32, EncodeError> {
    let length = u32::try_from(length).map_err(|_| EncodeError::LengthOverflow {
        kind,
        length,
        maximum: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
    })?;
    length.checked_add(1).ok_or(EncodeError::LengthOverflow {
        kind,
        length: usize::try_from(length).unwrap_or(usize::MAX),
        maximum: usize::try_from(u32::MAX - 1).unwrap_or(usize::MAX),
    })
}
