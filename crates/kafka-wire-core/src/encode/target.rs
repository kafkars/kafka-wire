//! Encoding targets for bytes and size calculation.
//!
//! Both targets implement the same sink contract so generated messages have one
//! semantic encoding path.

use bytes::BytesMut;

use super::EncodeError;

/// Destination consumed by `Encoder`.
pub trait EncodeTarget {
    /// Writes one contiguous byte slice.
    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), EncodeError>;

    /// Returns the number of bytes observed so far.
    fn len(&self) -> usize;

    /// Returns whether no bytes have been observed.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Target that appends to a growable byte buffer.
#[derive(Debug)]
pub struct BufferTarget<'a> {
    buffer: &'a mut BytesMut,
}

impl<'a> BufferTarget<'a> {
    pub(super) const fn new(buffer: &'a mut BytesMut) -> Self {
        Self { buffer }
    }
}

impl EncodeTarget for BufferTarget<'_> {
    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }
}

/// Target that counts bytes without materializing them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SizeTarget {
    len: usize,
}

impl SizeTarget {
    pub(super) const fn new() -> Self {
        Self { len: 0 }
    }
}

impl EncodeTarget for SizeTarget {
    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(EncodeError::LengthOverflow {
                kind: "encoded message",
                length: usize::MAX,
                maximum: usize::MAX,
            })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}
