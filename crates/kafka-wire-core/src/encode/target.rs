//! Encoding targets for bytes and size calculation.
//!
//! Both targets implement the same sink contract so generated messages have one
//! semantic encoding path.

use bytes::BytesMut;

use super::EncodeError;

/// What an encoding target needs for a payload whose length is already known.
///
/// This is an implementation hook for `Encoder`; protocol codecs should write
/// through the encoder rather than branch on the target themselves.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PremeasuredWrite {
    /// The target accounted for the payload from its length alone.
    Accounted,
    /// The target needs the payload writer to emit the actual bytes.
    WritePayload,
}

/// Destination consumed by `Encoder`.
pub trait EncodeTarget {
    /// Writes one contiguous byte slice.
    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), EncodeError>;

    /// Returns the number of bytes this target has observed.
    ///
    /// The count covers only writes made through this target, so a buffer shared
    /// with earlier frames still reports one message at a time.
    fn len(&self) -> usize;

    /// Returns whether this target has observed no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Accounts for a payload whose exact byte length was already measured.
    ///
    /// Byte-materializing targets use the default and receive the payload
    /// normally. A sizing target can advance directly and avoid traversing the
    /// same value a second time.
    #[doc(hidden)]
    fn prepare_premeasured(&mut self, _length: usize) -> Result<PremeasuredWrite, EncodeError> {
        Ok(PremeasuredWrite::WritePayload)
    }
}

/// Target that appends to a growable byte buffer.
///
/// A pipelining client reuses one buffer for a size prefix, a header, and a
/// body, so the target remembers where its own message began.
#[derive(Debug)]
pub struct BufferTarget<'a> {
    buffer: &'a mut BytesMut,
    start: usize,
}

impl<'a> BufferTarget<'a> {
    pub(super) fn new(buffer: &'a mut BytesMut) -> Self {
        let start = buffer.len();
        Self { buffer, start }
    }
}

impl EncodeTarget for BufferTarget<'_> {
    #[inline]
    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    /// Returns the bytes written since this target was created.
    ///
    /// The target only appends, so the buffer never shrinks below `start`.
    #[inline]
    fn len(&self) -> usize {
        self.buffer.len().saturating_sub(self.start)
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
    #[inline]
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

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn prepare_premeasured(&mut self, length: usize) -> Result<PremeasuredWrite, EncodeError> {
        self.len = self
            .len
            .checked_add(length)
            .ok_or(EncodeError::LengthOverflow {
                kind: "encoded message",
                length: usize::MAX,
                maximum: usize::MAX,
            })?;
        Ok(PremeasuredWrite::Accounted)
    }
}
