//! Decoder cursor ownership, bounded byte extraction, and shared limit checks.

use bytes::Bytes;

use super::super::{DecodeError, DecodeLimits};

/// Stateful decoder over one Kafka message body.
#[derive(Clone, Debug)]
pub struct Decoder {
    pub(super) input: Bytes,
    pub(super) initial_len: usize,
    pub(super) base_offset: usize,
    pub(super) limits: DecodeLimits,
}

impl Decoder {
    /// Creates a decoder with explicit resource limits.
    ///
    /// The complete frame is rejected before any field parser observes it.
    pub fn new(input: Bytes, limits: DecodeLimits) -> Result<Self, DecodeError> {
        Self::check_limit("frame", input.len(), limits.max_frame_bytes, 0)?;
        Ok(Self::with_base(input, limits, 0))
    }

    pub(super) fn child(input: Bytes, limits: DecodeLimits, base_offset: usize) -> Self {
        Self::with_base(input, limits, base_offset)
    }

    fn with_base(input: Bytes, limits: DecodeLimits, base_offset: usize) -> Self {
        let initial_len = input.len();
        Self {
            input,
            initial_len,
            base_offset,
            limits,
        }
    }

    /// Returns the current byte offset.
    #[inline]
    pub fn offset(&self) -> usize {
        self.base_offset + self.initial_len - self.input.len()
    }

    /// Returns the unread byte count.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.input.len()
    }

    /// Requires that the entire message body was consumed.
    pub fn finish(self) -> Result<(), DecodeError> {
        let remaining = self.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes { remaining })
        }
    }

    pub(super) fn check_limit(
        kind: &'static str,
        length: usize,
        limit: usize,
        offset: usize,
    ) -> Result<(), DecodeError> {
        if length <= limit {
            Ok(())
        } else {
            Err(DecodeError::LimitExceeded {
                kind,
                length,
                limit,
                offset,
            })
        }
    }

    /// Checks a downstream collection count against the configured element budget.
    ///
    /// Kafka containers such as record batches have their own count encodings,
    /// so they cannot call the standard array-length readers. This is the shared
    /// pre-allocation seam: the downstream parser supplies the count and its
    /// prefix offset, while `Decoder` remains the sole owner of how
    /// `max_array_elements` is enforced.
    pub fn check_collection_limit(
        &self,
        kind: &'static str,
        count: usize,
        offset: usize,
    ) -> Result<(), DecodeError> {
        Self::check_limit(kind, count, self.limits.max_array_elements, offset)
    }

    /// Rejects a claimed element count that the unread bytes cannot back.
    ///
    /// Every tagged-field entry occupies at least its tag and length varints,
    /// so a count larger than the remainder of the frame is malformed input.
    /// Arrays deliberately do not use this heuristic: a future validated
    /// structure may have zero wire width in a legacy version.
    pub(super) fn check_element_count(
        &self,
        kind: &'static str,
        count: usize,
        offset: usize,
    ) -> Result<(), DecodeError> {
        let remaining = self.remaining();
        if count <= remaining {
            Ok(())
        } else {
            Err(DecodeError::CountExceedsFrame {
                kind,
                count,
                remaining,
                offset,
            })
        }
    }

    /// Reads exactly `count` raw bytes, bounded by the unread remainder.
    ///
    /// This is the decode-side escape hatch: a downstream primitive (records or
    /// a future codec) claims a run of bytes it will frame itself. The returned
    /// `Bytes` is a zero-copy slice of the input cursor, and `count` is checked
    /// against the bytes that remain before slicing, so a peer cannot drive a
    /// read past the frame it sent.
    #[inline]
    pub fn take_bytes(&mut self, count: usize) -> Result<Bytes, DecodeError> {
        self.take(count)
    }

    #[inline]
    pub(super) fn take(&mut self, length: usize) -> Result<Bytes, DecodeError> {
        let remaining = self.input.len();
        if length > remaining {
            return Err(DecodeError::UnexpectedEnd {
                offset: self.offset(),
                needed: length,
                remaining,
            });
        }

        Ok(self.input.split_to(length))
    }
}
