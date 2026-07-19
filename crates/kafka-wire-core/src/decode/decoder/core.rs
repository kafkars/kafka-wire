//! Decoder cursor ownership, bounded byte extraction, and shared limit checks.

use bytes::Bytes;

use super::super::{DecodeError, DecodeLimits};

/// Stateful decoder over one Kafka message body.
#[derive(Clone, Debug)]
pub struct Decoder {
    pub(super) input: Bytes,
    pub(super) initial_len: usize,
    pub(super) limits: DecodeLimits,
}

impl Decoder {
    /// Creates a decoder with explicit resource limits.
    pub fn new(input: Bytes, limits: DecodeLimits) -> Self {
        let initial_len = input.len();
        Self {
            input,
            initial_len,
            limits,
        }
    }

    /// Returns the current byte offset.
    #[inline]
    pub fn offset(&self) -> usize {
        self.initial_len - self.input.len()
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

    /// Rejects a claimed element count that the unread bytes cannot back.
    ///
    /// Every array element and every tagged field occupies at least one wire
    /// byte, so a count larger than the remainder of the frame is malformed
    /// input no matter how `DecodeLimits` is configured. Rejecting it at the
    /// prefix keeps a peer from driving a caller's `Vec::with_capacity` with a
    /// length that no frame of this size could ever deliver.
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
