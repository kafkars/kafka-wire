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
    pub fn offset(&self) -> usize {
        self.initial_len - self.input.len()
    }

    /// Returns the unread byte count.
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
