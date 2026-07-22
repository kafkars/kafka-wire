//! Resource limits for decoding peer-authored record batches.
//!
//! This file owns the batch and decompression budgets layered above
//! `kafka_wire_core::DecodeLimits`. It deliberately owns no codec mechanics or
//! process-wide defaults hidden from a decode call.

use kafka_wire_core::DecodeLimits;

/// Largest complete batch Kafka's signed 32-bit `batchLength` can describe.
///
/// The field counts bytes after the initial 12-byte base-offset and length
/// prefix, so the complete container may be exactly 12 bytes larger than
/// `i32::MAX`.
pub const MAX_PROTOCOL_BATCH_BYTES: usize = 12 + 2_147_483_647;

/// Resource limits applied while decoding one Kafka record batch.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordDecodeLimits {
    /// Maximum encoded size of one complete batch, including its 12-byte prefix.
    pub max_batch_bytes: usize,
    /// Maximum byte length of the records payload after decompression.
    pub max_decompressed_records_bytes: usize,
    /// Wire limits used inside records; `max_array_elements` bounds both records
    /// per batch and headers per record before either collection allocates.
    ///
    /// `max_frame_bytes` is deliberately superseded here: `max_batch_bytes`
    /// bounds the exact encoded batch and `max_decompressed_records_bytes`
    /// bounds its expanded payload. The remaining wire limits govern fields
    /// inside those already-bounded containers.
    pub wire: DecodeLimits,
}

impl RecordDecodeLimits {
    /// Creates explicit record-batch limits.
    pub const fn new(
        max_batch_bytes: usize,
        max_decompressed_records_bytes: usize,
        wire: DecodeLimits,
    ) -> Self {
        Self {
            max_batch_bytes,
            max_decompressed_records_bytes,
            wire,
        }
    }

    pub(crate) const fn wire_for_container(self, length: usize) -> DecodeLimits {
        let mut wire = self.wire;
        wire.max_frame_bytes = length;
        wire
    }
}

impl Default for RecordDecodeLimits {
    fn default() -> Self {
        const DEFAULT_BATCH_BYTES: usize = 100 * 1024 * 1024;

        Self::new(
            DEFAULT_BATCH_BYTES,
            DEFAULT_BATCH_BYTES,
            DecodeLimits::default(),
        )
    }
}

/// Resource limits applied while encoding one Kafka record batch.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordEncodeLimits {
    /// Maximum byte length of the records before compression.
    pub max_uncompressed_records_bytes: usize,
    /// Maximum complete encoded batch length, including its 12-byte prefix.
    pub max_encoded_batch_bytes: usize,
}

impl RecordEncodeLimits {
    /// Creates explicit record-batch encoding limits.
    pub const fn new(
        max_uncompressed_records_bytes: usize,
        max_encoded_batch_bytes: usize,
    ) -> Self {
        Self {
            max_uncompressed_records_bytes,
            max_encoded_batch_bytes,
        }
    }

    pub(crate) const fn effective_max_encoded_batch_bytes(self) -> usize {
        if self.max_encoded_batch_bytes < MAX_PROTOCOL_BATCH_BYTES {
            self.max_encoded_batch_bytes
        } else {
            MAX_PROTOCOL_BATCH_BYTES
        }
    }
}

impl Default for RecordEncodeLimits {
    fn default() -> Self {
        const DEFAULT_BATCH_BYTES: usize = 100 * 1024 * 1024;

        Self::new(DEFAULT_BATCH_BYTES, DEFAULT_BATCH_BYTES)
    }
}
