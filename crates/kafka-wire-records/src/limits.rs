//! Resource limits for decoding peer-authored record batches.
//!
//! This file owns the batch and decompression budgets layered above
//! `kafka_wire_core::DecodeLimits`. It deliberately owns no codec mechanics or
//! process-wide defaults hidden from a decode call.

use kafka_wire_core::DecodeLimits;

/// Resource limits applied while decoding one Kafka record batch.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordDecodeLimits {
    /// Maximum encoded size of one complete batch, including its 12-byte prefix.
    pub max_batch_bytes: usize,
    /// Maximum byte length of the records payload after decompression.
    pub max_decompressed_records_bytes: usize,
    /// Primitive, string, array, and tagged-field limits used inside records.
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
