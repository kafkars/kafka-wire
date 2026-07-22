//! Record-layer failure vocabulary.
//!
//! A batch arrives from a peer, so every one of these is reachable from hostile
//! or merely mismatched input rather than from a bug here. Each names what was
//! expected and what arrived, because "malformed batch" tells an operator
//! nothing about which side is wrong.

use kafka_wire_core::{DecodeError, EncodeError};
use thiserror::Error;

/// Record batch decoding or encoding failure.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RecordError {
    /// A primitive read inside the batch failed.
    #[error(transparent)]
    Wire(#[from] DecodeError),

    /// A primitive write inside the batch failed.
    #[error(transparent)]
    Encode(#[from] EncodeError),

    /// A batch declared a negative byte length.
    #[error("record batch declares negative length {length}")]
    NegativeBatchLength {
        /// Signed length read from the batch prefix.
        length: i32,
    },

    /// A complete batch exceeded the caller's encoded-size budget.
    #[error("record batch length {length} exceeds configured limit {limit}")]
    BatchLimitExceeded {
        /// Complete encoded batch length.
        length: usize,
        /// Configured maximum.
        limit: usize,
    },

    /// A compressed records payload expanded past the caller's budget.
    #[error("{codec} records payload exceeds decompressed-byte limit {limit}")]
    DecompressionLimitExceeded {
        /// Codec whose output crossed the limit.
        codec: &'static str,
        /// Configured maximum expanded length.
        limit: usize,
    },

    /// The batch declared a magic byte this crate does not implement.
    ///
    /// v0 and v1 are the pre-KIP-98 message sets, which have a different frame
    /// entirely rather than a different field list. Refused by name so a caller
    /// reading an old log segment learns why rather than getting nonsense.
    #[error("record batch magic {magic} is not supported; this crate implements v2 only")]
    UnsupportedMagic {
        /// Magic byte the batch declared.
        magic: i8,
    },

    /// The CRC in the header disagreed with the bytes that follow it.
    ///
    /// Kafka computes CRC32C over everything after the CRC field to the end of
    /// the batch. A mismatch means corruption in transit or a disagreement about
    /// that span, and either way the batch must not be interpreted.
    #[error("record batch CRC32C is {actual:#010x} but the batch declares {declared:#010x}")]
    CorruptBatch {
        /// CRC the batch carried.
        declared: u32,
        /// CRC computed over the bytes that followed it.
        actual: u32,
    },

    /// The batch length prefix disagreed with the bytes available.
    #[error(
        "record batch declares {declared} bytes after its length prefix but {available} remain"
    )]
    TruncatedBatch {
        /// Length the batch declared.
        declared: usize,
        /// Bytes actually available.
        available: usize,
    },

    /// The record count disagreed with the records actually present.
    ///
    /// Kafka states the count in the header and then writes that many records.
    /// Trusting one over the other would let a peer hide a record or invent one.
    #[error("record batch declares {declared} record(s) but its payload holds {actual}")]
    RecordCountMismatch {
        /// Count the header declared.
        declared: usize,
        /// Records the payload actually contained.
        actual: usize,
    },

    /// The batch header declared a negative record count.
    #[error("record batch declares negative record count {count}")]
    NegativeRecordCount {
        /// Signed count read from the batch header.
        count: i32,
    },

    /// A record declared a negative body length.
    #[error("record declares negative length {length} at byte {offset}")]
    NegativeRecordLength {
        /// Signed zigzag-varint length read from the record prefix.
        length: i32,
        /// Absolute byte offset of the prefix reported by the decoder.
        offset: usize,
    },

    /// A record declared a negative number of headers.
    #[error("record declares negative header count {count} at byte {offset}")]
    NegativeHeaderCount {
        /// Signed count read from the record body.
        count: i32,
        /// Absolute byte offset of the count prefix.
        offset: usize,
    },

    /// A record's length prefix disagreed with what its fields consumed.
    #[error("record declares {declared} bytes but its fields used {consumed}")]
    RecordSizeMismatch {
        /// Length the record declared.
        declared: usize,
        /// Bytes its fields actually used.
        consumed: usize,
    },

    /// A nullable record field used a sentinel below `-1`.
    #[error("record field length {length} is below the null sentinel -1")]
    InvalidRecordFieldLength {
        /// Signed zigzag-varint length read from the record.
        length: i32,
    },

    /// A record header used the null sentinel for its required key.
    #[error("record header key is null; header keys must be present")]
    NullHeaderKey,

    /// A codec rejected the payload it was handed.
    ///
    /// Reachable from a peer, not only from a bug: a truncated or mislabelled
    /// payload reaches the codec before anything here can tell it is wrong.
    #[error("{codec} could not process the records payload: {detail}")]
    CompressionFailed {
        /// Codec that refused.
        codec: &'static str,
        /// What the codec reported.
        detail: String,
    },

    /// The attributes named a codec number the protocol does not define.
    #[error("record batch attributes name compression codec {codec}, which is not defined")]
    UnknownCompression {
        /// Codec number from attributes bits 0-2.
        codec: u8,
    },
}
