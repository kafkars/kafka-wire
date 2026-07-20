//! Record-layer failure vocabulary.
//!
//! A batch arrives from a peer, so every one of these is reachable from hostile
//! or merely mismatched input rather than from a bug here. Each names what was
//! expected and what arrived, because "malformed batch" tells an operator
//! nothing about which side is wrong.

use kafka_wire_core::DecodeError;
use thiserror::Error;

/// Record batch decoding or encoding failure.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RecordError {
    /// A primitive read or write inside the batch failed.
    #[error(transparent)]
    Wire(#[from] DecodeError),

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

    /// A record's length prefix disagreed with what its fields consumed.
    #[error("record declares {declared} bytes but its fields used {consumed}")]
    RecordSizeMismatch {
        /// Length the record declared.
        declared: usize,
        /// Bytes its fields actually used.
        consumed: usize,
    },

    /// The batch is compressed with a codec this build cannot decode.
    ///
    /// Named rather than returned as opaque bytes: a caller handed the
    /// still-compressed payload would have no way to tell it from a decompressed
    /// one, and would parse garbage as records.
    #[error("record batch is {codec} compressed, which this build does not implement")]
    UnsupportedCompression {
        /// Codec the attributes named.
        codec: &'static str,
    },

    /// The attributes named a codec number the protocol does not define.
    #[error("record batch attributes name compression codec {codec}, which is not defined")]
    UnknownCompression {
        /// Codec number from attributes bits 0-2.
        codec: u8,
    },
}
