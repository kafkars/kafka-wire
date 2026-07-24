//! The v2 record batch: a 61-byte header, a CRC, and a records payload.
//!
//! The layout below is not transcribed from documentation. Every offset and
//! width in it was read back out of bytes Apache Kafka's own
//! `MemoryRecordsBuilder` produced, and the corpus under `spec/records/` is the
//! record of that.
//!
//! Two facts are easy to get subtly wrong and are therefore stated here rather
//! than left to a reader:
//!
//! * `batch_length` counts everything after itself — the bytes from
//!   `partition_leader_epoch` to the end — and not the whole batch.
//! * the CRC is CRC32C (Castagnoli, the `iSCSI` polynomial) over everything
//!   *after* the CRC field, not over the batch and not over the records alone.

use crate::attributes::{Compression, TimestampType};
use crate::record::Record;

/// The only magic byte this crate implements.
pub const MAGIC_V2: i8 = 2;

/// Where the CRC's coverage begins: the first byte after the CRC field itself.
///
/// The CRC sits at offset 17 and is four bytes wide, so it covers everything
/// from 21 to the end of the batch — not the batch, and not the records alone.
pub(super) const CRC_COVERAGE_START: usize = 21;

/// One Kafka record batch, magic v2.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordBatch {
    /// Absolute offset of the first record.
    pub base_offset: i64,
    /// Offset of the original batch's last record relative to `base_offset`.
    ///
    /// This is independent of the number of records still present: log
    /// compaction preserves the original last offset while removing records and
    /// may leave an empty batch behind.
    pub last_offset_delta: i32,
    /// Leader epoch of the partition when this batch was appended, or `-1`.
    pub partition_leader_epoch: i32,
    /// Codec the records payload uses.
    pub compression: Compression,
    /// Whether the producer or the broker stamped the timestamps.
    pub timestamp_type: TimestampType,
    /// Whether this batch belongs to a transaction.
    pub is_transactional: bool,
    /// Whether this batch carries control records rather than user data.
    pub is_control: bool,
    /// Whether the batch carries a delete horizon, as tombstone retention needs.
    pub has_delete_horizon: bool,
    /// Timestamp of the first record.
    pub base_timestamp: i64,
    /// Largest timestamp in the batch, which need not be the last record's.
    pub max_timestamp: i64,
    /// Producer id, or `-1` outside the idempotent producer.
    pub producer_id: i64,
    /// Producer epoch, or `-1`.
    pub producer_epoch: i16,
    /// Sequence number of the first record, or `-1`.
    pub base_sequence: i32,
    /// The records themselves.
    pub records: Vec<Record>,
}
