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

use bytes::{Buf as _, Bytes};
use kafka_wire_core::Decoder;

use crate::attributes::{Attributes, Compression, TimestampType};
use crate::batch_prefix::exact_batch;
use crate::error::RecordError;
use crate::limits::RecordDecodeLimits;
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

impl RecordBatch {
    /// Reads and removes one batch from the front of `bytes`.
    ///
    /// The order matters. A CRC checked after parsing would let a corrupt length
    /// drive an allocation or a nonsense field reach a caller before anything
    /// noticed, so the checksum runs against the raw bytes first and the fields
    /// are only read once they are known to be the bytes Kafka wrote.
    ///
    /// Bytes after the declared batch remain in `bytes`, ready for the next
    /// call. A failure leaves the cursor unchanged.
    pub fn decode(bytes: &mut Bytes, limits: RecordDecodeLimits) -> Result<Self, RecordError> {
        let batch_bytes = exact_batch(bytes, limits.max_batch_bytes)?;
        let end = batch_bytes.len();
        let mut decoder = Decoder::new(batch_bytes.clone(), limits.wire_for_container(end))?;
        let base_offset = decoder.read_i64()?;
        let _validated_batch_length = decoder.read_i32()?;

        let partition_leader_epoch = decoder.read_i32()?;
        let magic = decoder.read_i8()?;
        if magic != MAGIC_V2 {
            return Err(RecordError::UnsupportedMagic { magic });
        }
        let crc = decoder.read_u32()?;
        let actual = crc32c::crc32c(&batch_bytes[CRC_COVERAGE_START..]);
        if actual != crc {
            return Err(RecordError::CorruptBatch {
                declared: crc,
                actual,
            });
        }

        let attributes = Attributes::decode(decoder.read_i16()?)?;
        let last_offset_delta = decoder.read_i32()?;
        let base_timestamp = decoder.read_i64()?;
        let max_timestamp = decoder.read_i64()?;
        let producer_id = decoder.read_i64()?;
        let producer_epoch = decoder.read_i16()?;
        let base_sequence = decoder.read_i32()?;
        let records_count_offset = decoder.offset();
        let records_count_wire = decoder.read_i32()?;
        let records_count =
            usize::try_from(records_count_wire).map_err(|_| RecordError::NegativeRecordCount {
                count: records_count_wire,
            })?;
        decoder.check_collection_limit(
            "record batch records",
            records_count,
            records_count_offset,
        )?;

        let payload = decoder.take_bytes(end - (CRC_COVERAGE_START + 40))?;
        let payload = attributes
            .compression
            .decompress(payload, limits.max_decompressed_records_bytes)?;
        let payload_len = payload.len();
        let records = crate::record_set::decode_all(
            payload,
            records_count,
            limits.wire_for_container(payload_len),
        )?;

        let batch = Self {
            base_offset,
            last_offset_delta,
            partition_leader_epoch,
            compression: attributes.compression,
            timestamp_type: attributes.timestamp_type,
            is_transactional: attributes.is_transactional,
            is_control: attributes.is_control,
            has_delete_horizon: attributes.has_delete_horizon,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        };
        bytes.advance(end);
        Ok(batch)
    }
}
