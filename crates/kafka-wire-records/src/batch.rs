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

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{Decoder, EncodeError, EncodeTarget, Encoder};

use crate::attributes::{Attributes, Compression, TimestampType};
use crate::error::RecordError;
use crate::record::{self, Record};

/// The only magic byte this crate implements.
pub const MAGIC_V2: i8 = 2;

/// Where the CRC's coverage begins: the first byte after the CRC field itself.
///
/// The CRC sits at offset 17 and is four bytes wide, so it covers everything
/// from 21 to the end of the batch — not the batch, and not the records alone.
const CRC_COVERAGE_START: usize = 21;

/// Bytes from `partition_leader_epoch` through `records_count`.
const HEADER_AFTER_LENGTH: usize = 49;

/// One Kafka record batch, magic v2.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordBatch {
    /// Absolute offset of the first record.
    pub base_offset: i64,
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
    /// Reads one batch, verifying its CRC before interpreting anything after it.
    ///
    /// The order matters. A CRC checked after parsing would let a corrupt length
    /// drive an allocation or a nonsense field reach a caller before anything
    /// noticed, so the checksum runs against the raw bytes first and the fields
    /// are only read once they are known to be the bytes Kafka wrote.
    pub fn decode(bytes: &Bytes) -> Result<Self, RecordError> {
        let mut decoder = Decoder::new(bytes.clone(), kafka_wire_core::DecodeLimits::default());
        let base_offset = decoder.read_i64()?;
        let batch_length = decoder.read_i32()?;
        let declared = usize::try_from(batch_length).unwrap_or(usize::MAX);
        if declared < HEADER_AFTER_LENGTH || declared > decoder.remaining() {
            return Err(RecordError::TruncatedBatch {
                declared,
                available: decoder.remaining(),
            });
        }
        // Everything past the declared length belongs to the next batch in the
        // blob, not to this one. The length is stated from just after itself,
        // so the batch ends 12 bytes (base offset plus the length) further on.
        let end = 12 + declared;

        let partition_leader_epoch = decoder.read_i32()?;
        let magic = decoder.read_i8()?;
        if magic != MAGIC_V2 {
            return Err(RecordError::UnsupportedMagic { magic });
        }
        let crc = decoder.read_u32()?;
        let actual = crc32c::crc32c(&bytes[CRC_COVERAGE_START..end]);
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
        let records_count = decoder.read_i32()?;
        let records_count = usize::try_from(records_count).unwrap_or(0);

        let payload = decoder.take_bytes(end - (CRC_COVERAGE_START + 40))?;
        if attributes.compression != Compression::None {
            return Err(RecordError::UnsupportedCompression {
                codec: attributes.compression.name(),
            });
        }
        let records = record::decode_all(payload, records_count)?;

        // Kafka derives this from the records rather than trusting it, and so
        // must anything that re-encodes the batch. Checking it here means a
        // header that disagrees with its own payload cannot round-trip
        // undetected.
        let expected_last = i32::try_from(records.len().saturating_sub(1)).unwrap_or(0);
        if last_offset_delta != expected_last {
            return Err(RecordError::RecordCountMismatch {
                declared: usize::try_from(last_offset_delta).unwrap_or(0) + 1,
                actual: records.len(),
            });
        }

        Ok(Self {
            base_offset,
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
        })
    }

    /// Writes this batch, computing the length and the CRC from what it wrote.
    ///
    /// Neither is carried on the struct. A length or a checksum a caller could
    /// set is one that can disagree with the bytes beside it, and Kafka derives
    /// both — so they are derived here too, and the type has no way to express
    /// a batch whose header lies about its payload.
    pub fn encode<T: EncodeTarget>(&self, encoder: &mut Encoder<T>) -> Result<(), EncodeError> {
        let mut payload = BytesMut::new();
        record::encode_all(&self.records, &mut payload)?;

        let mut body = BytesMut::new();
        let mut inner = Encoder::new(&mut body);
        inner.write_i16(
            Attributes {
                compression: self.compression,
                timestamp_type: self.timestamp_type,
                is_transactional: self.is_transactional,
                is_control: self.is_control,
                has_delete_horizon: self.has_delete_horizon,
            }
            .encode(),
        )?;
        inner.write_i32(i32::try_from(self.records.len().saturating_sub(1)).unwrap_or(0))?;
        inner.write_i64(self.base_timestamp)?;
        inner.write_i64(self.max_timestamp)?;
        inner.write_i64(self.producer_id)?;
        inner.write_i16(self.producer_epoch)?;
        inner.write_i32(self.base_sequence)?;
        inner.write_i32(i32::try_from(self.records.len()).map_err(|_| {
            EncodeError::LengthOverflow {
                kind: "record count",
                length: self.records.len(),
                maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            }
        })?)?;
        inner.write_raw_slice(&payload)?;

        // partition_leader_epoch + magic + crc + everything the CRC covers.
        let batch_length =
            i32::try_from(4 + 1 + 4 + body.len()).map_err(|_| EncodeError::LengthOverflow {
                kind: "record batch",
                length: body.len(),
                maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })?;

        encoder.write_i64(self.base_offset)?;
        encoder.write_i32(batch_length)?;
        encoder.write_i32(self.partition_leader_epoch)?;
        encoder.write_i8(MAGIC_V2)?;
        encoder.write_u32(crc32c::crc32c(&body))?;
        encoder.write_raw_slice(&body)
    }
}
