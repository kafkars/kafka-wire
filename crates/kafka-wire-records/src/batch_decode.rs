//! Cursor-safe complete and partial-tail record-batch decoding.

use bytes::{Buf as _, Bytes};
use kafka_wire_core::Decoder;

use crate::attributes::Attributes;
use crate::batch::{CRC_COVERAGE_START, MAGIC_V2, RecordBatch};
use crate::batch_prefix::{BatchPrefix, classify_next_batch, exact_batch};
use crate::error::RecordError;
use crate::limits::RecordDecodeLimits;

/// Result of inspecting the next batch in a Kafka record-set byte field.
#[non_exhaustive]
#[derive(Debug, Eq, PartialEq)]
pub enum RecordBatchDecode {
    /// One complete batch was decoded and removed from the input cursor.
    Complete {
        /// Decoded `RecordBatch` v2 fields and records.
        batch: RecordBatch,
        /// Additional visible byte storage retained after decompression.
        ///
        /// This is zero for an uncompressed batch because its record slices
        /// borrow the already-owned input bytes.
        additional_retained_payload_bytes: usize,
    },
    /// Kafka ended the record set with an incomplete next batch.
    ///
    /// The input cursor remains unchanged so the caller may inspect or retain
    /// the tail according to its own fetch policy.
    PartialTrailing {
        /// Bytes available for the incomplete batch.
        bytes: usize,
    },
}

impl RecordBatch {
    /// Reads and removes one batch from the front of `bytes`.
    ///
    /// Bytes after the declared batch remain ready for the next call. A failure
    /// leaves the cursor unchanged. This strict compatibility entrypoint treats
    /// an incomplete tail as an error; Fetch consumers should use
    /// [`Self::decode_next`] to distinguish Kafka's permitted partial tail.
    pub fn decode(bytes: &mut Bytes, limits: RecordDecodeLimits) -> Result<Self, RecordError> {
        let batch_bytes = exact_batch(bytes, limits.max_batch_bytes)?;
        let (batch, _) =
            decode_complete(&batch_bytes, limits, limits.max_decompressed_records_bytes)?;
        bytes.advance(batch_bytes.len());
        Ok(batch)
    }

    /// Decodes one complete batch or classifies Kafka's permitted partial tail.
    ///
    /// `max_additional_retained_payload_bytes` is the remaining cumulative
    /// budget for visible key, value, and header spans backed by newly
    /// decompressed storage. Decompression scratch remains independently
    /// bounded by [`RecordDecodeLimits::max_decompressed_records_bytes`].
    /// Uncompressed record slices retain the caller's input allocation and
    /// therefore consume zero of this additional payload budget.
    pub fn decode_next(
        bytes: &mut Bytes,
        limits: RecordDecodeLimits,
        max_additional_retained_payload_bytes: usize,
    ) -> Result<RecordBatchDecode, RecordError> {
        let batch_bytes = match classify_next_batch(bytes, limits.max_batch_bytes)? {
            BatchPrefix::Complete(batch) => batch,
            BatchPrefix::PartialTrailing { bytes } => {
                return Ok(RecordBatchDecode::PartialTrailing { bytes });
            }
        };
        let (batch, additional_retained_payload_bytes) =
            decode_complete(&batch_bytes, limits, max_additional_retained_payload_bytes)?;
        bytes.advance(batch_bytes.len());
        Ok(RecordBatchDecode::Complete {
            batch,
            additional_retained_payload_bytes,
        })
    }
}

fn decode_complete(
    batch_bytes: &Bytes,
    limits: RecordDecodeLimits,
    max_additional_retained_payload_bytes: usize,
) -> Result<(RecordBatch, usize), RecordError> {
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
    decoder.check_collection_limit("record batch records", records_count, records_count_offset)?;

    let payload = decoder.take_bytes(end - (CRC_COVERAGE_START + 40))?;
    let decoded = attributes
        .compression
        .decompress(payload, limits.max_decompressed_records_bytes)?;
    let payload_len = decoded.len();
    let records = crate::record_set::decode_all(
        decoded,
        records_count,
        limits.wire_for_container(payload_len),
    )?;
    let additional_retained_payload_bytes = if attributes.compression == crate::Compression::None {
        0
    } else {
        crate::record_set::retained_payload_bytes(&records, max_additional_retained_payload_bytes)?
    };
    Ok((
        RecordBatch {
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
        },
        additional_retained_payload_bytes,
    ))
}
