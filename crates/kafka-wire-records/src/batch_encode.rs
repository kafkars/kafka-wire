//! Preflighted, rollback-safe encoding for one v2 record batch.
//!
//! This file owns outbound allocation limits, fixed-header backfilling, and
//! direct payload emission. It does not own record fields or codec framing.

use bytes::{Bytes, BytesMut};
use kafka_wire_core::EncodeError;

use crate::{
    RecordBatch, RecordEncodeLimits, RecordError,
    attributes::{Attributes, Compression},
    batch::{CRC_COVERAGE_START, MAGIC_V2},
    limits::MAX_PROTOCOL_BATCH_BYTES,
};

const FIXED_BATCH_BYTES: usize = 61;
const BATCH_LENGTH_OFFSET: usize = 8;
const CRC_OFFSET: usize = 17;

impl RecordBatch {
    /// Encodes one batch into newly allocated immutable bytes under explicit limits.
    pub fn encode_to_bytes(&self, limits: RecordEncodeLimits) -> Result<Bytes, RecordError> {
        let mut buffer = BytesMut::new();
        self.encode_into(&mut buffer, limits)?;
        Ok(buffer.freeze())
    }

    /// Appends one complete batch and restores `buffer` exactly on every error.
    pub fn encode_into(
        &self,
        buffer: &mut BytesMut,
        limits: RecordEncodeLimits,
    ) -> Result<usize, RecordError> {
        let max_batch_bytes = limits.effective_max_encoded_batch_bytes();
        let uncompressed = crate::record_set::encoded_len_all(
            &self.records,
            limits.max_uncompressed_records_bytes,
        )?;
        let record_count =
            i32::try_from(self.records.len()).map_err(|_| EncodeError::LengthOverflow {
                kind: "record count",
                length: self.records.len(),
                maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })?;
        let minimum = if self.compression == Compression::None {
            complete_batch_len(uncompressed)?
        } else {
            FIXED_BATCH_BYTES
        };
        if minimum > max_batch_bytes {
            return Err(RecordError::BatchLimitExceeded {
                length: minimum,
                limit: max_batch_bytes,
            });
        }

        let start = buffer.len();
        buffer.reserve(minimum);
        let outcome =
            self.encode_preflighted(buffer, start, uncompressed, record_count, max_batch_bytes);
        if outcome.is_err() {
            buffer.truncate(start);
        }
        outcome
    }

    fn encode_preflighted(
        &self,
        buffer: &mut BytesMut,
        start: usize,
        uncompressed: usize,
        record_count: i32,
        max_batch_bytes: usize,
    ) -> Result<usize, RecordError> {
        write_header(buffer, self, record_count);

        if self.compression == Compression::None {
            crate::record_set::encode_all(&self.records, buffer)?;
        } else {
            let mut plain = BytesMut::with_capacity(uncompressed);
            crate::record_set::encode_all(&self.records, &mut plain)?;
            if plain.len() != uncompressed {
                return Err(EncodeError::SizeMismatch {
                    predicted: uncompressed,
                    actual: plain.len(),
                }
                .into());
            }
            self.compression
                .compress_into(&plain, buffer, start, max_batch_bytes)?;
        }

        let total = buffer.len().saturating_sub(start);
        if total > max_batch_bytes {
            return Err(RecordError::BatchLimitExceeded {
                length: total,
                limit: max_batch_bytes,
            });
        }
        let after_length = total.checked_sub(12).ok_or(EncodeError::LengthOverflow {
            kind: "record batch",
            length: total,
            maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
        })?;
        let batch_length =
            i32::try_from(after_length).map_err(|_| EncodeError::LengthOverflow {
                kind: "record batch",
                length: total,
                maximum: usize::try_from(i32::MAX).unwrap_or(usize::MAX),
            })?;
        buffer[start + BATCH_LENGTH_OFFSET..start + BATCH_LENGTH_OFFSET + 4]
            .copy_from_slice(&batch_length.to_be_bytes());
        let crc = crc32c::crc32c(&buffer[start + CRC_COVERAGE_START..start + total]);
        buffer[start + CRC_OFFSET..start + CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        Ok(total)
    }
}

pub(crate) fn complete_batch_len(records_bytes: usize) -> Result<usize, RecordError> {
    FIXED_BATCH_BYTES.checked_add(records_bytes).ok_or_else(|| {
        EncodeError::LengthOverflow {
            kind: "record batch",
            length: usize::MAX,
            maximum: MAX_PROTOCOL_BATCH_BYTES,
        }
        .into()
    })
}

fn write_header(buffer: &mut BytesMut, batch: &RecordBatch, record_count: i32) {
    buffer.extend_from_slice(&batch.base_offset.to_be_bytes());
    buffer.extend_from_slice(&0_i32.to_be_bytes());
    buffer.extend_from_slice(&batch.partition_leader_epoch.to_be_bytes());
    buffer.extend_from_slice(&MAGIC_V2.to_be_bytes());
    buffer.extend_from_slice(&0_u32.to_be_bytes());
    buffer.extend_from_slice(
        &Attributes {
            compression: batch.compression,
            timestamp_type: batch.timestamp_type,
            is_transactional: batch.is_transactional,
            is_control: batch.is_control,
            has_delete_horizon: batch.has_delete_horizon,
        }
        .encode()
        .to_be_bytes(),
    );
    buffer.extend_from_slice(&batch.last_offset_delta.to_be_bytes());
    buffer.extend_from_slice(&batch.base_timestamp.to_be_bytes());
    buffer.extend_from_slice(&batch.max_timestamp.to_be_bytes());
    buffer.extend_from_slice(&batch.producer_id.to_be_bytes());
    buffer.extend_from_slice(&batch.producer_epoch.to_be_bytes());
    buffer.extend_from_slice(&batch.base_sequence.to_be_bytes());
    buffer.extend_from_slice(&record_count.to_be_bytes());
}
