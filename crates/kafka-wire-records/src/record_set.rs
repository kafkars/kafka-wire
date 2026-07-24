//! Ordered record-list encoding and declared-count verification.
//!
//! One record's field framing remains in `record.rs`. This file owns the batch
//! payload seam where a declared count must agree with the complete run and no
//! trailing record bytes may remain.

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeLimits, Decoder, EncodeError, Encoder};

use crate::{Record, RecordError};

/// Computes the exact uncompressed record run before any batch allocation.
pub(crate) fn encoded_len_all(records: &[Record], limit: usize) -> Result<usize, RecordError> {
    let mut length = 0_usize;
    for record in records {
        length = length.checked_add(record.encoded_length()?).ok_or(
            RecordError::UncompressedRecordsLimitExceeded {
                length: usize::MAX,
                limit,
            },
        )?;
        if length > limit {
            return Err(RecordError::UncompressedRecordsLimitExceeded { length, limit });
        }
    }
    Ok(length)
}

/// Appends all records to `buffer`, for the batch encoder.
pub(crate) fn encode_all(records: &[Record], buffer: &mut BytesMut) -> Result<(), EncodeError> {
    let mut encoder = Encoder::new(buffer);
    for record in records {
        record.encode(&mut encoder)?;
    }
    Ok(())
}

/// Reads exactly `count` records, refusing a payload that holds a different
/// number than the batch header promised.
pub(crate) fn decode_all(
    payload: Bytes,
    count: usize,
    limits: DecodeLimits,
) -> Result<Vec<Record>, RecordError> {
    let mut decoder = Decoder::new(payload, limits)?;
    let mut records = Vec::with_capacity(count.min(decoder.remaining()));
    for _ in 0..count {
        if decoder.remaining() == 0 {
            return Err(RecordError::RecordCountMismatch {
                declared: count,
                actual: records.len(),
            });
        }
        records.push(Record::decode(&mut decoder)?);
    }
    if decoder.remaining() != 0 {
        return Err(RecordError::TrailingRecordBytes {
            bytes: decoder.remaining(),
        });
    }
    Ok(records)
}

/// Counts the visible payload spans retained by decoded records.
pub(crate) fn retained_payload_bytes(
    records: &[Record],
    limit: usize,
) -> Result<usize, RecordError> {
    let mut length = 0_usize;
    for record in records {
        length = add_retained(length, record.key.as_ref().map_or(0, Bytes::len), limit)?;
        length = add_retained(length, record.value.as_ref().map_or(0, Bytes::len), limit)?;
        for header in &record.headers {
            length = add_retained(length, header.key.len(), limit)?;
            length = add_retained(length, header.value.as_ref().map_or(0, Bytes::len), limit)?;
        }
    }
    if length > limit {
        return Err(RecordError::RetainedPayloadLimitExceeded { length, limit });
    }
    Ok(length)
}

fn add_retained(current: usize, added: usize, limit: usize) -> Result<usize, RecordError> {
    let length = current
        .checked_add(added)
        .ok_or(RecordError::RetainedPayloadLimitExceeded {
            length: usize::MAX,
            limit,
        })?;
    Ok(length)
}
