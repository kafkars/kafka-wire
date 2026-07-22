//! Ordered record-list encoding and declared-count verification.
//!
//! One record's field framing remains in `record.rs`. This file owns the batch
//! payload seam where a declared count must agree with the complete run and no
//! trailing record bytes may remain.

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeLimits, Decoder, EncodeError, Encoder};

use crate::{Record, RecordError};

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
        // Kafka's own reader stops at the declared count, so trailing bytes are
        // a peer writing more records than it counted. Naming it keeps a
        // truncated read from passing as a complete one.
        return Err(RecordError::RecordCountMismatch {
            declared: count,
            actual: records.len() + 1,
        });
    }
    Ok(records)
}
