//! Exact record-batch boundary discovery from the fixed twelve-byte prefix.
//!
//! This module owns signed-length validation before a wire decoder sees the
//! batch. It deliberately does not parse the batch body or advance the caller's
//! cursor.

use bytes::Bytes;
use kafka_wire_core::DecodeError;

use crate::RecordError;

const PREFIX_BYTES: usize = 12;
/// Bytes from `partition_leader_epoch` through `records_count`.
const HEADER_AFTER_LENGTH: usize = 49;

pub(super) fn exact_batch(bytes: &Bytes, max_batch_bytes: usize) -> Result<Bytes, RecordError> {
    if bytes.len() < PREFIX_BYTES {
        return Err(DecodeError::UnexpectedEnd {
            offset: 0,
            needed: PREFIX_BYTES,
            remaining: bytes.len(),
        }
        .into());
    }

    let batch_length = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let declared = usize::try_from(batch_length).map_err(|_| RecordError::NegativeBatchLength {
        length: batch_length,
    })?;
    let available = bytes.len() - PREFIX_BYTES;
    if declared < HEADER_AFTER_LENGTH || declared > available {
        return Err(RecordError::TruncatedBatch {
            declared,
            available,
        });
    }
    let end = PREFIX_BYTES
        .checked_add(declared)
        .ok_or(RecordError::BatchLimitExceeded {
            length: usize::MAX,
            limit: max_batch_bytes,
        })?;
    if end > max_batch_bytes {
        return Err(RecordError::BatchLimitExceeded {
            length: end,
            limit: max_batch_bytes,
        });
    }
    Ok(bytes.slice(..end))
}
