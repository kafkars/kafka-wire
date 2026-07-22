//! Absolute batch framing limits are enforced before output allocation.
//!
//! Scenarios: an unbounded caller limit clamps to Kafka's signed length field,
//! and arithmetic overflow is a named error rather than a sentinel length.

use kafka_wire_core::EncodeError;

use crate::{
    RecordEncodeLimits, batch_encode::complete_batch_len, limits::MAX_PROTOCOL_BATCH_BYTES,
};

#[test]
fn caller_limits_cannot_exceed_kafkas_batch_length_field() {
    let limits = RecordEncodeLimits::new(usize::MAX, usize::MAX);
    assert_eq!(
        limits.effective_max_encoded_batch_bytes(),
        MAX_PROTOCOL_BATCH_BYTES
    );
}

#[test]
fn complete_batch_length_overflow_is_explicit() {
    assert_eq!(
        complete_batch_len(usize::MAX),
        Err(EncodeError::LengthOverflow {
            kind: "record batch",
            length: usize::MAX,
            maximum: MAX_PROTOCOL_BATCH_BYTES,
        }
        .into())
    );
}
