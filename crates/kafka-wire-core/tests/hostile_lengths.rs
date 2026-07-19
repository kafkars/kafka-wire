//! A peer-controlled element count must be backed by bytes that actually remain.
//!
//! Every array element and every tagged field occupies at least one wire byte,
//! so a count larger than the unread remainder is malformed no matter how the
//! element limit is configured. These stories pin that rejection at its exact
//! boundary and show that the count handed to a generated `Vec::with_capacity`
//! can never exceed the frame that carried it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_core::{DecodeError, DecodeLimits, Decoder, TaggedFields};

/// Legacy array claiming 1,000,000 elements with two unread bytes behind it.
///
/// The count sits exactly on the default `max_array_elements` budget, so the
/// configured limit alone accepts it.
const AMPLIFYING_LEGACY_ARRAY: &[u8] = &[0x00, 0x0f, 0x42, 0x40, 0x00, 0x00];

/// Compact form of the same claim: `varint(1_000_001)` then two unread bytes.
const AMPLIFYING_COMPACT_ARRAY: &[u8] = &[0xc1, 0x84, 0x3d, 0x00, 0x00];

/// Tagged-field header claiming 4,096 fields with nothing behind it.
const AMPLIFYING_TAGGED_COUNT: &[u8] = &[0x80, 0x20];

/// Returns the count a generated legacy decoder would pass to `Vec::with_capacity`.
fn legacy_array_count(frame: &[u8]) -> Result<usize, DecodeError> {
    let mut decoder = Decoder::new(Bytes::copy_from_slice(frame), DecodeLimits::default());
    decoder.read_array_len()
}

/// Returns the count a generated flexible decoder would pass to `Vec::with_capacity`.
fn compact_array_count(frame: &[u8]) -> Result<usize, DecodeError> {
    let mut decoder = Decoder::new(Bytes::copy_from_slice(frame), DecodeLimits::default());
    decoder.read_compact_array_len()
}

fn tagged_fields(frame: &[u8]) -> Result<TaggedFields, DecodeError> {
    let mut decoder = Decoder::new(Bytes::copy_from_slice(frame), DecodeLimits::default());
    decoder.read_tagged_fields()
}

/// Builds a legacy array frame that claims `claimed` elements over `payload` bytes.
fn legacy_array_frame(claimed: usize, payload: usize) -> Vec<u8> {
    let claimed = i32::try_from(claimed).unwrap();
    let mut frame = Vec::with_capacity(4 + payload);
    frame.extend_from_slice(&claimed.to_be_bytes());
    frame.resize(4 + payload, 0);
    frame
}

#[test]
fn legacy_array_count_beyond_the_frame_is_rejected_at_the_prefix() {
    let error = legacy_array_count(AMPLIFYING_LEGACY_ARRAY).unwrap_err();

    assert_eq!(
        error,
        DecodeError::CountExceedsFrame {
            kind: "array",
            count: 1_000_000,
            remaining: 2,
            offset: 0,
        }
    );
}

#[test]
fn compact_array_count_beyond_the_frame_is_rejected_at_the_prefix() {
    let error = compact_array_count(AMPLIFYING_COMPACT_ARRAY).unwrap_err();

    assert_eq!(
        error,
        DecodeError::CountExceedsFrame {
            kind: "compact array",
            count: 1_000_000,
            remaining: 2,
            offset: 0,
        }
    );
}

#[test]
fn tagged_field_count_beyond_the_frame_is_rejected_at_the_prefix() {
    let error = tagged_fields(AMPLIFYING_TAGGED_COUNT).unwrap_err();

    assert_eq!(
        error,
        DecodeError::CountExceedsFrame {
            kind: "tagged field count",
            count: 4_096,
            remaining: 0,
            offset: 0,
        }
    );
}

#[test]
fn an_array_count_equal_to_the_remaining_bytes_is_still_accepted() {
    let frame = legacy_array_frame(8, 8);

    assert_eq!(legacy_array_count(&frame).unwrap(), 8);
}

#[test]
fn one_element_past_the_remaining_bytes_is_rejected() {
    let frame = legacy_array_frame(9, 8);

    assert_eq!(
        legacy_array_count(&frame).unwrap_err(),
        DecodeError::CountExceedsFrame {
            kind: "array",
            count: 9,
            remaining: 8,
            offset: 0,
        }
    );
}

/// The allocation-amplification property, stated as a sweep over claimed counts.
///
/// `kafka-wire-core` forbids `unsafe_code`, so a counting global allocator cannot
/// live here. The accepted count is the exact argument the generated decoder
/// hands to `Vec::with_capacity`, so bounding it bounds the reservation.
#[test]
fn an_accepted_array_count_never_exceeds_the_frame_that_carried_it() {
    let payload = 8_usize;

    for claimed in 0..2_000_usize {
        let frame = legacy_array_frame(claimed, payload);
        match legacy_array_count(&frame) {
            Ok(count) => assert!(
                count <= payload,
                "count {count} reserves past the {payload} bytes that remain"
            ),
            Err(_) => assert!(
                claimed > payload,
                "count {claimed} fits in {payload} bytes but was rejected"
            ),
        }
    }
}

#[test]
fn an_accepted_tagged_field_count_never_exceeds_the_frame_that_carried_it() {
    for claimed in 0..127_usize {
        let mut frame = vec![u8::try_from(claimed).unwrap()];
        frame.resize(1 + claimed * 3, 0);

        let remaining = frame.len() - 1;
        match tagged_fields(&frame) {
            Ok(fields) => assert!(fields.len() <= remaining),
            Err(error) => assert!(
                !matches!(error, DecodeError::CountExceedsFrame { .. }),
                "count {claimed} fits in {remaining} bytes but was rejected as unbacked"
            ),
        }
    }
}
