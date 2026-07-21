//! Peer-controlled collection counts stay bounded without driving reservations.
//!
//! Array counts are opaque tokens limited by configuration and `read_vec` grows
//! only after an element decodes. Tagged-field counts additionally have a known
//! minimum wire width, so they can be rejected against the unread frame.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_core::{BoundedCount, DecodeError, DecodeLimits, Decoder, TaggedFields};

/// Legacy array claiming 1,000,000 elements with two unread bytes behind it.
const AMPLIFYING_LEGACY_ARRAY: &[u8] = &[0x00, 0x0f, 0x42, 0x40, 0x00, 0x00];

/// Compact form of the same claim: `varint(1_000_001)` then two unread bytes.
const AMPLIFYING_COMPACT_ARRAY: &[u8] = &[0xc1, 0x84, 0x3d, 0x00, 0x00];

/// Tagged-field header claiming 4,096 fields with nothing behind it.
const AMPLIFYING_TAGGED_COUNT: &[u8] = &[0x80, 0x20];

fn decoder(frame: &[u8]) -> Decoder {
    Decoder::new(Bytes::copy_from_slice(frame), DecodeLimits::default()).unwrap()
}

fn legacy_array_count(frame: &[u8]) -> Result<BoundedCount, DecodeError> {
    decoder(frame).read_array_len()
}

fn tagged_fields(frame: &[u8]) -> Result<TaggedFields, DecodeError> {
    decoder(frame).read_tagged_fields()
}

fn legacy_array_frame(claimed: usize, payload: usize) -> Vec<u8> {
    let claimed = i32::try_from(claimed).unwrap();
    let mut frame = Vec::with_capacity(4 + payload);
    frame.extend_from_slice(&claimed.to_be_bytes());
    frame.resize(4 + payload, 0);
    frame
}

#[test]
fn legacy_array_count_is_opaque_and_does_not_reserve_from_the_prefix() {
    let mut decoder = decoder(AMPLIFYING_LEGACY_ARRAY);
    let count = decoder.read_array_len().unwrap();
    assert_eq!(count.get(), 1_000_000);

    let error = decoder.read_vec(count, Decoder::read_i8).unwrap_err();
    assert_eq!(
        error,
        DecodeError::UnexpectedEnd {
            needed: 1,
            remaining: 0,
            offset: 6,
        }
    );
}

#[test]
fn compact_array_count_is_opaque_and_does_not_reserve_from_the_prefix() {
    let mut decoder = decoder(AMPLIFYING_COMPACT_ARRAY);
    let count = decoder.read_compact_array_len().unwrap();
    assert_eq!(count.get(), 1_000_000);

    let error = decoder.read_vec(count, Decoder::read_i8).unwrap_err();
    assert!(matches!(
        error,
        DecodeError::UnexpectedEnd {
            needed: 1,
            remaining: 0,
            ..
        }
    ));
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
fn array_counts_do_not_assume_a_one_byte_element_width() {
    let count = legacy_array_count(&legacy_array_frame(9, 8)).unwrap();
    assert_eq!(count.get(), 9);
}

#[test]
fn array_counts_are_still_rejected_by_the_configured_budget() {
    let frame = legacy_array_frame(1_000_001, 0);
    let error = legacy_array_count(&frame).unwrap_err();
    assert_eq!(
        error,
        DecodeError::LimitExceeded {
            kind: "array",
            length: 1_000_001,
            limit: 1_000_000,
            offset: 0,
        }
    );
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
