//! Exact byte stories for a tagged-field section carrying both kinds of tag.
//!
//! A flexible message ends in one section holding two populations: the tags this
//! build knows, which it encodes from typed values, and the tags it does not,
//! which it carries verbatim so a message survives a round trip through a build
//! older than its peer. The wire format has a single ascending ordering across
//! the whole section, so the two must interleave — which is the one claim these
//! vectors exist to pin down.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{
    DecodeError, DecodeLimits, Decoder, EncodeError, EncodeTarget, Encoder, KnownTags, TagOutcome,
    TaggedField, TaggedFields,
};
use std::cell::Cell;

/// A section of one entry: tag 0, one byte of payload `0xaa`.
const UNKNOWN_TAG_0: &[u8] = &[0x01, 0x00, 0x01, 0xaa];

fn unknown(tag: u32, data: &'static [u8]) -> TaggedFields {
    TaggedFields::from_sorted(vec![TaggedField::new(tag, Bytes::from_static(data))]).unwrap()
}

/// Writes a section and asserts the sizing target agreed with the bytes.
fn wire(known: impl Fn() -> KnownTags, unknown: &TaggedFields) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder
        .write_merged_tagged_fields(known(), unknown, known_payload)
        .unwrap();

    let mut sizer = Encoder::sizing();
    sizer
        .write_merged_tagged_fields(known(), unknown, known_payload)
        .unwrap();
    assert_eq!(sizer.len(), buffer.len(), "merged sizing diverged");

    buffer.to_vec()
}

fn known_payload<T: EncodeTarget>(_tag: u32, encoder: &mut Encoder<T>) -> Result<(), EncodeError> {
    encoder.write_i16(7)
}

fn decoder(bytes: &[u8]) -> Decoder {
    Decoder::new(Bytes::copy_from_slice(bytes), DecodeLimits::default()).unwrap()
}

#[test]
fn a_known_tag_is_written_after_a_lower_unknown_one() {
    // The whole reason the two populations merge rather than concatenate. Tag 0
    // is unknown and tag 1 is known, so the unknown entry has to come first.
    let bytes = wire(
        || {
            let mut known = KnownTags::new();
            known.measure(1, |encoder| encoder.write_i16(7)).unwrap();
            known
        },
        &unknown(0, &[0xaa]),
    );

    assert_eq!(
        bytes,
        [
            0x02, // two entries
            0x00, 0x01, 0xaa, // tag 0, one byte, retained verbatim
            0x01, 0x02, 0x00, 0x07, // tag 1, two bytes, the known int16
        ]
    );
}

#[test]
fn a_known_tag_is_written_before_a_higher_unknown_one() {
    let bytes = wire(
        || {
            let mut known = KnownTags::new();
            known.measure(1, |encoder| encoder.write_i16(7)).unwrap();
            known
        },
        &unknown(9, &[0xaa]),
    );

    assert_eq!(bytes, [0x02, 0x01, 0x02, 0x00, 0x07, 0x09, 0x01, 0xaa]);
}

#[test]
fn a_section_of_only_unknown_tags_is_byte_identical_to_the_plain_writer() {
    // `write_merged_tagged_fields` with nothing known must not perturb the
    // bytes a build that knows no tags at all would have written.
    let retained = unknown(0, &[0xaa]);
    let merged = wire(KnownTags::new, &retained);

    let mut buffer = BytesMut::new();
    Encoder::new(&mut buffer)
        .write_tagged_fields(&retained)
        .unwrap();

    assert_eq!(merged, buffer.to_vec());
    assert_eq!(merged, UNKNOWN_TAG_0);
}

#[test]
fn a_known_tag_colliding_with_a_retained_one_is_named() {
    let mut known = KnownTags::new();
    known.claim(0).unwrap();

    let mut buffer = BytesMut::new();
    let error = Encoder::new(&mut buffer)
        .write_merged_tagged_fields(known, &unknown(0, &[0xaa]), known_payload)
        .unwrap_err();

    assert!(
        matches!(error, EncodeError::TaggedFieldsInvalid(_)),
        "a tag claimed by both populations must be named: {error}"
    );
    assert!(
        buffer.is_empty(),
        "the collision must be rejected before output"
    );
}

#[test]
fn a_claimed_default_tag_is_not_counted_or_written() {
    let bytes = wire(
        || {
            let mut known = KnownTags::new();
            known.claim(1).unwrap();
            known
        },
        &unknown(9, &[0xaa]),
    );

    assert_eq!(bytes, [0x01, 0x09, 0x01, 0xaa]);
}

#[test]
fn claiming_one_schema_tag_twice_is_a_duplicate() {
    let mut known = KnownTags::new();
    known.claim(1).unwrap();

    assert!(matches!(
        known.claim(1),
        Err(EncodeError::TaggedFieldsInvalid(_))
    ));
}

#[test]
fn a_sizing_target_accounts_for_a_measured_payload_without_replaying_it() {
    let traversals = Cell::new(0_usize);
    let mut known = KnownTags::new();
    known
        .measure(1, |encoder| {
            traversals.set(traversals.get() + 1);
            encoder.write_i16(7)
        })
        .unwrap();

    let mut sizer = Encoder::sizing();
    sizer
        .write_merged_tagged_fields(known, &TaggedFields::default(), |_, encoder| {
            traversals.set(traversals.get() + 1);
            encoder.write_i16(7)
        })
        .unwrap();

    assert_eq!(sizer.len(), 5);
    assert_eq!(
        traversals.get(),
        1,
        "the measured payload must not be replayed into SizeTarget"
    );
}

#[test]
fn a_known_payload_must_write_the_length_preflight_measured() {
    let mut known = KnownTags::new();
    known.measure(1, |encoder| encoder.write_i16(7)).unwrap();
    let mut buffer = BytesMut::new();
    let error = Encoder::new(&mut buffer)
        .write_merged_tagged_fields(known, &TaggedFields::default(), |_, encoder| {
            encoder.write_i32(7)
        })
        .unwrap_err();

    assert_eq!(
        error,
        EncodeError::SizeMismatch {
            predicted: 2,
            actual: 4,
        }
    );
}

#[test]
fn dispatch_decodes_the_known_tag_and_retains_the_rest() {
    let section = [
        0x02, // two entries
        0x00, 0x01, 0xaa, // tag 0, unknown to this dispatch
        0x01, 0x02, 0x00, 0x07, // tag 1, known
    ];

    let mut value = 0_i16;
    let retained = decoder(&section)
        .read_tagged_fields_with(|tag, entry| match tag {
            1 => {
                value = entry.read_i16()?;
                Ok(TagOutcome::Decoded)
            }
            _ => Ok(TagOutcome::Retained),
        })
        .unwrap();

    assert_eq!(value, 7);
    assert_eq!(retained.len(), 1);
    let kept = retained.iter().next().unwrap();
    assert_eq!(kept.tag(), 0);
    assert_eq!(kept.data().as_ref(), [0xaa]);
}

#[test]
fn a_decoded_tag_leaves_nothing_behind_in_the_retained_set() {
    // The entry this build understands must not also be forwarded as unknown,
    // or re-encoding would write it twice.
    let retained = decoder(&[0x01, 0x01, 0x02, 0x00, 0x07])
        .read_tagged_fields_with(|_, entry| {
            entry.read_i16()?;
            Ok(TagOutcome::Decoded)
        })
        .unwrap();

    assert!(retained.is_empty());
}

#[test]
fn a_value_shorter_than_its_declared_size_is_named() {
    // Tag 1 declares four bytes; this build reads it as an int16. The two
    // disagree about the tag's schema, which is reported rather than absorbed
    // by skipping the remainder.
    let error = decoder(&[0x01, 0x01, 0x04, 0x00, 0x07, 0x00, 0x00])
        .read_tagged_fields_with(|_, entry| {
            entry.read_i16()?;
            Ok(TagOutcome::Decoded)
        })
        .unwrap_err();

    assert!(
        matches!(
            error,
            DecodeError::TaggedFieldSize {
                tag: 1,
                size: 4,
                consumed: 2,
                ..
            }
        ),
        "an under-consumed entry must name the tag and both sizes: {error}"
    );
}

#[test]
fn a_value_reaching_past_its_declared_size_cannot_see_the_next_entry() {
    // Tag 0 declares two bytes and tag 1 follows. A known reader for tag 0 that
    // asks for four must fail on its own entry rather than consuming the
    // neighbour's tag and length as payload.
    let error = decoder(&[0x02, 0x00, 0x02, 0x00, 0x07, 0x01, 0x02, 0x00, 0x09])
        .read_tagged_fields_with(|tag, entry| {
            if tag == 0 {
                entry.read_i32()?;
                return Ok(TagOutcome::Decoded);
            }
            Ok(TagOutcome::Retained)
        })
        .unwrap_err();

    assert!(
        matches!(error, DecodeError::UnexpectedEnd { needed: 4, .. }),
        "an over-reading entry must be bounded by its own payload: {error}"
    );
}

#[test]
fn a_section_survives_the_round_trip_that_knows_none_of_it() {
    let retained = decoder(UNKNOWN_TAG_0).read_tagged_fields().unwrap();
    assert_eq!(wire(KnownTags::new, &retained), UNKNOWN_TAG_0);
}

#[test]
fn retained_tag_lookup_uses_the_ordered_numeric_identity() {
    let fields = TaggedFields::from_sorted(vec![
        TaggedField::new(1, Bytes::from_static(&[0xaa])),
        TaggedField::new(3, Bytes::from_static(&[0xbb])),
    ])
    .unwrap();

    assert!(fields.contains_tag(1));
    assert!(!fields.contains_tag(2));
    assert!(fields.contains_tag(3));
}
