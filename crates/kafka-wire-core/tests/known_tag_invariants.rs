//! Inline known-tag storage and measurement invariants.
//!
//! These scenarios prove fixed capacity, explicit claims, and the shared
//! sizing/write contract independently from section ordering.

#![allow(clippy::unwrap_used)]

use std::cell::Cell;

use bytes::BytesMut;
use kafka_wire_core::{EncodeError, Encoder, KnownTags, TaggedFields, TaggedFieldsError};

#[test]
fn claiming_one_schema_tag_twice_is_a_duplicate() {
    let mut known = KnownTags::<1>::new();
    known.claim(1).unwrap();

    assert_eq!(
        known.claim(1),
        Err(EncodeError::TaggedFieldsInvalid(
            TaggedFieldsError::Duplicate { tag: 1 }
        ))
    );
}

#[test]
fn fixed_capacity_exhaustion_is_named_without_allocating() {
    let mut known = KnownTags::<1>::new();
    known.claim(1).unwrap();

    assert_eq!(
        known.claim(2),
        Err(EncodeError::KnownTagCapacityExceeded { capacity: 1 })
    );
}

#[test]
fn measuring_an_unclaimed_tag_is_a_named_contract_failure() {
    let mut known = KnownTags::<1>::new();

    assert_eq!(
        known.measure(1, |encoder| encoder.write_i16(7)),
        Err(EncodeError::UnclaimedKnownTag { tag: 1 })
    );
}

#[test]
fn a_sizing_target_accounts_for_a_measured_payload_without_replaying_it() {
    let traversals = Cell::new(0_usize);
    let mut known = KnownTags::<1>::new();
    known.claim(1).unwrap();
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
    assert_eq!(traversals.get(), 1, "SizeTarget replayed the payload");
}

#[test]
fn a_known_payload_must_write_the_length_preflight_measured() {
    let mut known = KnownTags::<1>::new();
    known.claim(1).unwrap();
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
