//! Adversarial stories for frame, byte-field, and nested tagged-field budgets.
//!
//! These tests prove limits are enforced at the public decoder boundary and
//! diagnostics from bounded child decoders retain offsets in the outer frame.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_core::{DecodeError, DecodeLimits, Decoder, TagOutcome};

type ByteRead = fn(&mut Decoder) -> Result<(), DecodeError>;

#[test]
fn an_oversized_frame_is_rejected_before_parsing() {
    let mut limits = DecodeLimits::default();
    limits.max_frame_bytes = 2;

    let error = Decoder::new(Bytes::from_static(b"abc"), limits).unwrap_err();
    assert_eq!(
        error,
        DecodeError::LimitExceeded {
            kind: "frame",
            length: 3,
            limit: 2,
            offset: 0,
        }
    );
}

#[test]
fn every_byte_field_regime_obeys_the_byte_budget() {
    let mut limits = DecodeLimits::default();
    limits.max_bytes_bytes = 1;

    let cases: &[(&[u8], ByteRead, &str)] = &[
        (&[0, 0, 0, 2, 0xaa, 0xbb], legacy, "bytes"),
        (&[0, 0, 0, 2, 0xaa, 0xbb], legacy_nullable, "nullable bytes"),
        (&[3, 0xaa, 0xbb], compact, "compact bytes"),
        (&[3, 0xaa, 0xbb], compact_nullable, "compact nullable bytes"),
    ];

    for (wire, read, kind) in cases {
        let mut decoder = Decoder::new(Bytes::copy_from_slice(wire), limits).unwrap();
        let error = read(&mut decoder).unwrap_err();
        assert_eq!(
            error,
            DecodeError::LimitExceeded {
                kind,
                length: 2,
                limit: 1,
                offset: 0,
            }
        );
    }
}

fn legacy(decoder: &mut Decoder) -> Result<(), DecodeError> {
    decoder.read_bytes().map(drop)
}

fn legacy_nullable(decoder: &mut Decoder) -> Result<(), DecodeError> {
    decoder.read_nullable_bytes().map(drop)
}

fn compact(decoder: &mut Decoder) -> Result<(), DecodeError> {
    decoder.read_compact_bytes().map(drop)
}

fn compact_nullable(decoder: &mut Decoder) -> Result<(), DecodeError> {
    decoder.read_compact_nullable_bytes().map(drop)
}

#[test]
fn a_known_tag_reports_its_absolute_outer_frame_offset() {
    // Two prefix bytes, then one tag whose one-byte payload begins at byte 5.
    let frame = Bytes::from_static(&[0x00, 0x00, 0x01, 0x01, 0x01, 0xaa]);
    let mut decoder = Decoder::new(frame, DecodeLimits::default()).unwrap();
    assert_eq!(decoder.read_i16().unwrap(), 0);

    let error = decoder
        .read_tagged_fields_with(|_, entry| {
            entry.read_i32()?;
            Ok(TagOutcome::Decoded)
        })
        .unwrap_err();

    assert_eq!(
        error,
        DecodeError::UnexpectedEnd {
            needed: 4,
            remaining: 1,
            offset: 5,
        }
    );
}
