//! Exact byte stories for nullable array-length prefixes.
//!
//! These prefixes are the counterpart the audit found missing in both
//! directions. The legacy form spells null as `int32 -1`; the compact form
//! spells it as the varint `0`, with a present count stored as `count + 1`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{BoundedCount, DecodeError, DecodeLimits, Decoder, Encoder};

fn wire_legacy(length: Option<usize>) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_nullable_array_len(length).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_nullable_array_len(length).unwrap();
    assert_eq!(sizer.len(), buffer.len(), "legacy sizing diverged");

    buffer.to_vec()
}

fn wire_compact(length: Option<usize>) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_compact_nullable_array_len(length).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_compact_nullable_array_len(length).unwrap();
    assert_eq!(sizer.len(), buffer.len(), "compact sizing diverged");

    buffer.to_vec()
}

fn decoder(bytes: &[u8]) -> Decoder {
    // A short backing frame is enough: the count in these vectors is small
    // enough that the remaining-bytes check accepts it.
    let mut frame = bytes.to_vec();
    frame.resize(bytes.len() + 8, 0);
    Decoder::new(Bytes::from(frame), DecodeLimits::default()).unwrap()
}

#[test]
fn legacy_null_is_int32_minus_one() {
    let bytes = wire_legacy(None);
    assert_eq!(bytes, [0xff, 0xff, 0xff, 0xff]);
    assert_eq!(decoder(&bytes).read_nullable_array_len().unwrap(), None);
}

#[test]
fn legacy_present_count_is_a_plain_int32() {
    let bytes = wire_legacy(Some(2));
    assert_eq!(bytes, [0x00, 0x00, 0x00, 0x02]);
    assert_eq!(
        decoder(&bytes)
            .read_nullable_array_len()
            .unwrap()
            .map(BoundedCount::get),
        Some(2)
    );
}

#[test]
fn compact_null_is_varint_zero() {
    let bytes = wire_compact(None);
    assert_eq!(bytes, [0x00]);
    assert_eq!(
        decoder(&bytes).read_compact_nullable_array_len().unwrap(),
        None
    );
}

#[test]
fn compact_present_count_is_stored_plus_one() {
    let bytes = wire_compact(Some(2));
    assert_eq!(bytes, [0x03]);
    assert_eq!(
        decoder(&bytes)
            .read_compact_nullable_array_len()
            .unwrap()
            .map(BoundedCount::get),
        Some(2)
    );
}

#[test]
fn a_legacy_length_below_minus_one_is_malformed() {
    let error = Decoder::new(
        Bytes::from_static(&[0xff, 0xff, 0xff, 0xfe]),
        DecodeLimits::default(),
    )
    .unwrap()
    .read_nullable_array_len()
    .unwrap_err();
    assert!(matches!(
        error,
        DecodeError::NegativeLength {
            kind: "nullable array",
            ..
        }
    ));
}

#[test]
fn a_present_count_is_bounded_without_assuming_an_element_width() {
    // An array element may have zero wire width in some schema version. The
    // prefix therefore proves only the configured count budget; `read_vec`
    // grows after each successful element instead of reserving from this count.
    let count = Decoder::new(
        Bytes::from_static(&[0x00, 0x00, 0x00, 0x64]),
        DecodeLimits::default(),
    )
    .unwrap()
    .read_nullable_array_len()
    .unwrap()
    .unwrap();
    assert_eq!(count.get(), 100);
}
