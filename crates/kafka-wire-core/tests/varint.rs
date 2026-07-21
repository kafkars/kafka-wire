//! Exact byte stories for signed (zigzag) and unsigned base-128 integers.
//!
//! Zigzag maps small-magnitude negatives to short encodings, so the vectors here
//! lean on negative values: their exact bytes are the property most likely to be
//! wrong. Each writer is also checked against the sizing target, and the decoder
//! is shown rejecting overlong and over-length encodings.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeError, DecodeLimits, Decoder, Encoder};

fn wire_varint(value: i32) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_varint(value).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_varint(value).unwrap();
    assert_eq!(sizer.len(), buffer.len(), "varint sizing diverged");

    buffer.to_vec()
}

fn wire_varlong(value: i64) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_varlong(value).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_varlong(value).unwrap();
    assert_eq!(sizer.len(), buffer.len(), "varlong sizing diverged");

    buffer.to_vec()
}

fn wire_unsigned_varlong(value: u64) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_unsigned_varlong(value).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_unsigned_varlong(value).unwrap();
    assert_eq!(
        sizer.len(),
        buffer.len(),
        "unsigned varlong sizing diverged"
    );

    buffer.to_vec()
}

fn decoder(bytes: &[u8]) -> Decoder {
    Decoder::new(Bytes::copy_from_slice(bytes), DecodeLimits::default()).unwrap()
}

#[test]
fn signed_varint_maps_small_negatives_to_one_byte() {
    assert_eq!(wire_varint(0), [0x00]);
    assert_eq!(wire_varint(-1), [0x01]);
    assert_eq!(wire_varint(1), [0x02]);
    assert_eq!(wire_varint(-2), [0x03]);
    assert_eq!(wire_varint(2), [0x04]);
    // -64 is the largest-magnitude negative still fitting in one byte.
    assert_eq!(wire_varint(-64), [0x7f]);
}

#[test]
fn signed_varint_spills_to_a_second_byte_at_minus_sixty_five() {
    assert_eq!(wire_varint(-65), [0x81, 0x01]);
}

#[test]
fn signed_varint_encodes_a_multi_byte_negative() {
    assert_eq!(wire_varint(-1_000_000), [0xff, 0x88, 0x7a]);
}

#[test]
fn signed_varint_encodes_the_extreme_values() {
    assert_eq!(wire_varint(i32::MAX), [0xfe, 0xff, 0xff, 0xff, 0x0f]);
    assert_eq!(wire_varint(i32::MIN), [0xff, 0xff, 0xff, 0xff, 0x0f]);
}

#[test]
fn signed_varint_round_trips_across_the_sign_boundary() {
    for value in [0, 1, -1, 63, -64, 64, -65, i32::MAX, i32::MIN, -1_000_000] {
        let bytes = wire_varint(value);
        assert_eq!(decoder(&bytes).read_varint().unwrap(), value);
    }
}

#[test]
fn signed_varlong_maps_small_negatives_to_one_byte() {
    assert_eq!(wire_varlong(0), [0x00]);
    assert_eq!(wire_varlong(-1), [0x01]);
    assert_eq!(wire_varlong(1), [0x02]);
}

#[test]
fn signed_varlong_encodes_the_extreme_values() {
    assert_eq!(
        wire_varlong(i64::MAX),
        [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
    assert_eq!(
        wire_varlong(i64::MIN),
        [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
}

#[test]
fn signed_varlong_round_trips_across_the_sign_boundary() {
    for value in [0, 1, -1, 300, -300, i64::MAX, i64::MIN, i64::from(i32::MIN)] {
        let bytes = wire_varlong(value);
        assert_eq!(decoder(&bytes).read_varlong().unwrap(), value);
    }
}

#[test]
fn unsigned_varlong_encodes_up_to_ten_bytes() {
    assert_eq!(wire_unsigned_varlong(0), [0x00]);
    assert_eq!(wire_unsigned_varlong(300), [0xac, 0x02]);
    assert_eq!(
        wire_unsigned_varlong(u64::MAX),
        [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
}

#[test]
fn unsigned_varlong_round_trips_the_full_width() {
    for value in [0, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
        let bytes = wire_unsigned_varlong(value);
        assert_eq!(decoder(&bytes).read_unsigned_varlong().unwrap(), value);
    }
}

#[test]
fn a_two_byte_encoding_of_zero_is_rejected_as_overlong() {
    // 0x80 0x00 spells zero in two bytes; a canonical writer emits 0x00.
    let error = decoder(&[0x80, 0x00]).read_unsigned_varlong().unwrap_err();
    assert!(matches!(error, DecodeError::MalformedVarint { offset: 0 }));

    let error = decoder(&[0x80, 0x00]).read_varint().unwrap_err();
    assert!(matches!(error, DecodeError::MalformedVarint { offset: 0 }));
}

#[test]
fn a_varlong_past_ten_bytes_is_rejected() {
    // Ten continuation bytes never terminate.
    let never_ends = [0x80_u8; 10];
    let error = decoder(&never_ends).read_unsigned_varlong().unwrap_err();
    assert!(matches!(error, DecodeError::MalformedVarint { offset: 0 }));
}

#[test]
fn a_varlong_whose_final_byte_overflows_u64_is_rejected() {
    // Nine 0xff bytes fill the low 63 bits; a final 0x02 would set bit 64.
    let overflow = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
    let error = decoder(&overflow).read_unsigned_varlong().unwrap_err();
    assert!(matches!(error, DecodeError::MalformedVarint { offset: 0 }));
}

#[test]
fn a_varint_beyond_the_u32_domain_is_rejected() {
    // A canonical five-byte varint whose value exceeds u32 cannot be a zigzag i32.
    let too_wide = [0xff, 0xff, 0xff, 0xff, 0x1f];
    let error = decoder(&too_wide).read_varint().unwrap_err();
    assert!(matches!(error, DecodeError::MalformedVarint { offset: 0 }));
}
