//! Exact byte story for the eight-byte IEEE-754 double.
//!
//! The vectors are the canonical big-endian bit patterns for a few values whose
//! encoding is well known, so a byte-order or width mistake is visible at once.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeLimits, Decoder, Encoder};

fn wire(value: f64) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_float64(value).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_float64(value).unwrap();
    assert_eq!(sizer.len(), buffer.len(), "float64 sizing diverged");

    buffer.to_vec()
}

fn decoder(bytes: &[u8]) -> Decoder {
    Decoder::new(Bytes::copy_from_slice(bytes), DecodeLimits::default()).unwrap()
}

#[test]
fn one_point_zero_is_the_canonical_big_endian_pattern() {
    let bytes = wire(1.0);
    assert_eq!(bytes, [0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

    assert_eq!(
        decoder(&bytes).read_float64().unwrap().to_bits(),
        1.0_f64.to_bits()
    );
}

#[test]
fn a_negative_double_carries_its_sign_in_the_high_byte() {
    let bytes = wire(-2.0);
    assert_eq!(bytes, [0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

    assert_eq!(
        decoder(&bytes).read_float64().unwrap().to_bits(),
        (-2.0_f64).to_bits()
    );
}

#[test]
fn several_doubles_round_trip_bit_for_bit() {
    for value in [
        0.0_f64,
        -0.0,
        1.0,
        -1.0,
        0.5,
        1_234.567_89,
        f64::MAX,
        f64::MIN,
    ] {
        let bytes = wire(value);
        let decoded = decoder(&bytes).read_float64().unwrap();
        assert_eq!(decoded.to_bits(), value.to_bits());
    }
}

#[test]
fn non_finite_doubles_round_trip() {
    for value in [f64::INFINITY, f64::NEG_INFINITY] {
        let bytes = wire(value);
        assert_eq!(
            decoder(&bytes).read_float64().unwrap().to_bits(),
            value.to_bits()
        );
    }

    let nan = wire(f64::NAN);
    assert!(decoder(&nan).read_float64().unwrap().is_nan());
}

#[test]
fn a_truncated_double_is_rejected() {
    let seven = [0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00];
    let error = decoder(&seven).read_float64().unwrap_err();
    assert!(matches!(
        error,
        kafka_wire_core::DecodeError::UnexpectedEnd {
            needed: 8,
            remaining: 7,
            ..
        }
    ));
}
