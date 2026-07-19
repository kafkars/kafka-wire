//! Exact byte story for the sixteen-byte big-endian UUID.
//!
//! A UUID is written verbatim, so the vector is the sixteen bytes in order. The
//! all-zero sentinel and the round trip are pinned alongside the sizing check.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeLimits, Decoder, Encoder, Uuid};

/// A UUID whose bytes ascend, so a reordering would be obvious in the vector.
const SAMPLE: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];

fn wire(uuid: Uuid) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_uuid(uuid).unwrap();

    let mut sizer = Encoder::sizing();
    sizer.write_uuid(uuid).unwrap();
    assert_eq!(sizer.len(), buffer.len(), "uuid sizing diverged");

    buffer.to_vec()
}

fn decoder(bytes: &[u8]) -> Decoder {
    Decoder::new(Bytes::copy_from_slice(bytes), DecodeLimits::default())
}

#[test]
fn uuid_is_sixteen_big_endian_bytes_in_order() {
    let bytes = wire(Uuid::from_bytes(SAMPLE));
    assert_eq!(bytes, SAMPLE);

    let decoded = decoder(&bytes).read_uuid().unwrap();
    assert_eq!(decoded.to_bytes(), SAMPLE);
    assert!(!decoded.is_zero());
}

#[test]
fn the_zero_uuid_is_sixteen_zero_bytes() {
    let bytes = wire(Uuid::ZERO);
    assert_eq!(bytes, [0x00; 16]);

    let decoded = decoder(&bytes).read_uuid().unwrap();
    assert_eq!(decoded, Uuid::ZERO);
    assert!(decoded.is_zero());
    assert_eq!(Uuid::default(), Uuid::ZERO);
}

#[test]
fn a_truncated_uuid_is_rejected() {
    let error = decoder(&SAMPLE[..15]).read_uuid().unwrap_err();
    assert!(matches!(
        error,
        kafka_wire_core::DecodeError::UnexpectedEnd {
            needed: 16,
            remaining: 15,
            ..
        }
    ));
}
