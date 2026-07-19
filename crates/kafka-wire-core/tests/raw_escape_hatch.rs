//! The public raw-byte escape hatch: `write_raw_slice` and `take_bytes`.
//!
//! A downstream primitive (records, a future codec) must be able to emit and
//! claim raw byte runs from outside this crate. These stories prove the hatch is
//! public, that emit and claim compose into a round trip, that the write is
//! counted by the sizing target, and that the read is bounded by the remainder.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeError, DecodeLimits, Decoder, Encoder};

const PAYLOAD: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05];

#[test]
fn a_raw_slice_is_written_verbatim_with_no_prefix() {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_raw_slice(PAYLOAD).unwrap();
    assert_eq!(buffer.as_ref(), PAYLOAD);
}

#[test]
fn the_sizing_target_counts_a_raw_slice() {
    let mut sizer = Encoder::sizing();
    sizer.write_raw_slice(PAYLOAD).unwrap();
    assert_eq!(sizer.len(), PAYLOAD.len());
}

#[test]
fn write_raw_slice_then_take_bytes_round_trips_from_outside_the_crate() {
    let mut buffer = BytesMut::new();
    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_i32(7).unwrap();
    encoder.write_raw_slice(PAYLOAD).unwrap();

    let mut decoder = Decoder::new(buffer.freeze(), DecodeLimits::default());
    assert_eq!(decoder.read_i32().unwrap(), 7);
    let claimed = decoder.take_bytes(PAYLOAD.len()).unwrap();
    assert_eq!(claimed.as_ref(), PAYLOAD);
    decoder.finish().unwrap();
}

#[test]
fn take_bytes_returns_a_zero_copy_slice_of_the_input() {
    let frame = Bytes::from(PAYLOAD.to_vec());
    let mut decoder = Decoder::new(frame, DecodeLimits::default());
    let claimed = decoder.take_bytes(PAYLOAD.len()).unwrap();
    assert_eq!(claimed.as_ref(), PAYLOAD);
}

#[test]
fn take_bytes_is_bounded_by_the_remaining_frame() {
    let mut decoder = Decoder::new(Bytes::from_static(&[0xaa, 0xbb]), DecodeLimits::default());
    let error = decoder.take_bytes(8).unwrap_err();
    assert!(matches!(
        error,
        DecodeError::UnexpectedEnd {
            needed: 8,
            remaining: 2,
            offset: 0,
        }
    ));
}
