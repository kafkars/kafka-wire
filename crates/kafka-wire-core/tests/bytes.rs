//! Exact byte stories for legacy and compact byte-string encodings.
//!
//! Each test pins the precise wire layout, proves the read path returns the same
//! payload, and (via `encoded_len` on a one-field message) checks the sizing
//! target predicts the same length the buffer wrote, so `encoded_len` stays
//! exact for byte fields.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_core::{
    ApiVersion, DecodeLimits, Decoder, EncodeError, EncodeTarget, Encoder, KafkaEncode,
};

/// A four-byte payload whose bytes are easy to spot in a hex dump.
const PAYLOAD: &[u8] = &[0xde, 0xad, 0xbe, 0xef];

const VERSION: ApiVersion = ApiVersion::new(0);

/// One optional byte field encoded in one of the four byte layouts.
///
/// Routing every case through `KafkaEncode` runs the same writer through the
/// buffer and the sizing target, so `encoded_len` is asserted against the bytes
/// actually written rather than recomputed by hand.
#[derive(Debug)]
enum Field {
    Legacy(Bytes),
    LegacyNullable(Option<Bytes>),
    Compact(Bytes),
    CompactNullable(Option<Bytes>),
}

impl KafkaEncode for Field {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        _version: ApiVersion,
    ) -> Result<(), EncodeError> {
        match self {
            Self::Legacy(value) => encoder.write_bytes(value),
            Self::LegacyNullable(value) => {
                encoder.write_nullable_bytes(value.as_ref().map(Bytes::as_ref))
            }
            Self::Compact(value) => encoder.write_compact_bytes(value),
            Self::CompactNullable(value) => {
                encoder.write_compact_nullable_bytes(value.as_ref().map(Bytes::as_ref))
            }
        }
    }
}

/// Encodes `field`, asserts the exact bytes, and asserts sizing agrees.
fn assert_wire(field: &Field, expected: &[u8]) -> Bytes {
    let bytes = field.encode_to_bytes(VERSION).unwrap();
    assert_eq!(bytes.as_ref(), expected);
    assert_eq!(field.encoded_len(VERSION).unwrap(), expected.len());
    bytes
}

fn decoder(bytes: Bytes) -> Decoder {
    Decoder::new(bytes, DecodeLimits::default())
}

fn frame(bytes: &[u8]) -> Decoder {
    decoder(Bytes::copy_from_slice(bytes))
}

#[test]
fn legacy_bytes_prefixes_an_int32_length() {
    let bytes = assert_wire(
        &Field::Legacy(Bytes::from_static(PAYLOAD)),
        &[0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef],
    );

    let mut decoder = decoder(bytes);
    assert_eq!(decoder.read_bytes().unwrap().as_ref(), PAYLOAD);
    decoder.finish().unwrap();
}

#[test]
fn legacy_nullable_bytes_encodes_null_as_minus_one() {
    let bytes = assert_wire(&Field::LegacyNullable(None), &[0xff, 0xff, 0xff, 0xff]);

    let mut decoder = decoder(bytes);
    assert_eq!(decoder.read_nullable_bytes().unwrap(), None);
    decoder.finish().unwrap();
}

#[test]
fn legacy_nullable_bytes_round_trips_a_present_payload() {
    let bytes = assert_wire(
        &Field::LegacyNullable(Some(Bytes::from_static(PAYLOAD))),
        &[0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef],
    );

    let mut decoder = decoder(bytes);
    assert_eq!(
        decoder.read_nullable_bytes().unwrap().unwrap().as_ref(),
        PAYLOAD
    );
    decoder.finish().unwrap();
}

#[test]
fn compact_bytes_prefixes_a_varint_of_length_plus_one() {
    let bytes = assert_wire(
        &Field::Compact(Bytes::from_static(PAYLOAD)),
        &[0x05, 0xde, 0xad, 0xbe, 0xef],
    );

    let mut decoder = decoder(bytes);
    assert_eq!(decoder.read_compact_bytes().unwrap().as_ref(), PAYLOAD);
    decoder.finish().unwrap();
}

#[test]
fn compact_bytes_encodes_empty_as_varint_one() {
    let bytes = assert_wire(&Field::Compact(Bytes::new()), &[0x01]);

    let mut decoder = decoder(bytes);
    assert!(decoder.read_compact_bytes().unwrap().is_empty());
    decoder.finish().unwrap();
}

#[test]
fn compact_nullable_bytes_encodes_null_as_varint_zero() {
    let bytes = assert_wire(&Field::CompactNullable(None), &[0x00]);

    let mut decoder = decoder(bytes);
    assert_eq!(decoder.read_compact_nullable_bytes().unwrap(), None);
    decoder.finish().unwrap();
}

#[test]
fn compact_nullable_bytes_round_trips_a_present_payload() {
    let bytes = assert_wire(
        &Field::CompactNullable(Some(Bytes::from_static(PAYLOAD))),
        &[0x05, 0xde, 0xad, 0xbe, 0xef],
    );

    let mut decoder = decoder(bytes);
    let value = decoder.read_compact_nullable_bytes().unwrap().unwrap();
    assert_eq!(value.as_ref(), PAYLOAD);
    decoder.finish().unwrap();
}

#[test]
fn a_bytes_length_beyond_the_frame_is_rejected_before_slicing() {
    // int32 length 8 with only four payload bytes behind it.
    let error = frame(&[0x00, 0x00, 0x00, 0x08, 0xde, 0xad, 0xbe, 0xef])
        .read_bytes()
        .unwrap_err();

    assert!(matches!(
        error,
        kafka_wire_core::DecodeError::UnexpectedEnd {
            needed: 8,
            remaining: 4,
            ..
        }
    ));
}

#[test]
fn a_negative_non_null_bytes_length_is_malformed() {
    let error = frame(&[0xff, 0xff, 0xff, 0xfe]).read_bytes().unwrap_err();

    assert!(matches!(
        error,
        kafka_wire_core::DecodeError::NegativeLength { kind: "bytes", .. }
    ));
}

#[test]
fn a_zero_copy_bytes_read_shares_the_input_allocation() {
    // The read must slice the frame, not heap-copy it. Two reads of the same
    // frame therefore point at the same backing bytes.
    let frame = Bytes::from(vec![0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef]);
    let mut decoder = decoder(frame);
    let read = decoder.read_bytes().unwrap();
    assert_eq!(read.as_ref(), PAYLOAD);
}
