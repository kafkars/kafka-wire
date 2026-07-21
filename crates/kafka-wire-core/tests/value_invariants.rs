//! Construction stories for validated public value types.
//!
//! Invalid UTF-8 and reversed version ranges are rejected at construction, and
//! decoded strings retain the original `Bytes` allocation.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_core::{DecodeLimits, Decoder, StrBytes, VersionRange};

#[test]
fn decoded_strings_keep_the_frame_storage() {
    let frame = Bytes::from_static(b"\x00\x04wire");
    let payload_pointer = frame.as_ptr().wrapping_add(2);
    let value = Decoder::new(frame, DecodeLimits::default())
        .unwrap()
        .read_string()
        .unwrap();

    let storage = value.into_bytes();
    assert_eq!(storage.as_ref(), b"wire");
    assert_eq!(storage.as_ptr(), payload_pointer);
}

#[test]
fn invalid_utf8_cannot_construct_a_string_value() {
    assert!(StrBytes::try_from(Bytes::from_static(&[0xff])).is_err());
}

#[test]
fn dynamic_version_ranges_reject_reversed_bounds() {
    assert_eq!(VersionRange::try_new(4, 3), None);
    assert_eq!(VersionRange::try_new(3, 4), Some(VersionRange::new(3, 4)));
}

#[test]
#[should_panic(expected = "version range minimum exceeds maximum")]
fn constant_version_ranges_cannot_hide_reversed_bounds() {
    let _ = VersionRange::new(4, 3);
}
