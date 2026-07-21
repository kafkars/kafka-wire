//! Record bodies and fields remain inside their declared byte and policy limits.
//!
//! Scenarios: a short record body cannot read its successor, negative and
//! trailing sizes get precise diagnostics, and every key/value flavor enforces
//! the caller's byte or string budget at its own prefix offset.

#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeError, DecodeLimits, Decoder, Encoder, StrBytes};
use kafka_wire_records::{Record, RecordError, RecordHeader};

fn empty_record() -> Record {
    Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: None,
        value: None,
        headers: Vec::new(),
    }
}

fn encoded(record: &Record) -> Bytes {
    let mut bytes = BytesMut::new();
    record.encode(&mut Encoder::new(&mut bytes)).unwrap();
    bytes.freeze()
}

fn decode(bytes: Bytes, limits: DecodeLimits) -> Result<Record, RecordError> {
    let mut decoder = Decoder::new(bytes, limits).unwrap();
    let record = Record::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(record)
}

fn assert_limit(record: &Record, limits: DecodeLimits, expected_kind: &'static str) -> usize {
    let bytes = encoded(record);
    let error = decode(bytes.clone(), limits).unwrap_err();
    let RecordError::Wire(DecodeError::LimitExceeded {
        kind,
        length,
        limit,
        offset,
    }) = error
    else {
        panic!("{expected_kind} produced the wrong error: {error}");
    };
    assert_eq!(kind, expected_kind);
    assert_eq!(length, 2);
    assert_eq!(limit, 1);
    // Signed varint length 2 is zigzag-encoded as wire byte 4. This proves the
    // diagnostic points at the field prefix rather than the end of its payload.
    assert_eq!(bytes[offset], 4);
    offset
}

#[test]
fn a_short_declared_body_cannot_read_the_next_record() {
    let first = encoded(&empty_record());
    let second = encoded(&empty_record());
    assert_eq!(first[0] & 1, 0, "the fixture length is not positive");
    let mut joined = BytesMut::new();
    joined.extend_from_slice(&first);
    joined.extend_from_slice(&second);
    joined[0] = 0;
    let joined = joined.freeze();
    let mut decoder = Decoder::new(joined.clone(), DecodeLimits::default()).unwrap();

    let error = Record::decode(&mut decoder).unwrap_err();

    assert!(matches!(
        error,
        RecordError::Wire(DecodeError::UnexpectedEnd {
            offset: 1,
            needed: 1,
            remaining: 0,
        })
    ));
    assert_eq!(
        decoder.remaining(),
        joined.len() - 1,
        "the bounded child consumed bytes beyond its zero-byte declaration"
    );
}

#[test]
fn a_negative_record_length_is_named_at_its_prefix() {
    let mut bytes = encoded(&empty_record()).to_vec();
    bytes[0] = 1; // Zigzag encoding of -1.
    let mut decoder = Decoder::new(Bytes::from(bytes), DecodeLimits::default()).unwrap();

    assert_eq!(
        Record::decode(&mut decoder).unwrap_err(),
        RecordError::NegativeRecordLength {
            length: -1,
            offset: 0,
        }
    );
}

#[test]
fn trailing_record_body_bytes_report_the_consumed_size() {
    let mut bytes = encoded(&empty_record()).to_vec();
    let declared = usize::from(bytes[0] / 2);
    bytes[0] += 2; // One additional positive byte in zigzag encoding.
    bytes.push(0);

    assert_eq!(
        decode(Bytes::from(bytes), DecodeLimits::default()).unwrap_err(),
        RecordError::RecordSizeMismatch {
            declared: declared + 1,
            consumed: declared,
        }
    );
}

#[test]
fn record_keys_values_and_header_values_obey_the_byte_budget() {
    let mut limits = DecodeLimits::default();
    limits.max_bytes_bytes = 1;

    let mut key = empty_record();
    key.key = Some(Bytes::from_static(b"ab"));
    assert_limit(&key, limits, "record key");

    let mut value = empty_record();
    value.value = Some(Bytes::from_static(b"ab"));
    assert_limit(&value, limits, "record value");

    let mut header = empty_record();
    header.headers.push(RecordHeader {
        key: StrBytes::from("k"),
        value: Some(Bytes::from_static(b"ab")),
    });
    assert_limit(&header, limits, "record header value");
}

#[test]
fn record_header_keys_obey_the_string_budget_and_report_utf8_payload_offsets() {
    let mut record = empty_record();
    record.headers.push(RecordHeader {
        key: StrBytes::from("ab"),
        value: None,
    });
    let mut limits = DecodeLimits::default();
    limits.max_string_bytes = 1;
    let prefix_offset = assert_limit(&record, limits, "record header key");

    let mut malformed = encoded(&record).to_vec();
    malformed[prefix_offset + 1] = 0xff;
    assert!(matches!(
        decode(Bytes::from(malformed), DecodeLimits::default()),
        Err(RecordError::Wire(DecodeError::InvalidUtf8 {
            offset,
            valid_up_to: 0,
        })) if offset == prefix_offset + 1
    ));
}
