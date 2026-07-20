//! Record-batch decoding is bounded, cursor-preserving, and strict about malformed fields.
//!
//! These scenarios exercise parser obligations rather than byte authority; the
//! Apache Kafka corpus in `kafka-wire-conformance` owns the canonical byte layouts.

#![allow(clippy::field_reassign_with_default, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordDecodeLimits, RecordError, RecordHeader, TimestampType,
};

const CRC_START: usize = 21;
const CRC_RANGE: std::ops::Range<usize> = 17..21;
const RECORD_COUNT_RANGE: std::ops::Range<usize> = 57..61;
const NULL_RECORD_KEY_OFFSET: usize = 65;
const EMPTY_HEADER_KEY_OFFSET: usize = 68;

fn batch(compression: Compression, base_offset: i64) -> RecordBatch {
    RecordBatch {
        base_offset,
        partition_leader_epoch: 2,
        compression,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp: 1_000,
        max_timestamp: 1_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: Some(Bytes::from(vec![b'x'; 1_024])),
            headers: Vec::new(),
        }],
    }
}

fn rewrite_crc(bytes: &mut [u8]) {
    let crc = crc32c::crc32c(&bytes[CRC_START..]);
    bytes[CRC_RANGE].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn decoding_one_batch_leaves_the_next_batch_on_the_cursor() {
    let first = batch(Compression::None, 10).encode_to_bytes().unwrap();
    let second = batch(Compression::None, 20).encode_to_bytes().unwrap();
    let mut joined = BytesMut::new();
    joined.extend_from_slice(&first);
    joined.extend_from_slice(&second);
    let mut cursor = joined.freeze();

    let decoded_first = RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()).unwrap();
    assert_eq!(decoded_first.base_offset, 10);
    assert_eq!(cursor.as_ref(), second.as_ref());

    let decoded_second = RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()).unwrap();
    assert_eq!(decoded_second.base_offset, 20);
    assert!(cursor.is_empty());
}

#[test]
fn a_failed_decode_does_not_advance_the_cursor() {
    let mut cursor = batch(Compression::None, 10).encode_to_bytes().unwrap();
    let original = cursor.clone();
    let mut limits = RecordDecodeLimits::default();
    limits.max_batch_bytes = cursor.len() - 1;

    assert!(matches!(
        RecordBatch::decode(&mut cursor, limits),
        Err(RecordError::BatchLimitExceeded { .. })
    ));
    assert_eq!(cursor, original);
}

#[test]
fn every_codec_obeys_the_decompressed_byte_limit() {
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let mut cursor = batch(compression, 10).encode_to_bytes().unwrap();
        let original = cursor.clone();
        let mut limits = RecordDecodeLimits::default();
        limits.max_decompressed_records_bytes = 32;

        assert!(
            matches!(
                RecordBatch::decode(&mut cursor, limits),
                Err(RecordError::DecompressionLimitExceeded { .. })
            ),
            "{} ignored the decompression limit",
            compression.name()
        );
        assert_eq!(
            cursor,
            original,
            "{} advanced on failure",
            compression.name()
        );
    }
}

#[test]
fn a_negative_record_count_is_rejected() {
    let mut bytes = batch(Compression::None, 10)
        .encode_to_bytes()
        .unwrap()
        .to_vec();
    bytes[RECORD_COUNT_RANGE].copy_from_slice(&(-1_i32).to_be_bytes());
    rewrite_crc(&mut bytes);
    let mut cursor = Bytes::from(bytes);

    assert!(matches!(
        RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()),
        Err(RecordError::NegativeRecordCount { count: -1 })
    ));
}

#[test]
fn a_null_record_header_key_is_rejected_instead_of_becoming_empty() {
    let mut source = batch(Compression::None, 10);
    source.records[0].value = None;
    source.records[0].headers = vec![RecordHeader {
        key: String::new(),
        value: None,
    }];
    let mut bytes = source.encode_to_bytes().unwrap().to_vec();
    assert_eq!(bytes[EMPTY_HEADER_KEY_OFFSET], 0);
    bytes[EMPTY_HEADER_KEY_OFFSET] = 1;
    rewrite_crc(&mut bytes);
    let mut cursor = Bytes::from(bytes);

    assert!(matches!(
        RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()),
        Err(RecordError::NullHeaderKey)
    ));
}

#[test]
fn a_record_field_length_below_the_null_sentinel_is_rejected() {
    let mut source = batch(Compression::None, 10);
    source.records[0].value = None;
    let mut bytes = source.encode_to_bytes().unwrap().to_vec();
    assert_eq!(bytes[NULL_RECORD_KEY_OFFSET], 1);
    // Signed varints use zigzag encoding: wire value 3 represents -2.
    bytes[NULL_RECORD_KEY_OFFSET] = 3;
    rewrite_crc(&mut bytes);
    let mut cursor = Bytes::from(bytes);

    assert!(matches!(
        RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()),
        Err(RecordError::InvalidRecordFieldLength { length: -2 })
    ));
}
