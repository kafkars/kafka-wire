//! Record-batch decoding is bounded, cursor-preserving, and strict about malformed fields.
//!
//! These scenarios exercise parser obligations rather than byte authority; the
//! Apache Kafka corpus in `kafka-wire-conformance` owns the canonical byte layouts.

#![allow(clippy::field_reassign_with_default, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeError, StrBytes};
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
        last_offset_delta: 0,
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
            value: Some(Bytes::from(vec![b'x'; 512])),
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
fn concatenated_batches_do_not_share_one_wire_frame_budget() {
    let first = batch(Compression::None, 10).encode_to_bytes().unwrap();
    let second = batch(Compression::None, 20).encode_to_bytes().unwrap();
    let mut joined = BytesMut::new();
    joined.extend_from_slice(&first);
    joined.extend_from_slice(&second);
    let mut cursor = joined.freeze();
    let mut limits = RecordDecodeLimits::default();
    limits.wire.max_frame_bytes = first.len();

    assert!(cursor.len() > limits.wire.max_frame_bytes);
    assert_eq!(
        RecordBatch::decode(&mut cursor, limits)
            .unwrap()
            .base_offset,
        10
    );
    assert_eq!(cursor, second);
}

#[test]
fn the_record_layer_outer_budgets_supersede_the_wire_frame_budget() {
    let mut cursor = batch(Compression::None, 10).encode_to_bytes().unwrap();
    let mut limits = RecordDecodeLimits::default();
    limits.wire.max_frame_bytes = 1;

    assert_eq!(
        RecordBatch::decode(&mut cursor, limits)
            .unwrap()
            .base_offset,
        10
    );
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

        let error = RecordBatch::decode(&mut cursor, limits).unwrap_err();
        assert!(
            matches!(error, RecordError::DecompressionLimitExceeded { .. })
                || matches!(
                    (compression, &error),
                    (
                        Compression::Zstd,
                        RecordError::CompressionFailed { codec: "zstd", .. }
                    )
                ),
            "{} ignored the decompression limit: {error}",
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
        key: StrBytes::default(),
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

#[test]
fn a_record_count_above_the_element_budget_is_rejected_before_allocation() {
    let mut cursor = batch(Compression::None, 10).encode_to_bytes().unwrap();
    let original = cursor.clone();
    let mut limits = RecordDecodeLimits::default();
    limits.wire.max_array_elements = 0;

    assert!(matches!(
        RecordBatch::decode(&mut cursor, limits),
        Err(RecordError::Wire(DecodeError::LimitExceeded {
            kind: "record batch records",
            length: 1,
            limit: 0,
            ..
        }))
    ));
    assert_eq!(cursor, original);
}

#[test]
fn a_header_count_above_the_element_budget_is_rejected_before_allocation() {
    let mut source = batch(Compression::None, 10);
    source.records[0].headers = vec![
        RecordHeader {
            key: StrBytes::from("first"),
            value: None,
        },
        RecordHeader {
            key: StrBytes::from("second"),
            value: None,
        },
    ];
    let mut cursor = source.encode_to_bytes().unwrap();
    let original = cursor.clone();
    let mut limits = RecordDecodeLimits::default();
    limits.wire.max_array_elements = 1;

    assert!(matches!(
        RecordBatch::decode(&mut cursor, limits),
        Err(RecordError::Wire(DecodeError::LimitExceeded {
            kind: "record headers",
            length: 2,
            limit: 1,
            ..
        }))
    ));
    assert_eq!(cursor, original);
}
