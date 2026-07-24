//! Fetch record sets distinguish complete batches from a bounded partial tail.

#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_core::{DecodeLimits, StrBytes};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordBatchDecode, RecordDecodeLimits, RecordEncodeLimits,
    RecordError, RecordHeader, TimestampType,
};

fn record_with_payload() -> Record {
    Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
        headers: vec![
            RecordHeader {
                key: StrBytes::from("dup"),
                value: Some(Bytes::from_static(b"a")),
            },
            RecordHeader {
                key: StrBytes::from("dup"),
                value: Some(Bytes::from_static(b"bc")),
            },
        ],
    }
}

fn batch_with_record(compression: Compression, base_offset: i64, record: Record) -> RecordBatch {
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
        records: vec![record],
    }
}

fn encoded_record(compression: Compression, base_offset: i64, record: Record) -> Bytes {
    batch_with_record(compression, base_offset, record)
        .encode_to_bytes(RecordEncodeLimits::default())
        .unwrap()
}

#[test]
fn uncompressed_fields_borrow_input_and_consume_no_additional_payload_budget() {
    let mut cursor = encoded_record(Compression::None, 10, record_with_payload());
    let input = cursor.clone();
    let input_start = input.as_ptr() as usize;
    let input_end = input_start + input.len();
    let result = RecordBatch::decode_next(&mut cursor, RecordDecodeLimits::default(), 0).unwrap();

    let RecordBatchDecode::Complete {
        batch,
        additional_retained_payload_bytes,
        ..
    } = result
    else {
        panic!("complete uncompressed batch became a partial tail");
    };
    assert_eq!(batch.base_offset, 10);
    assert_eq!(additional_retained_payload_bytes, 0);
    let key = batch.records[0].key.as_ref().unwrap();
    let value = batch.records[0].value.as_ref().unwrap();
    for retained in [key, value] {
        let retained_start = retained.as_ptr() as usize;
        assert!(retained_start >= input_start);
        assert!(retained_start + retained.len() <= input_end);
    }
    assert!(cursor.is_empty());
}

#[test]
fn every_compressed_codec_debits_exact_visible_payload_spans() {
    const RETAINED_PAYLOAD_BYTES: usize = 17;
    for compression in [
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let encoded = encoded_record(compression, 10, record_with_payload());
        let mut cursor = encoded.clone();
        let result = RecordBatch::decode_next(
            &mut cursor,
            RecordDecodeLimits::default(),
            RETAINED_PAYLOAD_BYTES,
        )
        .unwrap();
        let RecordBatchDecode::Complete {
            additional_retained_payload_bytes,
            ..
        } = result
        else {
            panic!("complete compressed batch became a partial tail");
        };
        assert_eq!(additional_retained_payload_bytes, RETAINED_PAYLOAD_BYTES);
        assert!(cursor.is_empty());

        let mut cursor = encoded.clone();
        assert_eq!(
            RecordBatch::decode_next(
                &mut cursor,
                RecordDecodeLimits::default(),
                RETAINED_PAYLOAD_BYTES - 1,
            ),
            Err(RecordError::RetainedPayloadLimitExceeded {
                length: RETAINED_PAYLOAD_BYTES,
                limit: RETAINED_PAYLOAD_BYTES - 1,
            })
        );
        assert_eq!(cursor, encoded);
    }
}

#[test]
fn compressed_null_only_record_retains_no_payload_bytes() {
    let record = Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: None,
        value: None,
        headers: Vec::new(),
    };
    for compression in [
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let mut cursor = encoded_record(compression, 10, record.clone());
        let result =
            RecordBatch::decode_next(&mut cursor, RecordDecodeLimits::default(), 0).unwrap();
        let RecordBatchDecode::Complete {
            additional_retained_payload_bytes,
            ..
        } = result
        else {
            panic!("complete compressed batch became a partial tail");
        };
        assert_eq!(additional_retained_payload_bytes, 0);
        assert!(cursor.is_empty());
    }
}

#[test]
fn incomplete_prefix_and_declared_body_are_partial_and_cursor_preserving() {
    let complete = encoded_record(Compression::None, 10, record_with_payload());
    for retained in [complete.slice(..7), complete.slice(..complete.len() - 1)] {
        let expected = retained.len();
        let mut cursor = retained.clone();
        assert_eq!(
            RecordBatch::decode_next(&mut cursor, RecordDecodeLimits::default(), usize::MAX)
                .unwrap(),
            RecordBatchDecode::PartialTrailing { bytes: expected }
        );
        assert_eq!(cursor, retained);
    }
}

#[test]
fn malformed_or_over_limit_prefixes_are_not_partial_tails() {
    let mut undersized = vec![0_u8; 12];
    undersized[8..12].copy_from_slice(&1_i32.to_be_bytes());
    let mut undersized = Bytes::from(undersized);
    let original = undersized.clone();
    let limits = RecordDecodeLimits::new(12, 1_024, DecodeLimits::default());
    assert_eq!(
        RecordBatch::decode_next(&mut undersized, limits, usize::MAX),
        Err(RecordError::TruncatedBatch {
            declared: 1,
            available: 0,
        })
    );
    assert_eq!(undersized, original);

    let mut over_limit = encoded_record(Compression::None, 10, record_with_payload()).slice(..12);
    let original = over_limit.clone();
    let limits = RecordDecodeLimits::new(12, 1_024, DecodeLimits::default());
    assert!(matches!(
        RecordBatch::decode_next(&mut over_limit, limits, usize::MAX),
        Err(RecordError::BatchLimitExceeded { .. })
    ));
    assert_eq!(over_limit, original);

    let mut negative = BytesMut::zeroed(12);
    negative[8..12].copy_from_slice(&(-1_i32).to_be_bytes());
    let mut negative = negative.freeze();
    let original = negative.clone();
    assert_eq!(
        RecordBatch::decode_next(&mut negative, RecordDecodeLimits::default(), usize::MAX,),
        Err(RecordError::NegativeBatchLength { length: -1 })
    );
    assert_eq!(negative, original);
}

#[test]
fn corrupt_complete_batch_is_not_a_partial_tail_and_preserves_cursor() {
    let encoded = encoded_record(Compression::None, 10, record_with_payload());
    let mut corrupt = BytesMut::from(encoded.as_ref());
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xff;
    let mut cursor = corrupt.freeze();
    let original = cursor.clone();

    assert!(matches!(
        RecordBatch::decode_next(&mut cursor, RecordDecodeLimits::default(), usize::MAX,),
        Err(RecordError::CorruptBatch { .. })
    ));
    assert_eq!(cursor, original);
}

#[test]
fn strict_decode_keeps_reporting_an_incomplete_tail_as_an_error() {
    let complete = encoded_record(Compression::None, 10, record_with_payload());
    let mut cursor = complete.slice(..complete.len() - 1);
    assert!(matches!(
        RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()),
        Err(RecordError::TruncatedBatch { .. })
    ));
}
