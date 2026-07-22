//! Batch metadata cannot reinterpret bytes the implementation does not own.
//!
//! Scenarios: unknown attribute bits fail closed and bytes after the declared
//! record count report their exact remainder rather than a fictional count.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordDecodeLimits, RecordEncodeLimits, RecordError,
    TimestampType,
};

const CRC_START: usize = 21;
const CRC_RANGE: std::ops::Range<usize> = 17..21;
const ATTRIBUTES_RANGE: std::ops::Range<usize> = 21..23;
const RECORD_COUNT_RANGE: std::ops::Range<usize> = 57..61;

fn batch() -> RecordBatch {
    RecordBatch {
        base_offset: 10,
        last_offset_delta: 0,
        partition_leader_epoch: 2,
        compression: Compression::None,
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
            value: Some(Bytes::from_static(b"x")),
            headers: Vec::new(),
        }],
    }
}

fn encoded() -> Vec<u8> {
    batch()
        .encode_to_bytes(RecordEncodeLimits::default())
        .unwrap()
        .to_vec()
}

fn rewrite_crc(bytes: &mut [u8]) {
    let crc = crc32c::crc32c(&bytes[CRC_START..]);
    bytes[CRC_RANGE].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn unknown_batch_attribute_bits_fail_closed() {
    let mut bytes = encoded();
    bytes[ATTRIBUTES_RANGE].copy_from_slice(&0x0080_i16.to_be_bytes());
    rewrite_crc(&mut bytes);
    let original = Bytes::from(bytes);
    let mut cursor = original.clone();

    assert_eq!(
        RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()).unwrap_err(),
        RecordError::UnknownBatchAttributes { bits: 0x0080 }
    );
    assert_eq!(cursor, original);
}

#[test]
fn trailing_record_payload_bytes_report_the_exact_remainder() {
    let mut bytes = encoded();
    bytes[RECORD_COUNT_RANGE].copy_from_slice(&0_i32.to_be_bytes());
    let trailing = bytes.len() - 61;
    rewrite_crc(&mut bytes);
    let mut cursor = Bytes::from(bytes);

    assert_eq!(
        RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()).unwrap_err(),
        RecordError::TrailingRecordBytes { bytes: trailing }
    );
}
