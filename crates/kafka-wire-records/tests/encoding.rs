//! Record-batch encoding is preflighted, bounded, and transactional.
//!
//! Scenarios: exact limits succeed, either budget fails one byte below the
//! requirement, streaming codecs cannot cross their cap, and every failure
//! restores a caller-owned output buffer byte for byte.

#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordDecodeLimits, RecordEncodeLimits, RecordError,
    TimestampType,
};

fn batch(compression: Compression) -> RecordBatch {
    RecordBatch {
        base_offset: 7,
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
            value: Some(Bytes::from(vec![b'x'; 4_096])),
            headers: Vec::new(),
        }],
    }
}

fn codecs() -> [Compression; 5] {
    [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ]
}

#[test]
fn every_codec_accepts_its_exact_encoded_limit_and_round_trips() {
    for compression in codecs() {
        let source = batch(compression);
        let reference = source
            .encode_to_bytes(RecordEncodeLimits::default())
            .unwrap();
        let limits = RecordEncodeLimits::new(usize::MAX, reference.len());
        let encoded = source.encode_to_bytes(limits).unwrap();
        assert_eq!(encoded, reference, "{} was not stable", compression.name());

        let mut cursor = encoded;
        let decoded = RecordBatch::decode(&mut cursor, RecordDecodeLimits::default()).unwrap();
        assert_eq!(decoded, source, "{} did not round trip", compression.name());
        assert!(cursor.is_empty());
    }
}

#[test]
fn uncompressed_preflight_rejects_one_byte_under_the_exact_record_size() {
    let source = batch(Compression::None);
    let encoded = source
        .encode_to_bytes(RecordEncodeLimits::default())
        .unwrap();
    let records_bytes = encoded.len() - 61;
    let limits = RecordEncodeLimits::new(records_bytes - 1, usize::MAX);
    let mut output = BytesMut::from(&b"prefix"[..]);
    let original = output.clone();

    assert_eq!(
        source.encode_into(&mut output, limits).unwrap_err(),
        RecordError::UncompressedRecordsLimitExceeded {
            length: records_bytes,
            limit: records_bytes - 1,
        }
    );
    assert_eq!(output, original);
}

#[test]
fn every_codec_rolls_back_when_the_final_batch_cap_is_one_byte_short() {
    for compression in codecs() {
        let source = batch(compression);
        let encoded = source
            .encode_to_bytes(RecordEncodeLimits::default())
            .unwrap();
        let limit = encoded.len() - 1;
        let limits = RecordEncodeLimits::new(usize::MAX, limit);
        let mut output = BytesMut::from(&b"prefix"[..]);
        let original = output.clone();

        assert!(matches!(
            source.encode_into(&mut output, limits),
            Err(RecordError::BatchLimitExceeded {
                limit: observed,
                ..
            }) if observed == limit
        ));
        assert_eq!(
            output,
            original,
            "{} left a partial batch behind",
            compression.name()
        );
    }
}

#[test]
fn compressed_output_is_stopped_while_the_codec_is_producing_it() {
    for compression in [
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let source = batch(compression);
        let limits = RecordEncodeLimits::new(usize::MAX, 62);
        let mut output = BytesMut::from(&b"prefix"[..]);
        let original = output.clone();

        assert!(matches!(
            source.encode_into(&mut output, limits),
            Err(RecordError::BatchLimitExceeded { limit: 62, .. })
        ));
        assert_eq!(output, original, "{} escaped rollback", compression.name());
    }
}

#[test]
fn successful_append_preserves_the_existing_prefix_and_reports_its_own_size() {
    let source = batch(Compression::None);
    let mut output = BytesMut::from(&b"prefix"[..]);
    let written = source
        .encode_into(&mut output, RecordEncodeLimits::default())
        .unwrap();

    assert_eq!(&output[..6], b"prefix");
    assert_eq!(written, output.len() - 6);
}
