//! Apache Kafka reads back exactly the records this repository compressed.
//!
//! Scenario: take a batch Kafka's own producer compressed, decode it, RE-ENCODE
//! it through this repository's compressor, and compare the result against the
//! bytes `spec/records/verified.json` records Kafka being handed — then check
//! that the records Kafka recovered from them are the records the batch started
//! with, field for field.
//!
//! This is the direction long recorded as unreachable, and the record was wrong
//! about the property rather than about the evidence. Byte identity with Java's
//! `Deflater`, `zstd-jni`, `lz4-java`, and `snappy-java` is unachievable and
//! would be a coincidence if observed — but byte identity was never what a
//! producer needs. What it needs is that the broker can read the payload, and
//! that is a question only the broker can answer. `RecordOracle --verify` asks
//! it, through `MemoryRecords.readableRecords` and Kafka's own iterators.
//!
//! What the transcript cannot do is verify itself, which is why the hex
//! comparison comes first: it pins Kafka's answer to bytes this build actually
//! produces, so a compressor whose output moves fails here rather than
//! inheriting a verdict about bytes that no longer exist.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_conformance::{from_hex, to_hex};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordDecodeLimits, RecordEncodeLimits, RecordError,
};

mod support;

use support::{ReadBatch, ReadRecord};

fn decode_batch(mut bytes: Bytes) -> Result<RecordBatch, RecordError> {
    let batch = RecordBatch::decode(&mut bytes, RecordDecodeLimits::default())?;
    assert!(
        bytes.is_empty(),
        "a one-batch oracle vector carried {} trailing byte(s)",
        bytes.len()
    );
    Ok(batch)
}

#[test]
fn every_codec_this_repository_writes_is_read_back_by_kafka() {
    let corpus = support::batches();
    let mut codecs = Vec::new();

    for entry in support::verified() {
        let vector = corpus
            .iter()
            .find(|batch| batch.name == entry.name)
            .unwrap_or_else(|| panic!("{}: vectors.json carries no such batch", entry.name));
        let authored = decode_batch(Bytes::from(from_hex(&vector.hex).unwrap()))
            .unwrap_or_else(|error| panic!("{}: {error}", entry.name));

        // Kafka's answer is about specific bytes. If this build no longer writes
        // them, the answer is about nothing.
        let rewritten = authored
            .encode_to_bytes(RecordEncodeLimits::default())
            .unwrap_or_else(|error| panic!("{}: re-encode: {error}", entry.name));
        assert_eq!(
            to_hex(&rewritten),
            entry.hex,
            "{}: this build compresses differently than the bytes Kafka was shown; \
             re-run `cargo xtask records --refresh`\n  why: {}",
            entry.name,
            entry.why
        );

        let read = one_batch(&entry.kafka.batches, &entry.name);
        assert_eq!(
            read.compression,
            authored.compression.name(),
            "{}: Kafka read a different codec than this repository wrote",
            entry.name
        );
        assert_header(read, &authored, &entry.name);
        assert_records(read, &authored, &entry.name);
        codecs.push(read.compression.clone());
    }

    codecs.sort();
    assert_eq!(
        codecs,
        ["gzip", "lz4", "snappy", "zstd"],
        "Kafka must have read back every codec this crate implements"
    );
}

#[test]
fn every_compressed_batch_in_the_corpus_is_put_to_kafka() {
    // The transcript is authored from the corpus, so a codec that stopped being
    // verified would otherwise just disappear quietly.
    let transcript = support::verified();
    let mut missing = Vec::new();

    for batch in support::batches() {
        let decoded = decode_batch(Bytes::from(from_hex(&batch.hex).unwrap()))
            .unwrap_or_else(|error| panic!("{}: {error}", batch.name));
        if decoded.compression == Compression::None {
            continue;
        }
        if !transcript.iter().any(|entry| entry.name == batch.name) {
            missing.push(batch.name);
        }
    }

    assert!(
        missing.is_empty(),
        "compressed batch(es) {missing:?} were never put to Kafka's reader; \
         re-run `cargo xtask records --refresh`"
    );
}

#[test]
fn a_payload_kafka_could_not_read_would_be_visible_here() {
    // The suite above only means something if a broken payload fails it. The
    // transcript cannot be corrupted to prove that — it is Kafka's answer — so
    // what is corrupted is the comparison's other side: a batch whose codec bits
    // say gzip over a payload that is not gzip must fail to decompress, which is
    // the same refusal Kafka would raise on the wire.
    let vector = support::batches()
        .into_iter()
        .find(|batch| batch.name == "gzip")
        .expect("the corpus must carry gzip");
    let mut bytes = from_hex(&vector.hex).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;

    assert!(
        decode_batch(Bytes::from(bytes)).is_err(),
        "a compressed batch with a mangled payload decoded anyway, so the transcript \
         comparison is not exercising decompression at all"
    );
}

fn one_batch<'a>(batches: &'a [ReadBatch], name: &str) -> &'a ReadBatch {
    assert_eq!(
        batches.len(),
        1,
        "{name}: Kafka read {} batch(es) out of one batch's bytes",
        batches.len()
    );
    &batches[0]
}

/// The header travels uncompressed, so Kafka reading it wrongly means we wrote
/// it wrongly rather than that a codec misbehaved.
fn assert_header(read: &ReadBatch, authored: &RecordBatch, name: &str) {
    assert_eq!(read.magic, 2, "{name}: magic");
    assert_eq!(read.base_offset, authored.base_offset, "{name}: baseOffset");
    assert_eq!(
        read.last_offset,
        authored.base_offset + i64::from(authored.records.last().unwrap().offset_delta),
        "{name}: lastOffset"
    );
    assert_eq!(
        read.partition_leader_epoch, authored.partition_leader_epoch,
        "{name}: partitionLeaderEpoch"
    );
    assert_eq!(read.producer_id, authored.producer_id, "{name}: producerId");
    assert_eq!(
        read.producer_epoch, authored.producer_epoch,
        "{name}: producerEpoch"
    );
    assert_eq!(
        read.base_sequence, authored.base_sequence,
        "{name}: baseSequence"
    );
    assert_eq!(
        read.max_timestamp, authored.max_timestamp,
        "{name}: maxTimestamp"
    );
    assert_eq!(read.timestamp_type, "CreateTime", "{name}: timestampType");
    assert_eq!(
        read.transactional, authored.is_transactional,
        "{name}: transactional"
    );
    assert_eq!(read.control_batch, authored.is_control, "{name}: control");
}

/// The records are what came out of the codec, and are the point of the exercise.
fn assert_records(read: &ReadBatch, authored: &RecordBatch, name: &str) {
    assert_eq!(
        read.records.len(),
        authored.records.len(),
        "{name}: Kafka recovered a different number of records"
    );

    for (index, (found, expected)) in read.records.iter().zip(&authored.records).enumerate() {
        let at = format!("{name} record {index}");
        assert_record(found, expected, authored, &at);
    }
}

fn assert_record(found: &ReadRecord, expected: &Record, batch: &RecordBatch, at: &str) {
    // Kafka resolves the deltas this crate stores; comparing the absolutes is
    // what proves the base values were written where Kafka looks for them.
    assert_eq!(
        found.offset,
        batch.base_offset + i64::from(expected.offset_delta),
        "{at}: offset"
    );
    assert_eq!(
        found.timestamp,
        batch.base_timestamp + expected.timestamp_delta,
        "{at}: timestamp"
    );
    assert_eq!(found.key, expected.key.as_deref().map(to_hex), "{at}: key");
    assert_eq!(
        found.value,
        expected.value.as_deref().map(to_hex),
        "{at}: value"
    );

    assert_eq!(
        found.headers.len(),
        expected.headers.len(),
        "{at}: header count"
    );
    for (found, expected) in found.headers.iter().zip(&expected.headers) {
        assert_eq!(
            found.key.as_str(),
            expected.key.as_str(),
            "{at}: header key"
        );
        assert_eq!(
            found.value,
            expected.value.as_deref().map(to_hex),
            "{at}: header value"
        );
    }
}
