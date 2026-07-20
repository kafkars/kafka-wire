//! Record batches agree with Apache Kafka's own producer, byte for byte.
//!
//! Scenario: for every vector under `spec/records/`, decode the bytes Kafka's
//! `MemoryRecordsBuilder` produced and re-encode them to the identical bytes.
//!
//! This is the only direction available here, and it is stronger than it looks
//! precisely because this repository did not author the bytes. A batch carries a
//! CRC32C over its own tail and a length that counts from the middle of its
//! header — two quantities that a decoder and an encoder sharing a misreading
//! would still agree on with each other, and cannot agree on with Kafka.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{fs, path::PathBuf};

use bytes::{Bytes, BytesMut};
use kafka_wire_conformance::{from_hex, to_hex};
use kafka_wire_core::Encoder;
use kafka_wire_records::{Compression, RecordBatch, RecordError};

#[derive(serde::Deserialize)]
struct Corpus {
    vectors: Vec<Vector>,
}

#[derive(serde::Deserialize)]
struct Vector {
    name: String,
    why: String,
    hex: String,
}

fn corpus() -> Vec<Vector> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("spec/records/vectors.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let corpus: Corpus = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    corpus.vectors
}

#[test]
fn every_uncompressed_batch_decodes_and_re_encodes_to_the_same_bytes() {
    let mut failures = Vec::new();
    let mut checked = 0;

    for vector in corpus() {
        let expected = from_hex(&vector.hex).expect("hex");
        let batch = match RecordBatch::decode(&Bytes::from(expected.clone())) {
            Ok(batch) => batch,
            // Compression is not implemented; the vectors exist so that the
            // refusal is proved against a real compressed batch rather than a
            // hand-made one, and so the codec is already covered when it lands.
            Err(RecordError::UnsupportedCompression { .. }) => continue,
            Err(error) => {
                failures.push(format!("{}: decode failed: {error}", vector.name));
                continue;
            }
        };

        let mut buffer = BytesMut::new();
        match batch.encode(&mut Encoder::new(&mut buffer)) {
            Ok(()) if buffer.as_ref() == expected.as_slice() => checked += 1,
            Ok(()) => failures.push(format!(
                "{}: re-encoding changed the bytes\n  kafka: {}\n  rust:  {}\n  why:   {}",
                vector.name,
                vector.hex,
                to_hex(&buffer),
                vector.why
            )),
            Err(error) => failures.push(format!("{}: re-encode failed: {error}", vector.name)),
        }
    }

    assert!(
        failures.is_empty(),
        "{} batch(es) disagree with Apache Kafka:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert!(
        checked >= 9,
        "only {checked} uncompressed batch(es) were checked; the corpus should carry more"
    );
}

#[test]
fn every_compressed_batch_is_refused_by_the_name_of_its_codec() {
    // A codec this build cannot decode must be named, not handed back as opaque
    // bytes: a caller given a still-compressed payload would parse it as records
    // and get garbage that looks like data.
    let mut seen = Vec::new();
    for vector in corpus() {
        let bytes = Bytes::from(from_hex(&vector.hex).expect("hex"));
        if let Err(RecordError::UnsupportedCompression { codec }) = RecordBatch::decode(&bytes) {
            seen.push(codec);
        }
    }
    seen.sort_unstable();
    assert_eq!(
        seen,
        ["gzip", "lz4", "snappy", "zstd"],
        "every compressed codec in the corpus must be refused by name"
    );
}

#[test]
fn a_corrupted_batch_is_rejected_by_its_crc() {
    // The suite above only means something if a flipped byte fails it, and the
    // CRC is the first thing that should notice.
    let vector = corpus()
        .into_iter()
        .find(|vector| vector.name == "one_record_with_key")
        .expect("the corpus must carry one_record_with_key");
    let mut bytes = from_hex(&vector.hex).expect("hex");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;

    let error = RecordBatch::decode(&Bytes::from(bytes)).unwrap_err();
    assert!(
        matches!(error, RecordError::CorruptBatch { .. }),
        "a flipped payload byte must fail the CRC: {error}"
    );
}

#[test]
fn the_uncompressed_vectors_carry_the_shapes_that_discriminate() {
    // A corpus of one batch shape would pass a decoder that ignored half the
    // header. These are the distinctions that a wrong implementation collapses.
    let batches: Vec<_> = corpus()
        .into_iter()
        .filter_map(|vector| {
            RecordBatch::decode(&Bytes::from(from_hex(&vector.hex).expect("hex")))
                .ok()
                .map(|batch| (vector.name, batch))
        })
        .collect();

    let named = |name: &str| {
        batches
            .iter()
            .find(|(vector, _)| vector == name)
            .map_or_else(
                || panic!("the corpus must carry {name}"),
                |(_, batch)| batch,
            )
    };

    // Absent is not empty, in both directions.
    let empty = &named("an_empty_key_and_an_empty_value").records[0];
    assert_eq!(empty.key.as_deref(), Some(&b""[..]));
    assert_eq!(empty.value.as_deref(), Some(&b""[..]));
    let tombstone = &named("a_null_value").records[0];
    assert_eq!(tombstone.value, None, "a tombstone must decode as absent");
    assert!(tombstone.key.is_some());

    // maxTimestamp is the largest, not the last.
    let deltas = named("three_records_with_deltas");
    assert_eq!(deltas.records.len(), 3);
    assert_eq!(deltas.base_offset, 100);
    assert_eq!(
        deltas.max_timestamp,
        deltas.base_timestamp + 500,
        "maxTimestamp must be the largest timestamp, not the last record's"
    );
    assert_eq!(deltas.records[2].timestamp_delta, 200);

    // The producer identity and the transactional bit travel in the header.
    let txn = named("transactional_with_producer_identity");
    assert!(txn.is_transactional);
    assert_eq!(txn.producer_id, 12345);
    assert_eq!(txn.producer_epoch, 7);
    assert_eq!(txn.base_sequence, 100);
    assert_eq!(txn.partition_leader_epoch, 42);
    assert!(!named("one_record_no_key").is_transactional);

    // Header values are nullable; header keys are not.
    let headers = &named("headers_on_one_record").records[0].headers;
    assert_eq!(headers.len(), 3);
    assert_eq!(headers[0].key, "trace-id");
    assert_eq!(headers[1].value.as_deref(), Some(&b""[..]));
    assert_eq!(
        headers[2].value, None,
        "an absent header value is not empty"
    );

    assert_eq!(named("one_record_no_key").compression, Compression::None);
}
