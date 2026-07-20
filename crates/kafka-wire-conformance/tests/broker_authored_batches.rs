//! Record batches agree with Apache Kafka's own writers, byte for byte.
//!
//! Scenario: for every vector under `spec/records/`, decode the bytes Kafka's
//! record-batch writers produced and re-encode them to the identical bytes.
//!
//! This is the only direction available here, and it is stronger than it looks
//! precisely because this repository did not author the bytes. A batch carries a
//! CRC32C over its own tail and a length that counts from the middle of its
//! header — two quantities that a decoder and an encoder sharing a misreading
//! would still agree on with each other, and cannot agree on with Kafka.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire_conformance::{from_hex, to_hex};
use kafka_wire_core::Encoder;
use kafka_wire_records::{Compression, RecordBatch, RecordDecodeLimits, RecordError};

mod support;

use support::batches as corpus;

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
fn every_uncompressed_batch_decodes_and_re_encodes_to_the_same_bytes() {
    let mut failures = Vec::new();
    let mut checked = 0;

    for vector in corpus() {
        let expected = from_hex(&vector.hex).expect("hex");
        let batch = match decode_batch(Bytes::from(expected.clone())) {
            Ok(batch) => batch,
            Err(error) => {
                failures.push(format!("{}: decode failed: {error}", vector.name));
                continue;
            }
        };
        // A compressed payload is not asserted to reproduce Java's bytes: that
        // would require this crate's compressor to agree with Java's down to the
        // encoder's internal choices, and where it happens to today it is a
        // coincidence of two libraries rather than a protocol property. Those
        // batches are judged by whether Kafka can read them back instead.
        if batch.compression != Compression::None {
            continue;
        }

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
        checked >= 11,
        "only {checked} uncompressed batch(es) were checked; the corpus should carry more"
    );
}

#[test]
fn every_codec_decompresses_to_the_records_kafka_compressed() {
    // The half of compression that CAN be held to Kafka's bytes. Each codec's
    // batch carries the same two records as the uncompressed twin, so decoding
    // one must produce exactly those records — which fails if the framing is
    // read wrongly, and snappy's is the one most likely to be: Kafka writes the
    // xerial container, not the standard snappy frame format.
    let batches: Vec<_> = corpus()
        .into_iter()
        .map(|vector| {
            let bytes = Bytes::from(from_hex(&vector.hex).expect("hex"));
            let batch =
                decode_batch(bytes).unwrap_or_else(|error| panic!("{}: {error}", vector.name));
            (vector.name, batch)
        })
        .collect();

    let find = |name: &str| {
        batches
            .iter()
            .find(|(vector, _)| vector == name)
            .map_or_else(
                || panic!("the corpus must carry {name}"),
                |(_, batch)| batch,
            )
    };

    let plain = find("compression_twin_uncompressed");
    let mut codecs = Vec::new();
    for name in ["gzip", "snappy", "lz4", "zstd"] {
        let batch = find(name);
        assert_ne!(
            batch.compression,
            Compression::None,
            "{name} must decode as compressed"
        );
        assert_eq!(
            batch.records, plain.records,
            "{name} decompressed to different records than the uncompressed twin"
        );
        codecs.push(batch.compression.name());
    }
    codecs.sort_unstable();
    assert_eq!(codecs, ["gzip", "lz4", "snappy", "zstd"]);
}

#[test]
fn a_compressed_batch_round_trips_its_records_though_not_its_bytes() {
    // The cheap half of the encode direction: this crate's own compressor and
    // decompressor agree. That is a weak claim on its own — both could share one
    // misreading — and it is not the one that matters. Kafka reading these bytes
    // back is, and `kafka_reads_what_this_repository_compressed` asserts it.
    for name in ["gzip", "snappy", "lz4", "zstd"] {
        let vector = corpus()
            .into_iter()
            .find(|vector| vector.name == name)
            .unwrap_or_else(|| panic!("the corpus must carry {name}"));
        let original = decode_batch(Bytes::from(from_hex(&vector.hex).expect("hex")))
            .unwrap_or_else(|error| panic!("{name}: {error}"));

        let mut buffer = BytesMut::new();
        original
            .encode(&mut Encoder::new(&mut buffer))
            .unwrap_or_else(|error| panic!("{name}: re-encode: {error}"));
        let reread = decode_batch(buffer.freeze())
            .unwrap_or_else(|error| panic!("{name}: re-decode: {error}"));

        assert_eq!(
            reread, original,
            "{name} did not survive a round trip through this crate's own codec"
        );
    }
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

    let error = decode_batch(Bytes::from(bytes)).unwrap_err();
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
            decode_batch(Bytes::from(from_hex(&vector.hex).expect("hex")))
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

    // Compaction preserves offsets while removing records, including all of
    // them. The last offset is therefore metadata of its own, not count - 1.
    let compacted = named("compacted_records_keep_offset_gaps");
    assert_eq!(compacted.records.len(), 2);
    assert_eq!(compacted.records[0].offset_delta, 0);
    assert_eq!(compacted.records[1].offset_delta, 2);
    assert_eq!(compacted.last_offset_delta, 2);
    let empty = named("empty_compacted_batch_keeps_last_offset");
    assert!(empty.records.is_empty());
    assert_eq!(empty.base_offset, 100);
    assert_eq!(empty.last_offset_delta, 2);

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
