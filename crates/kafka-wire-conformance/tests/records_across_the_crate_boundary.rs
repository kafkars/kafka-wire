//! A `records` field carries a real Kafka batch between the two crates that own it.
//!
//! Scenario: take a batch Apache Kafka's own record writer laid out, put it in the
//! `records` field of a `ProduceRequest` and of a `FetchResponse`, encode the
//! message, decode it back, and hand the recovered blob to
//! `kafka_wire_records::RecordBatch::decode`.
//!
//! `kafka-wire` carries `records` as an opaque length-prefixed blob and
//! `kafka-wire-records` parses what is inside it. Each crate's own suite proves its
//! half; neither can see the seam. A length prefix written in the wrong regime,
//! or a blob that gained or lost a byte crossing the field, leaves a message
//! that still round-trips and a batch that no longer parses — a failure only the
//! two crates together can state.

// Every generated DTO is `#[non_exhaustive]`, so the struct literal
// `field_reassign_with_default` asks for cannot be written outside
// `kafka-wire`. Default-then-assign is the only construction available here.
#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::unwrap_used
)]

use bytes::Bytes;
use kafka_wire::{
    FetchResponse, ProduceRequest,
    fetch_response::{FetchableTopicResponse, PartitionData},
    produce_request::{PartitionProduceData, TopicProduceData},
};
use kafka_wire_conformance::from_hex;
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaDecode, KafkaEncode, StrBytes};
use kafka_wire_records::{RecordBatch, RecordDecodeLimits, RecordError};

mod support;

/// The oldest `Produce`, the first flexible one, and the newest.
///
/// A records field is length-prefixed differently either side of version 9 —
/// `int32` below it, unsigned varint at and above — so a regime read wrongly
/// truncates the blob on exactly one side of that line.
const PRODUCE_VERSIONS: [i16; 3] = [3, 9, 13];

/// The same three questions for `Fetch`, whose flexible window opens at 12.
const FETCH_VERSIONS: [i16; 3] = [4, 12, 18];

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
fn a_produce_request_delivers_every_broker_authored_batch_intact() {
    let mut checked = 0;
    for batch in support::batches() {
        let authored = Bytes::from(from_hex(&batch.hex).unwrap());
        let expected = decode_batch(authored.clone())
            .unwrap_or_else(|error| panic!("{}: {error}", batch.name));

        for version in PRODUCE_VERSIONS {
            let recovered = through_produce_request(version, &authored).unwrap_or_else(|| {
                panic!(
                    "{} v{version}: the records field came back null",
                    batch.name
                )
            });
            assert_eq!(
                recovered, authored,
                "{} v{version}: ProduceRequest did not return the bytes it was given\n  why: {}",
                batch.name, batch.why
            );

            let parsed = decode_batch(recovered.clone()).unwrap_or_else(|error| {
                panic!(
                    "{} v{version}: the recovered blob is no longer a batch: {error}",
                    batch.name
                )
            });
            assert_eq!(
                parsed, expected,
                "{} v{version}: the records changed crossing the field",
                batch.name
            );
            checked += 1;
        }
    }

    assert!(
        checked >= 3 * PRODUCE_VERSIONS.len(),
        "only {checked} carriage(s) were checked; the batch corpus should be larger"
    );
}

#[test]
fn a_fetch_response_delivers_every_broker_authored_batch_intact() {
    for batch in support::batches() {
        let authored = Bytes::from(from_hex(&batch.hex).unwrap());
        let expected = decode_batch(authored.clone())
            .unwrap_or_else(|error| panic!("{}: {error}", batch.name));

        for version in FETCH_VERSIONS {
            let recovered = through_fetch_response(version, &authored).unwrap_or_else(|| {
                panic!(
                    "{} v{version}: the records field came back null",
                    batch.name
                )
            });
            assert_eq!(
                recovered, authored,
                "{} v{version}: FetchResponse did not return the bytes it was given\n  why: {}",
                batch.name, batch.why
            );

            let parsed = decode_batch(recovered.clone()).unwrap_or_else(|error| {
                panic!(
                    "{} v{version}: the recovered blob is no longer a batch: {error}",
                    batch.name
                )
            });
            assert_eq!(
                parsed, expected,
                "{} v{version}: the records changed crossing the field",
                batch.name
            );
        }
    }
}

#[test]
fn a_batch_that_lost_a_byte_still_crosses_the_field_and_fails_to_parse() {
    // Why the composition is worth asserting at all, and what each crate can
    // and cannot see. `kafka-wire` is content-blind about a records field,
    // so a truncated batch survives the message round trip unremarked; only
    // `kafka-wire-records` notices. If this passed the parse, the tests above would
    // be proving nothing about the seam.
    let batch = support::batches()
        .into_iter()
        .find(|batch| batch.name == "one_record_with_key")
        .expect("the corpus must carry one_record_with_key");

    let mut truncated = from_hex(&batch.hex).unwrap();
    truncated.pop();
    let truncated = Bytes::from(truncated);

    let recovered = through_produce_request(13, &truncated).expect("records must survive");
    assert_eq!(
        recovered, truncated,
        "the message round trip must stay indifferent to what the blob contains"
    );
    assert!(
        decode_batch(recovered).is_err(),
        "a batch missing its last byte parsed anyway, so the parse proves nothing"
    );
}

/// Carry `records` out to the wire in a `ProduceRequest` and read it back.
fn through_produce_request(version: i16, records: &Bytes) -> Option<Bytes> {
    let version = ApiVersion::new(version);

    let mut partition = PartitionProduceData::default();
    partition.index = 3;
    partition.records = Some(records.clone());

    let mut topic = TopicProduceData::default();
    topic.name = StrBytes::from("orders");
    topic.partition_data = vec![partition];

    let mut request = ProduceRequest::default();
    request.acks = -1;
    request.timeout_ms = 30_000;
    request.topic_data = vec![topic];

    let bytes = request.encode_to_bytes(version).unwrap();
    let decoded =
        ProduceRequest::decode_from_bytes(bytes, version, DecodeLimits::default()).unwrap();
    decoded.topic_data[0].partition_data[0].records.clone()
}

/// Carry `records` out to the wire in a `FetchResponse` and read it back.
fn through_fetch_response(version: i16, records: &Bytes) -> Option<Bytes> {
    let version = ApiVersion::new(version);

    let mut partition = PartitionData::default();
    partition.partition_index = 3;
    partition.high_watermark = 42;
    partition.records = Some(records.clone());

    let mut topic = FetchableTopicResponse::default();
    topic.topic = StrBytes::from("orders");
    topic.partitions = vec![partition];

    let mut response = FetchResponse::default();
    response.responses = vec![topic];

    let bytes = response.encode_to_bytes(version).unwrap();
    let decoded =
        FetchResponse::decode_from_bytes(bytes, version, DecodeLimits::default()).unwrap();
    decoded.responses[0].partitions[0].records.clone()
}
