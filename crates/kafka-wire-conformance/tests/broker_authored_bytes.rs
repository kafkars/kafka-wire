//! Generated Rust agrees with Apache Kafka's own writer, byte for byte.
//!
//! Scenario: for every checked-in vector, run both directions that a wrong
//! implementation could pass one of and fail the other.
//!
//! Decoding proves this repository reads Kafka's bytes and writes them back
//! unchanged. Constructing from the canonical JSON value proves it reaches those
//! bytes from a value it had to build itself — which is where a wrong default, a
//! misnamed field, or a missing version gate shows up. A corpus that only
//! round-tripped bytes would be blind to all three, because a decoder and an
//! encoder that share a misreading agree with each other perfectly.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use kafka_wire_conformance::{Subject, from_hex, load, to_hex};

#[test]
fn every_vector_decodes_and_re_encodes_to_the_same_bytes() {
    let vectors = load().unwrap();
    let mut failures = Vec::new();

    for vector in &vectors {
        let expected = from_hex(&vector.hex).unwrap();
        let decoded = match Subject::decode(
            &vector.message,
            vector.version,
            Bytes::from(expected.clone()),
        ) {
            Ok(decoded) => decoded,
            Err(error) => {
                failures.push(format!(
                    "{} v{} [{}]: decode failed: {error}\n  bytes: {}",
                    vector.message, vector.version, vector.name, vector.hex
                ));
                continue;
            }
        };

        match decoded.encode(vector.version) {
            Ok(actual) if actual.as_ref() == expected.as_slice() => {}
            Ok(actual) => failures.push(format!(
                "{} v{} [{}]: re-encoding changed the bytes\n  kafka: {}\n  rust:  {}\n  why:   {}",
                vector.message,
                vector.version,
                vector.name,
                vector.hex,
                to_hex(&actual),
                vector.why
            )),
            Err(error) => failures.push(format!(
                "{} v{} [{}]: re-encode failed: {error}",
                vector.message, vector.version, vector.name
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} vector(s) disagree with Apache Kafka:\n\n{}",
        failures.len(),
        vectors.len(),
        failures.join("\n\n")
    );
}

#[test]
fn every_vector_encodes_from_its_canonical_json_value() {
    let vectors = load().unwrap();
    let mut failures = Vec::new();

    for vector in &vectors {
        let subject = match Subject::from_vector(vector) {
            Ok(subject) => subject,
            Err(error) => {
                failures.push(format!(
                    "{} v{} [{}]: could not build from json_value: {error}",
                    vector.message, vector.version, vector.name
                ));
                continue;
            }
        };

        match subject.encode(vector.version) {
            Ok(actual) if to_hex(&actual) == vector.hex => {}
            Ok(actual) => failures.push(format!(
                "{} v{} [{}]: encoding the canonical value did not reproduce Kafka's bytes\n  \
                 kafka: {}\n  rust:  {}\n  json:  {}\n  why:   {}",
                vector.message,
                vector.version,
                vector.name,
                vector.hex,
                to_hex(&actual),
                vector.json_value,
                vector.why
            )),
            Err(error) => failures.push(format!(
                "{} v{} [{}]: encode failed: {error}",
                vector.message, vector.version, vector.name
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} vector(s) disagree with Apache Kafka:\n\n{}",
        failures.len(),
        vectors.len(),
        failures.join("\n\n")
    );
}

#[test]
fn a_corrupted_vector_would_be_rejected() {
    // The suite above only means something if a wrong byte fails it. Flip the
    // last byte of a real vector and confirm the comparison notices.
    let vectors = load().unwrap();
    let vector = vectors
        .iter()
        .find(|vector| !vector.hex.is_empty())
        .unwrap();

    let mut corrupted = from_hex(&vector.hex).unwrap();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xff;

    let subject = Subject::from_vector(vector).unwrap();
    let encoded = subject.encode(vector.version).unwrap();

    assert_ne!(
        encoded.as_ref(),
        corrupted.as_slice(),
        "a corrupted vector still compared equal, so the byte comparison proves nothing"
    );
}
