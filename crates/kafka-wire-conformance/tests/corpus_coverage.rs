//! The corpus is large enough, wide enough, and honest about what it covers.
//!
//! Scenario: a conformance suite that loads nothing passes. So does one that
//! covers only the versions and shapes that already worked. These assertions are
//! the floor beneath the byte comparisons: a real vector count, every supported
//! version present, the hand-transcribed identity in each vector agreeing with
//! the generated descriptors, and — the blind spot this corpus was built to
//! close — at least one length that needs more than a single varint byte.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;

use kafka_wire::{MESSAGE_DESCRIPTORS, MessageDirection};
use kafka_wire_conformance::{Direction, Vector, facts, is_flexible, load};

/// Smallest corpus that can honestly be called coverage.
const MINIMUM_VECTORS: usize = 48;

/// Largest length that still fits one varint byte.
const SINGLE_BYTE_VARINT_MAXIMUM: usize = 0x7f;

#[test]
fn the_corpus_is_not_vacuously_small() {
    let vectors = load().unwrap();

    assert!(
        vectors.len() >= MINIMUM_VECTORS,
        "the corpus holds {} vector(s), below the floor of {MINIMUM_VECTORS}; \
         a conformance run that inspects almost nothing reports success",
        vectors.len()
    );
}

#[test]
fn every_generated_message_and_version_is_covered() {
    let vectors = load().unwrap();
    let covered = vectors
        .iter()
        .map(|vector| (vector.message.clone(), vector.version))
        .collect::<BTreeSet<_>>();
    let mut missing = Vec::new();

    for descriptor in MESSAGE_DESCRIPTORS {
        let range = descriptor.supported_versions;
        for version in range.min().value()..=range.max().value() {
            if !covered.contains(&(descriptor.name.to_owned(), version)) {
                missing.push(format!("{} v{version}", descriptor.name));
            }
        }
    }

    assert!(
        missing.is_empty(),
        "these generated message versions have no vector, so every encoding \
         decision they make is untested: {missing:?}"
    );
}

#[test]
fn every_vector_matches_the_generated_descriptor() {
    let vectors = load().unwrap();
    let mut findings = Vec::new();

    for vector in &vectors {
        let facts = facts(&vector.message).unwrap();
        let at = format!("{} v{} [{}]", vector.message, vector.version, vector.name);

        // A framing schema states neither, and the two sides must agree about
        // that too: a key appearing on one side only is a disagreement.
        if facts.api_key != vector.api_key {
            findings.push(format!(
                "{at}: Kafka reported api key {:?} but this repository generated {:?}",
                vector.api_key, facts.api_key
            ));
        }

        let expected = match vector.direction {
            Direction::Request => Some(MessageDirection::Request),
            Direction::Response => Some(MessageDirection::Response),
            Direction::Framing => None,
        };
        if facts.direction != expected {
            findings.push(format!("{at}: direction disagrees with the descriptor"));
        }

        // The version guard in the Java oracle is what keeps an out-of-range
        // version from being minted at all. Re-asserting it here means a vector
        // that escaped that guard still fails, in pure Rust, with no jar in sight.
        let range = facts.supported_versions;
        if vector.version < range.min().value() || vector.version > range.max().value() {
            findings.push(format!(
                "{at}: version is outside the generated supported range {range}"
            ));
        }

        // `flexible` is transcribed by hand from the upstream JSONC, so this
        // compares an independent reading against the lowered `flexibleVersions`.
        let generated = is_flexible(&vector.message, vector.version).unwrap();
        if generated != vector.flexible {
            findings.push(format!(
                "{at}: the plan declares flexible={} but the generated message says {generated}",
                vector.flexible
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "vector identity disagrees with generated metadata:\n  {}",
        findings.join("\n  ")
    );
}

#[test]
fn the_corpus_crosses_the_single_byte_varint_boundary() {
    let vectors = load().unwrap();

    let longest_string = vectors
        .iter()
        .map(|vector| longest_string_bytes(&vector.json_value))
        .max()
        .unwrap_or(0);
    let longest_array = vectors
        .iter()
        .map(|vector| longest_array(&vector.json_value))
        .max()
        .unwrap_or(0);
    let most_tagged_fields = vectors
        .iter()
        .map(|vector| vector.unknown_tagged_fields.len())
        .max()
        .unwrap_or(0);

    assert!(
        longest_string > SINGLE_BYTE_VARINT_MAXIMUM,
        "the longest string in the corpus is {longest_string} byte(s); nothing forces a \
         compact-string length past {SINGLE_BYTE_VARINT_MAXIMUM}, so multi-byte varint \
         lengths stay untested"
    );
    assert!(
        longest_array > SINGLE_BYTE_VARINT_MAXIMUM,
        "the longest array in the corpus holds {longest_array} element(s); array counts \
         never cross {SINGLE_BYTE_VARINT_MAXIMUM}"
    );
    assert!(
        most_tagged_fields > SINGLE_BYTE_VARINT_MAXIMUM,
        "the largest tagged-field set in the corpus holds {most_tagged_fields} field(s); \
         the tagged-field count varint never crosses {SINGLE_BYTE_VARINT_MAXIMUM}"
    );
}

#[test]
fn the_corpus_covers_both_encodings_and_both_directions() {
    let vectors = load().unwrap();

    assert!(
        vectors.iter().any(|vector| vector.flexible),
        "no flexible vector: compact strings and tagged fields are untested"
    );
    assert!(
        vectors.iter().any(|vector| !vector.flexible),
        "no legacy vector: int16 string and int32 array prefixes are untested"
    );
    assert!(
        vectors
            .iter()
            .any(|vector| vector.direction == Direction::Request),
        "no request vector"
    );
    assert!(
        vectors
            .iter()
            .any(|vector| vector.direction == Direction::Response),
        "no response vector"
    );
    assert!(
        vectors.iter().any(empty_body),
        "no vector encodes an empty body, so the version gate that drops every \
         field is unproven"
    );
}

fn empty_body(vector: &Vector) -> bool {
    vector.hex.is_empty()
}

fn longest_string_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(text) => text.len(),
        serde_json::Value::Array(elements) => {
            elements.iter().map(longest_string_bytes).max().unwrap_or(0)
        }
        serde_json::Value::Object(entries) => entries
            .values()
            .map(longest_string_bytes)
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

fn longest_array(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(elements) => elements
            .iter()
            .map(longest_array)
            .max()
            .unwrap_or(0)
            .max(elements.len()),
        serde_json::Value::Object(entries) => {
            entries.values().map(longest_array).max().unwrap_or(0)
        }
        _ => 0,
    }
}
