//! A records field takes Kafka's null default where the type can hold it.
//!
//! Scenario: Kafka's generator returns `null` for every records-typed field
//! unconditionally — `FieldSpec.fieldDefault`'s `isRecords()` arm ignores the
//! declared default. This repository matches that where the field is nullable
//! across the message's supported versions, because only then is its Rust type
//! an `Option` that can hold the null; where it is not, the type is a bare
//! `Bytes` and the empty batch is kept as a deliberate, recorded divergence.
//!
//! The byte corpus cannot see any of this: records is present at `0+` in every
//! message that has it, so the decode branch that would substitute the default
//! never fires. The defect this guards against lives entirely in
//! `Default::default()`, which is public API surface.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{DefaultValue, Message, SourceFile, lower_message, parse_jsonc};

#[test]
fn a_records_field_nullable_in_range_takes_kafkas_null() {
    // Nullable at 0+ within validVersions 0-2: the Rust type is `Option<Bytes>`,
    // which holds Kafka's null, so this repository matches rather than encoding a
    // present, empty batch where Kafka means an absent one.
    let message = lower(
        r#"{ "name": "Records", "type": "records", "versions": "0+",
             "nullableVersions": "0+" }"#,
    );

    assert_eq!(message.fields[0].default, DefaultValue::Null);
}

#[test]
fn a_non_nullable_records_field_keeps_the_empty_batch() {
    // No nullableVersions: a bare `Bytes` that cannot hold null. This is the
    // FetchSnapshotResponse.unalignedRecords divergence.
    let message = lower(r#"{ "name": "UnalignedRecords", "type": "records", "versions": "0+" }"#);

    assert_eq!(message.fields[0].default, DefaultValue::Empty);
}

#[test]
fn a_records_field_nullable_only_outside_the_supported_range_keeps_the_empty_batch() {
    // ShareFetchResponse.records declares nullableVersions "0" under validVersions
    // "1-2": nullable in a version the message does not support is not nullable
    // here at all, so the type stays a bare `Bytes` and the empty batch is kept.
    let message = lower_message_source(
        r#"{ "apiKey": 1, "type": "response", "name": "ExampleResponse",
             "validVersions": "1-2", "flexibleVersions": "1+",
             "fields": [ { "name": "Records", "type": "records", "versions": "0+",
                           "nullableVersions": "0" } ] }"#,
    );

    assert_eq!(message.fields[0].default, DefaultValue::Empty);
}

/// Lowers a request whose single root field is the records field under test.
fn lower(field: &str) -> Message {
    lower_message_source(&format!(
        r#"{{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
              "validVersions": "0-2", "flexibleVersions": "2+",
              "fields": [ {field} ] }}"#
    ))
}

fn lower_message_source(source: &str) -> Message {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    lower_message(raw, PathBuf::from("fixture.json")).expect("fixture must lower")
}
