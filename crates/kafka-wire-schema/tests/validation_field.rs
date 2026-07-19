//! The per-field contract: names, presence, nullability, defaults, tags, hints.
//!
//! Every stable diagnostic in this domain gets a pair: one field that must trip
//! it and one minimally-repaired field that must trip nothing. A negative test
//! alone cannot tell a working detector from one that fires at everything.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{SourceFile, lower_message, parse_jsonc, validate_message};

#[test]
fn a_field_with_no_faults_reports_nothing() {
    assert_codes(
        &fields(r#"{ "name": "Host", "type": "string", "versions": "0+" }"#),
        &[],
    );
}

#[test]
fn sibling_field_names_must_be_distinct_in_both_languages() {
    // A repeated protocol name necessarily repeats its Rust spelling too, so
    // this case legitimately reports both codes.
    assert_codes(
        &fields(
            r#"{ "name": "Host", "type": "string", "versions": "0+" },
               { "name": "Host", "type": "string", "versions": "0+" }"#,
        ),
        &[
            "KAFKA_SCHEMA_DUPLICATE_FIELD",
            "KAFKA_SCHEMA_RUST_NAME_COLLISION",
        ],
    );

    // Distinct upstream names that normalize to one identifier are the case
    // only the second detector can catch.
    assert_codes(
        &fields(
            r#"{ "name": "HostName", "type": "string", "versions": "0+" },
               { "name": "hostName", "type": "string", "versions": "0+" }"#,
        ),
        &["KAFKA_SCHEMA_RUST_NAME_COLLISION"],
    );
}

#[test]
fn only_a_type_that_encodes_absence_may_be_nullable() {
    assert_codes(
        &fields(
            r#"{ "name": "Count", "type": "int32", "versions": "0+",
                 "nullableVersions": "0+" }"#,
        ),
        &["KAFKA_SCHEMA_ILLEGAL_NULLABLE_TYPE"],
    );
    assert_codes(
        &fields(
            r#"{ "name": "Note", "type": "string", "versions": "0+",
                 "nullableVersions": "0+", "default": "null" }"#,
        ),
        &[],
    );
}

#[test]
fn nullability_may_not_be_claimed_where_the_field_is_absent() {
    assert_codes(
        &fields(
            r#"{ "name": "Note", "type": "string", "versions": "1+",
                 "nullableVersions": "0+", "default": "null" }"#,
        ),
        &["KAFKA_SCHEMA_NULLABLE_OUTSIDE_FIELD"],
    );
    assert_codes(
        &fields(
            r#"{ "name": "Note", "type": "string", "versions": "1+",
                 "nullableVersions": "1+", "default": "null" }"#,
        ),
        &[],
    );
}

#[test]
fn a_field_absent_from_every_valid_version_is_dead_weight() {
    assert_codes(
        &fields(r#"{ "name": "Gone", "type": "string", "versions": "3+" }"#),
        &["KAFKA_SCHEMA_UNUSED_FIELD"],
    );
    assert_codes(
        &fields(r#"{ "name": "Here", "type": "string", "versions": "2+" }"#),
        &[],
    );
}

#[test]
fn a_nested_field_is_judged_against_its_parent_not_the_message() {
    // `Legacy` exists in versions 0-1 and the message supports 0-2, so judging
    // it against the message finds it present and reports nothing. It is only
    // reachable through a parent introduced at version 2, where it never
    // appears — the parent's window is the one that decides.
    assert_codes(
        &fields(
            r#"{ "name": "Topics", "type": "[]TopicData", "versions": "2+", "fields": [
                 { "name": "Legacy", "type": "string", "versions": "0-1" } ] }"#,
        ),
        &["KAFKA_SCHEMA_UNUSED_FIELD"],
    );
    assert_codes(
        &fields(
            r#"{ "name": "Topics", "type": "[]TopicData", "versions": "2+", "fields": [
                 { "name": "Current", "type": "string", "versions": "2+" } ] }"#,
        ),
        &[],
    );
}

#[test]
fn a_retired_message_does_not_report_every_field_as_unused() {
    // Under `validVersions: "none"` no field is present. That is the point of
    // retiring the API, not one fault per field.
    let retired = format!(
        r#"{{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
          "validVersions": "none", "flexibleVersions": "none",
          "fields": [ {} ] }}"#,
        r#"{ "name": "Host", "type": "string", "versions": "0+" },
           { "name": "Port", "type": "int32", "versions": "0+" }"#
    );

    assert_codes(&retired, &[]);
}

#[test]
fn a_map_key_is_meaningful_only_inside_a_structured_element() {
    assert_codes(
        &fields(r#"{ "name": "Host", "type": "string", "versions": "0+", "mapKey": true }"#),
        &["KAFKA_SCHEMA_ROOT_MAP_KEY"],
    );
    assert_codes(
        &fields(
            r#"{ "name": "Topics", "type": "[]TopicData", "versions": "0+", "fields": [
                 { "name": "Name", "type": "string", "versions": "0+", "mapKey": true } ] }"#,
        ),
        &[],
    );
}

#[test]
fn inline_fields_require_a_struct_shaped_type() {
    assert_codes(
        &fields(
            r#"{ "name": "Host", "type": "string", "versions": "0+", "fields": [
                 { "name": "Inner", "type": "string", "versions": "0+" } ] }"#,
        ),
        &["KAFKA_SCHEMA_UNEXPECTED_NESTED_FIELDS"],
    );
}

#[test]
fn entity_type_names_a_scalar_value_not_a_struct_shape() {
    assert_codes(
        &fields(
            r#"{ "name": "Topics", "type": "[]TopicData", "versions": "0+",
                 "entityType": "topicName", "fields": [
                 { "name": "Name", "type": "string", "versions": "0+" } ] }"#,
        ),
        &["KAFKA_SCHEMA_ENTITY_TYPE_ON_STRUCT"],
    );

    // The same annotation on the scalar that actually holds the name, and on an
    // array whose *elements* are names, is exactly what upstream means by it.
    assert_codes(
        &fields(
            r#"{ "name": "Name", "type": "string", "versions": "0+",
                 "entityType": "topicName" },
               { "name": "Names", "type": "[]string", "versions": "0+",
                 "entityType": "topicName" }"#,
        ),
        &[],
    );
}

#[test]
fn zero_copy_promises_something_only_a_byte_run_can_deliver() {
    assert_codes(
        &fields(r#"{ "name": "Blob", "type": "string", "versions": "0+", "zeroCopy": true }"#),
        &["KAFKA_SCHEMA_ZERO_COPY_TYPE"],
    );
    assert_codes(
        &fields(r#"{ "name": "Blob", "type": "bytes", "versions": "0+", "zeroCopy": true }"#),
        &[],
    );
}

#[test]
fn a_default_must_fit_the_type_it_defaults() {
    assert_codes(
        &fields(r#"{ "name": "Count", "type": "int8", "versions": "0+", "default": "9999" }"#),
        &["KAFKA_SCHEMA_DEFAULT_TYPE"],
    );
    assert_codes(
        &fields(r#"{ "name": "Count", "type": "int8", "versions": "0+", "default": "99" }"#),
        &[],
    );
}

#[test]
fn the_implicit_default_of_every_scalar_type_is_representable() {
    // A uuid, a float64, and a non-nullable struct each have an implicit
    // default that is not null. Lowering them to null made every such field a
    // schema error, which is how 24 upstream files failed to load.
    assert_codes(
        &fields(
            r#"{ "name": "TopicId", "type": "uuid", "versions": "0+" },
               { "name": "Quota", "type": "float64", "versions": "0+" },
               { "name": "Leader", "type": "LeaderIdAndEpoch", "versions": "0+", "fields": [
                 { "name": "LeaderId", "type": "int32", "versions": "0+", "default": "-1" } ] }"#,
        ),
        &[],
    );
}

#[test]
fn a_hexadecimal_default_is_read_as_the_number_it_spells() {
    // The fetch APIs write their sentinel limits in hex. A decimal-only parser
    // rejects `"0x7fffffff"` as a malformed default rather than reading i32::MAX.
    assert_codes(
        &fields(
            r#"{ "name": "MaxBytes", "type": "int32", "versions": "0+",
                 "default": "0x7fffffff" }"#,
        ),
        &[],
    );
    assert_codes(
        &fields(
            r#"{ "name": "MaxBytes", "type": "int16", "versions": "0+",
                 "default": "0x7fffffff" }"#,
        ),
        &["KAFKA_SCHEMA_DEFAULT_TYPE"],
    );
}

#[test]
fn a_tag_and_its_versions_must_both_be_present() {
    assert_codes(
        &fields(
            r#"{ "name": "Extra", "type": "int32", "versions": "0+",
                 "taggedVersions": "2+" }"#,
        ),
        &["KAFKA_SCHEMA_MISSING_TAG"],
    );
    assert_codes(
        &fields(r#"{ "name": "Extra", "type": "int32", "versions": "0+", "tag": 0 }"#),
        &["KAFKA_SCHEMA_UNUSED_TAG"],
    );
    assert_codes(
        &fields(
            r#"{ "name": "Extra", "type": "int32", "versions": "0+",
                 "taggedVersions": "2+", "tag": 0 }"#,
        ),
        &[],
    );
}

#[test]
fn one_tag_number_belongs_to_one_sibling() {
    assert_codes(
        &fields(
            r#"{ "name": "First", "type": "int32", "versions": "0+",
                 "taggedVersions": "2+", "tag": 0 },
               { "name": "Second", "type": "int32", "versions": "0+",
                 "taggedVersions": "2+", "tag": 0 }"#,
        ),
        &["KAFKA_SCHEMA_DUPLICATE_TAG"],
    );
}

#[test]
fn a_tag_may_not_outlive_its_field_or_its_flexible_encoding() {
    assert_codes(
        &fields(
            r#"{ "name": "Extra", "type": "int32", "versions": "0-1",
                 "taggedVersions": "2+", "tag": 0 }"#,
        ),
        &["KAFKA_SCHEMA_TAG_OUTSIDE_FIELD"],
    );

    // There is no tagged-field section to carry a tag outside flexible versions.
    let legacy = fields(
        r#"{ "name": "Extra", "type": "int32", "versions": "0+",
             "taggedVersions": "0+", "tag": 0 }"#,
    )
    .replace(
        r#""flexibleVersions": "2+""#,
        r#""flexibleVersions": "none""#,
    );
    assert_codes(&legacy, &["KAFKA_SCHEMA_TAG_OUTSIDE_FLEXIBLE"]);
}

#[test]
fn tagged_versions_must_stay_open_so_a_tag_is_never_reused() {
    // Closing the range frees the number for a later version to reassign, and a
    // peer that skipped the versions in between would decode the new field as
    // the old one.
    assert_codes(
        &fields(
            r#"{ "name": "Extra", "type": "int32", "versions": "0+",
                 "taggedVersions": "2", "tag": 0 }"#,
        ),
        &["KAFKA_SCHEMA_TAG_NOT_OPEN_ENDED"],
    );
}

/// Builds a valid request whose root fields are the ones under test.
fn fields(fields: &str) -> String {
    format!(
        r#"{{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
          "validVersions": "0-2", "flexibleVersions": "2+",
          "fields": [ {fields} ] }}"#
    )
}

/// Runs the whole front end and asserts exactly which codes it collected.
fn assert_codes(source: &str, expected: &[&str]) {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    let message = lower_message(raw, PathBuf::from("fixture.json")).expect("fixture must lower");

    let actual = match validate_message(&message) {
        Ok(()) => Vec::new(),
        Err(errors) => errors.0.iter().map(|error| error.code).collect::<Vec<_>>(),
    };

    assert_eq!(actual, expected, "for schema:\n{source}");
}
