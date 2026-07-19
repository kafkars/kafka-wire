//! Message identity, version range, listeners, and the per-message struct table.
//!
//! Every stable diagnostic in this domain gets a pair: one schema that must
//! trip it and one minimally-repaired schema that must trip nothing. A negative
//! test alone cannot tell a working detector from one that fires at everything.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{SourceFile, lower_message, parse_jsonc, validate_message};

/// One request schema, written as the parts a test needs to vary.
///
/// `Default` is a schema that violates nothing, so every case below reads as
/// "the valid request, except this one property" and the diagnostic it expects
/// can only have come from that property.
struct Request {
    api_key: &'static str,
    name: &'static str,
    valid_versions: &'static str,
    listeners: &'static str,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            api_key: r#""apiKey": 1,"#,
            name: "ExampleRequest",
            valid_versions: "0-2",
            listeners: r#"["broker"]"#,
        }
    }
}

impl Request {
    fn render(&self) -> String {
        let Self {
            api_key,
            name,
            valid_versions,
            listeners,
        } = self;
        format!(
            r#"{{ {api_key} "type": "request", "name": "{name}",
              "validVersions": "{valid_versions}", "flexibleVersions": "2+",
              "listeners": {listeners},
              "fields": [ {{ "name": "Host", "type": "string", "versions": "0+" }} ] }}"#
        )
    }
}

#[test]
fn a_schema_with_no_faults_reports_nothing() {
    assert_codes(&Request::default().render(), &[]);
}

#[test]
fn valid_versions_must_be_one_interval_but_may_be_empty() {
    assert_codes(
        &Request {
            valid_versions: "0-1,3-4",
            ..Request::default()
        }
        .render(),
        &["KAFKA_SCHEMA_VALID_RANGE"],
    );

    // `"none"` retires an API without deleting its schema. It is one normalized
    // value, not a disjoint set, and every field under it is absent by design
    // rather than by mistake.
    assert_codes(
        &Request {
            valid_versions: "none",
            ..Request::default()
        }
        .render(),
        &[],
    );
}

#[test]
fn a_dispatched_message_must_declare_a_non_negative_api_key() {
    assert_codes(
        &Request {
            api_key: "",
            ..Request::default()
        }
        .render(),
        &["KAFKA_SCHEMA_MISSING_API_KEY"],
    );
    assert_codes(
        &Request {
            api_key: r#""apiKey": -1,"#,
            ..Request::default()
        }
        .render(),
        &["KAFKA_SCHEMA_NEGATIVE_API_KEY"],
    );
}

#[test]
fn a_header_is_not_dispatched_so_it_must_not_claim_an_api_key() {
    const HEADER: &str = r#"
    { "type": "header", "name": "ExampleHeader", "validVersions": "0-2",
      "flexibleVersions": "2+",
      "fields": [ { "name": "Host", "type": "string", "versions": "0+" } ] }
    "#;

    assert_codes(HEADER, &[]);
    assert_codes(
        &HEADER.replace(r#""type": "header""#, r#""apiKey": 1, "type": "header""#),
        &["KAFKA_SCHEMA_UNEXPECTED_API_KEY"],
    );
}

#[test]
fn a_data_schema_is_neither_dispatched_nor_suffixed() {
    const DATA: &str = r#"
    { "type": "data", "name": "AbortedTxn", "validVersions": "0",
      "flexibleVersions": "none",
      "fields": [ { "name": "ProducerId", "type": "int64", "versions": "0+" } ] }
    "#;

    assert_codes(DATA, &[]);
}

#[test]
fn a_message_name_must_match_the_kind_it_declares() {
    assert_codes(
        &Request {
            name: "ExampleThing",
            ..Request::default()
        }
        .render(),
        &["KAFKA_SCHEMA_DIRECTION_NAME"],
    );
}

#[test]
fn only_a_request_is_accepted_on_a_listener() {
    const RESPONSE: &str = r#"
    { "apiKey": 1, "type": "response", "name": "ExampleResponse",
      "validVersions": "0-2", "flexibleVersions": "2+",
      "fields": [ { "name": "Host", "type": "string", "versions": "0+" } ] }
    "#;

    assert_codes(RESPONSE, &[]);
    assert_codes(
        &RESPONSE.replace(
            r#""flexibleVersions": "2+""#,
            r#""flexibleVersions": "2+", "listeners": ["broker"]"#,
        ),
        &["KAFKA_SCHEMA_UNEXPECTED_LISTENERS"],
    );
}

#[test]
fn listener_names_must_be_present_and_distinct() {
    assert_codes(
        &Request {
            listeners: r#"["  "]"#,
            ..Request::default()
        }
        .render(),
        &["KAFKA_SCHEMA_EMPTY_LISTENER"],
    );
    assert_codes(
        &Request {
            listeners: r#"["broker", "broker"]"#,
            ..Request::default()
        }
        .render(),
        &["KAFKA_SCHEMA_DUPLICATE_LISTENER"],
    );
    assert_codes(
        &Request {
            listeners: r#"["broker", "controller"]"#,
            ..Request::default()
        }
        .render(),
        &[],
    );
}

#[test]
fn a_struct_reference_must_resolve_within_its_message() {
    const TOPICS: &str = r#"{ "name": "Topics", "type": "[]TopicData", "versions": "0+" }"#;

    assert_codes(
        &with_fields(TOPICS, ""),
        &["KAFKA_SCHEMA_UNRESOLVED_STRUCT"],
    );
    assert_codes(
        &with_fields(
            TOPICS,
            r#""commonStructs": [ { "name": "TopicData", "versions": "0+",
                "fields": [ { "name": "Name", "type": "string", "versions": "0+" } ] } ],"#,
        ),
        &[],
    );
}

#[test]
fn one_message_may_not_declare_one_struct_name_twice() {
    // the earlier flat naming rule qualifies nested structs by their owning message, which is only
    // sufficient because no message declares one name twice. That is a measured
    // property of today's corpus, so it is asserted rather than assumed.
    assert_codes(
        &with_fields(
            r#"{ "name": "Topics", "type": "[]TopicData", "versions": "0+",
                 "fields": [ { "name": "Name", "type": "string", "versions": "0+" } ] }"#,
            r#""commonStructs": [ { "name": "TopicData", "versions": "0+",
                "fields": [ { "name": "Name", "type": "string", "versions": "0+" } ] } ],"#,
        ),
        &["KAFKA_SCHEMA_DUPLICATE_STRUCT"],
    );
}

#[test]
fn a_common_struct_nobody_refers_to_is_a_fault() {
    assert_codes(
        &with_fields(
            r#"{ "name": "Host", "type": "string", "versions": "0+" }"#,
            r#""commonStructs": [ { "name": "Orphan", "versions": "0+",
                "fields": [ { "name": "Name", "type": "string", "versions": "0+" } ] } ],"#,
        ),
        &["KAFKA_SCHEMA_UNUSED_COMMON_STRUCT"],
    );
}

#[test]
fn two_common_structs_may_not_refer_to_each_other() {
    // Inline declarations cannot cycle because JSON nesting is a tree, but a
    // common struct is reachable by name from anywhere in the message. An
    // undetected cycle is an infinitely sized Rust type, not a late diagnostic.
    assert_codes(
        &with_fields(
            r#"{ "name": "First", "type": "Left", "versions": "0+" }"#,
            r#""commonStructs": [
                { "name": "Left", "versions": "0+",
                  "fields": [ { "name": "Next", "type": "Right", "versions": "0+" } ] },
                { "name": "Right", "versions": "0+",
                  "fields": [ { "name": "Back", "type": "Left", "versions": "0+" } ] } ],"#,
        ),
        &["KAFKA_SCHEMA_STRUCT_CYCLE"],
    );
}

#[test]
fn a_map_key_inside_a_common_struct_is_not_a_root_field() {
    // A `commonStructs` body is written at the top of the file but is only
    // reached through a field that refers to it, so its members are struct
    // members and `mapKey` is meaningful on them.
    assert_codes(
        &with_fields(
            r#"{ "name": "Topics", "type": "[]TopicData", "versions": "0+" }"#,
            r#""commonStructs": [ { "name": "TopicData", "versions": "0+",
                "fields": [ { "name": "Name", "type": "string", "versions": "0+",
                              "mapKey": true } ] } ],"#,
        ),
        &[],
    );
}

/// Builds a request whose root fields and struct table are given explicitly.
fn with_fields(fields: &str, common_structs: &str) -> String {
    format!(
        r#"{{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
          "validVersions": "0-2", "flexibleVersions": "2+",
          {common_structs} "fields": [ {fields} ] }}"#
    )
}

/// Runs the whole front end and returns the diagnostic codes it collected.
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
