//! What survives the walk from upstream JSON into the normalized IR.
//!
//! Lowering is where upstream semantics are either preserved or quietly lost.
//! These tests assert the preservation — that `entityType`, `zeroCopy`, the
//! struct table, and every scalar default arrive intact — and that a spelling
//! the adapter cannot interpret fails loudly instead of becoming a plausible
//! substitute.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{
    DefaultValue, EntityType, FieldType, LowerError, Message, MessageKind, SourceFile, StructRef,
    lower_message, parse_jsonc,
};

#[test]
fn entity_type_reaches_the_ir_as_a_typed_value() {
    // A client routing a request has no other machine-readable statement that
    // this particular int32 is a broker id rather than a partition index.
    let message = lower(&request(
        r#"{ "name": "Topic", "type": "string", "versions": "0+",
             "entityType": "topicName" },
           { "name": "Leader", "type": "int32", "versions": "0+",
             "entityType": "brokerId" },
           { "name": "Plain", "type": "int32", "versions": "0+" }"#,
    ));

    assert_eq!(message.fields[0].entity_type, Some(EntityType::TopicName));
    assert_eq!(message.fields[1].entity_type, Some(EntityType::BrokerId));
    assert_eq!(message.fields[2].entity_type, None);
}

#[test]
fn every_entity_type_upstream_spells_is_modeled() {
    for spelling in [
        "topicName",
        "brokerId",
        "groupId",
        "producerId",
        "transactionalId",
    ] {
        let message = lower(&request(&format!(
            r#"{{ "name": "Value", "type": "string", "versions": "0+",
                  "entityType": "{spelling}" }}"#
        )));

        assert_eq!(
            message.fields[0]
                .entity_type
                .expect("a declared entityType must survive lowering")
                .as_str(),
            spelling,
        );
    }
}

#[test]
fn an_unmodeled_entity_type_is_an_error_not_an_opaque_string() {
    let error = lower_error(&request(
        r#"{ "name": "Value", "type": "string", "versions": "0+",
             "entityType": "clusterLinkId" }"#,
    ));

    assert!(
        matches!(&error, LowerError::EntityType { field, reason, .. }
            if field == "Value" && reason.contains("clusterLinkId")),
        "expected an entityType diagnostic naming the spelling, got {error}",
    );
}

#[test]
fn zero_copy_reaches_the_ir_as_a_decoding_hint() {
    let message = lower(&request(
        r#"{ "name": "Data", "type": "bytes", "versions": "0+", "zeroCopy": true },
           { "name": "Other", "type": "bytes", "versions": "0+" }"#,
    ));

    assert!(message.fields[0].zero_copy);
    assert!(!message.fields[1].zero_copy);
}

#[test]
fn a_field_may_pin_its_own_flexible_encoding() {
    // `RequestHeader.ClientId` keeps the legacy two-byte length prefix even in
    // flexible versions, so a broker can read the header of an
    // ApiVersionsRequest before it knows which version the client chose.
    let message = lower(&request(
        r#"{ "name": "ClientId", "type": "string", "versions": "0+",
             "flexibleVersions": "none" },
           { "name": "Host", "type": "string", "versions": "0+" }"#,
    ));

    assert_eq!(
        message.fields[0]
            .flexible_versions
            .as_ref()
            .map(ToString::to_string),
        Some("none".to_owned()),
    );
    assert_eq!(message.fields[1].flexible_versions, None);
}

#[test]
fn an_unknown_type_spelling_never_becomes_a_phantom_struct() {
    // `strng` is lowercase, so it cannot be a struct reference and must not
    // silently become one that no declaration will ever satisfy.
    let error = lower_error(&request(
        r#"{ "name": "Value", "type": "strng", "versions": "0+" }"#,
    ));

    assert!(
        matches!(&error, LowerError::FieldType { field, reason, .. }
            if field == "Value" && reason.contains("strng")),
        "expected a type diagnostic naming the spelling, got {error}",
    );
}

#[test]
fn float64_is_a_type_rather_than_a_struct_named_float64() {
    let message = lower(&request(
        r#"{ "name": "Quota", "type": "float64", "versions": "0+" }"#,
    ));

    assert_eq!(message.fields[0].ty, FieldType::Float64);
    assert_eq!(
        message.fields[0].default,
        DefaultValue::Float(kafka_wire_schema::FloatDefault::new(0.0)),
    );
}

#[test]
fn a_uuid_defaults_to_the_zero_uuid_and_parses_an_explicit_one() {
    let message = lower(&request(
        r#"{ "name": "Implicit", "type": "uuid", "versions": "0+" },
           { "name": "Explicit", "type": "uuid", "versions": "0+",
             "default": "00112233-4455-6677-8899-aabbccddeeff" }"#,
    ));

    assert_eq!(message.fields[0].default, DefaultValue::Uuid([0; 16]));
    assert_eq!(
        message.fields[1].default,
        DefaultValue::Uuid([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]),
    );
}

#[test]
fn a_non_nullable_struct_defaults_to_its_members_defaults() {
    let message = lower(&request(
        r#"{ "name": "Leader", "type": "LeaderIdAndEpoch", "versions": "0+", "fields": [
             { "name": "LeaderId", "type": "int32", "versions": "0+", "default": "-1" } ] }"#,
    ));

    assert_eq!(message.fields[0].default, DefaultValue::StructDefaults);
}

#[test]
fn header_and_data_schemas_lower_without_an_api_key() {
    let header = lower(
        r#"{ "type": "header", "name": "RequestHeader", "validVersions": "1-2",
             "flexibleVersions": "2+",
             "fields": [ { "name": "CorrelationId", "type": "int32", "versions": "0+" } ] }"#,
    );
    assert_eq!(header.kind, MessageKind::Header);
    assert_eq!(header.api_key, None);

    let data = lower(
        r#"{ "type": "data", "name": "AbortedTxn", "validVersions": "0",
             "flexibleVersions": "none",
             "fields": [ { "name": "ProducerId", "type": "int64", "versions": "0+" } ] }"#,
    );
    assert_eq!(data.kind, MessageKind::Data);
    assert_eq!(data.api_key, None);
}

#[test]
fn a_schema_that_predates_tagged_fields_is_flexible_at_no_version() {
    // The four APIs Apache Kafka 4.0 retired omit `flexibleVersions` entirely.
    // Absent must mean "never flexible", not "flexible everywhere".
    let message = lower(
        r#"{ "apiKey": 7, "type": "request", "name": "ControlledShutdownRequest",
             "validVersions": "none",
             "fields": [ { "name": "BrokerId", "type": "int32", "versions": "0+" } ] }"#,
    );

    assert!(message.flexible_versions.is_empty());
}

#[test]
fn latest_version_unstable_is_kept_as_negotiation_policy() {
    let message = lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-2", "flexibleVersions": "2+",
             "latestVersionUnstable": true,
             "fields": [ { "name": "Host", "type": "string", "versions": "0+" } ] }"#,
    );

    assert!(message.latest_version_unstable);
}

#[test]
fn common_structs_become_the_message_struct_table() {
    let message = lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-2", "flexibleVersions": "2+",
             "commonStructs": [ { "name": "ReplicaState", "versions": "0+", "fields": [
               { "name": "ReplicaId", "type": "int32", "versions": "0+" } ] } ],
             "fields": [ { "name": "Voters", "type": "[]ReplicaState", "versions": "0+" } ] }"#,
    );

    assert_eq!(message.common_structs.len(), 1);
    assert_eq!(message.common_structs[0].name.declared(), "ReplicaState");
    assert_eq!(message.common_structs[0].versions.to_string(), "0+");
    assert_eq!(message.common_structs[0].fields.len(), 1);
    assert_eq!(
        message.fields[0]
            .ty
            .struct_reference()
            .map(StructRef::declared),
        Some("ReplicaState"),
    );
}

#[test]
fn inline_nesting_deeper_than_the_adapter_walks_is_rejected() {
    // The pinned corpus nests five levels. The bound exists so a crafted file
    // cannot choose how deep the front end recurses.
    let mut nested = r#"{ "name": "Leaf", "type": "string", "versions": "0+" }"#.to_owned();
    for depth in 0..40 {
        nested = format!(
            r#"{{ "name": "Level{depth}", "type": "[]Level{depth}Data", "versions": "0+",
                  "fields": [ {nested} ] }}"#
        );
    }

    let error = lower_error(&request(&nested));

    assert!(
        matches!(&error, LowerError::NestingDepth { limit, .. } if *limit == 32),
        "expected a nesting-depth diagnostic, got {error}",
    );
}

#[test]
fn source_control_characters_are_rejected_at_the_ir_boundary() {
    let message_source = request("").replacen("ExampleRequest", "Example\\u000aRequest", 1);
    let message_error = lower_error(&message_source);
    assert!(
        matches!(&message_error, LowerError::Identifier { kind, name, .. }
            if *kind == "message" && name == "Example\nRequest"),
        "expected a contained message-name diagnostic, got {message_error}"
    );

    let field_error = lower_error(&request(
        r#"{ "name": "Bad\u2028Field", "type": "string", "versions": "0+" }"#,
    ));
    assert!(
        matches!(&field_error, LowerError::Identifier { kind, name, .. }
            if *kind == "field" && name == "Bad\u{2028}Field"),
        "expected a contained field-name diagnostic, got {field_error}"
    );
}

/// Builds a valid request whose root fields are the ones under test.
fn request(fields: &str) -> String {
    format!(
        r#"{{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
          "validVersions": "0-2", "flexibleVersions": "2+",
          "fields": [ {fields} ] }}"#
    )
}

fn lower(source: &str) -> Message {
    outcome(source).expect("fixture must lower")
}

fn lower_error(source: &str) -> LowerError {
    outcome(source).expect_err("fixture must fail to lower")
}

fn outcome(source: &str) -> Result<Message, LowerError> {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    lower_message(raw, PathBuf::from("fixture.json"))
}
