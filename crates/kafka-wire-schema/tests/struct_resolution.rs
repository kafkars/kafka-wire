//! A struct name that binds to nothing, or to two things in one module, is a
//! diagnostic.
//!
//! Scenario: lowering hands every nested struct upstream's own spelling without
//! consulting anything, so the two ways it can go wrong are checked afterwards —
//! a reference that resolves to no declaration, and two declarations that land
//! in one module under one name.
//!
//! Both must fail in the schema layer with a code, a source path, and the names
//! involved. The alternative is generated Rust that names a type nothing
//! declares, or two `struct` items with one name: rustc reports those against
//! the generated file, which is disposable output, with no path back to the
//! schema that caused it.
//!
//! The scope is what these cases fix. A message module is the namespace, so two
//! *messages* declaring one spelling is correct and must pass, while
//! one message declaring a name twice — counting its own type — must still fail.
//! A guard that got those two backwards would pass every schema and emit code
//! that does not compile.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{
    Message, SourceFile, ValidationError, lower_message, parse_jsonc, validate_message,
    validate_struct_names,
};

#[test]
fn an_unresolvable_reference_names_the_field_the_struct_and_the_lost_type() {
    let error = fault(&lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-2", "flexibleVersions": "0+",
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "0+" } ] }"#,
    ));

    assert_eq!(error.code, "KAFKA_SCHEMA_UNRESOLVED_STRUCT");
    assert_eq!(error.path, PathBuf::from("fixture.json"));
    assert_eq!(error.field.as_deref(), Some("Topics"));
    assert!(
        error.message.contains("TopicData") && error.message.contains("ExampleRequest"),
        "the diagnostic must name both the lost spelling and the message whose \
         module would have had to define it, got: {}",
        error.message,
    );
}

#[test]
fn a_reference_that_binds_to_a_declaration_reports_nothing() {
    // The repaired half of the pair above. A detector that fired at everything
    // would pass the negative test alone.
    let message = lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-2", "flexibleVersions": "0+",
             "commonStructs": [
               { "name": "TopicData", "versions": "0+", "fields": [
                 { "name": "Name", "type": "string", "versions": "0+" } ] } ],
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "0+" } ] }"#,
    );

    assert_eq!(validate_message(&message), Ok(()));
}

#[test]
fn a_reference_cannot_escape_its_declarations_effective_versions() {
    let error = fault(&lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-2", "flexibleVersions": "0+",
             "commonStructs": [
               { "name": "TopicData", "versions": "1+", "fields": [
                 { "name": "Name", "type": "string", "versions": "1+" } ] } ],
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "0+" } ] }"#,
    ));

    assert_eq!(error.code, "KAFKA_SCHEMA_STRUCT_VERSION_ESCAPE");
    assert_eq!(error.field.as_deref(), Some("Topics"));
    assert!(
        error.message.contains("0-2") && error.message.contains("1-2"),
        "the diagnostic must name both effective windows, got: {}",
        error.message,
    );
}

#[test]
fn a_reference_inside_its_declarations_effective_versions_is_valid() {
    let message = lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-2", "flexibleVersions": "0+",
             "commonStructs": [
               { "name": "TopicData", "versions": "1+", "fields": [
                 { "name": "Name", "type": "string", "versions": "1+" } ] } ],
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "1+" } ] }"#,
    );

    assert_eq!(validate_message(&message), Ok(()));
}

#[test]
fn two_messages_may_declare_one_struct_name() {
    // The defining inversion. Under a flat namespace this pair was the canonical
    // unexportable collision: both messages declare `PartitionData`
    // and something had to give. Each now renders into its own module, so
    // nothing collides and the guard must say so — a guard still scoped
    // globally would reject a corpus that generates cleanly, which is the
    // failure that stops the whole decision.
    let messages = vec![
        lower(
            r#"{ "apiKey": 1, "type": "request", "name": "AlphaRequest",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Beta", "type": "PartitionData", "versions": "0+", "fields": [
                     { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
        ),
        lower(
            r#"{ "apiKey": 1, "type": "response", "name": "AlphaResponse",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Beta", "type": "PartitionData", "versions": "0+", "fields": [
                     { "name": "Code", "type": "int16", "versions": "0+" } ] } ] }"#,
        ),
    ];

    for message in &messages {
        assert_eq!(validate_message(message), Ok(()));
    }

    assert_eq!(validate_struct_names(&messages), Ok(()));
}

#[test]
fn a_message_type_holds_its_name_against_a_struct_it_declares() {
    // The module holds the message type as well as the structs, so the one
    // scope covers both. A single message declaring a struct spelled exactly
    // like itself is two items of one name inside one `pub mod` — `E0428` — and
    // it is the case a guard scoped only to declarations would miss.
    let messages = vec![lower(
        r#"{ "apiKey": 1, "type": "request", "name": "AlphaRequest",
             "validVersions": "0", "flexibleVersions": "0+",
             "fields": [
               { "name": "Echo", "type": "AlphaRequest", "versions": "0+", "fields": [
                 { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
    )];

    let errors = validate_struct_names(&messages)
        .expect_err("a message name and a struct it declares must not coincide");

    assert_eq!(errors.0[0].code, "KAFKA_SCHEMA_QUALIFIED_STRUCT_COLLISION");
    assert!(
        errors.0[0].message.contains("message `AlphaRequest`"),
        "the diagnostic must say a message claims the name, got: {}",
        errors.0[0].message,
    );
}

#[test]
fn distinct_messages_with_distinct_structs_pass_the_assertion() {
    // The ordinary case, kept so the guard is shown accepting something it has
    // no reason to reject. A detector that fired at everything would still fail
    // the two positive cases above; one that fired at nothing would pass all
    // three, which is what `a_message_type_holds_its_name_against_a_struct_it_declares`
    // is there to rule out.
    let messages = vec![
        lower(
            r#"{ "apiKey": 52, "type": "request", "name": "VoteRequest",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Topics", "type": "[]TopicData", "versions": "0+", "fields": [
                     { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
        ),
        lower(
            r#"{ "apiKey": 52, "type": "response", "name": "VoteResponse",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Topics", "type": "[]PartitionData", "versions": "0+", "fields": [
                     { "name": "Code", "type": "int16", "versions": "0+" } ] } ] }"#,
        ),
    ];

    assert_eq!(validate_struct_names(&messages), Ok(()));
}

/// Returns the single validation fault a fixture is written to produce.
fn fault(message: &Message) -> ValidationError {
    let errors = validate_message(message).expect_err("fixture must report a fault");
    assert_eq!(
        errors.0.len(),
        1,
        "fixture must isolate one fault, got {:?}",
        errors.0,
    );
    errors.0[0].clone()
}

fn lower(source: &str) -> Message {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    lower_message(raw, PathBuf::from("fixture.json")).expect("fixture must lower")
}
