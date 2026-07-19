//! A struct name that binds to nothing, or to two things, is a diagnostic.
//!
//! Scenario: qualification hands every nested struct a name without consulting
//! anything, so the two ways it can go wrong are checked afterwards — a
//! reference that resolves to no declaration, and two declarations that resolve
//! to one emitted type.
//!
//! Both must fail in the schema layer with a code, a source path, and the names
//! involved. The alternative is generated Rust that names a type nothing
//! declares, or two `struct` items with one name: rustc reports those against
//! the generated file, which is disposable output, with no path back to the
//! schema that caused it.

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
        error.message.contains("TopicData") && error.message.contains("ExampleRequestTopicData"),
        "the diagnostic must name both the upstream spelling and the type that \
         would have been emitted, got: {}",
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
fn two_messages_may_not_qualify_down_to_one_generated_type() {
    // Neither message is faulty on its own, and no per-message rule can see
    // this: `AlphaRequest` declaring `BetaGamma` and the data schema
    // `AlphaRequestBeta` declaring `Gamma` both qualify to
    // `AlphaRequestBetaGamma`. The crate facade re-exports every generated type
    // flat, so this is unexportable — caught here or discovered by rustc.
    let messages = vec![
        lower(
            r#"{ "apiKey": 1, "type": "request", "name": "AlphaRequest",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Beta", "type": "BetaGamma", "versions": "0+", "fields": [
                     { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
        ),
        lower(
            r#"{ "type": "data", "name": "AlphaRequestBeta",
                 "validVersions": "0", "flexibleVersions": "none",
                 "fields": [
                   { "name": "Gamma", "type": "Gamma", "versions": "0+", "fields": [
                     { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
        ),
    ];

    for message in &messages {
        assert_eq!(validate_message(message), Ok(()));
    }

    let errors = validate_struct_names(&messages)
        .expect_err("two declarations qualifying to one type must be reported");

    assert_eq!(errors.0.len(), 1);
    assert_eq!(errors.0[0].code, "KAFKA_SCHEMA_QUALIFIED_STRUCT_COLLISION");
    assert!(
        errors.0[0].message.contains("AlphaRequestBetaGamma")
            && errors.0[0].message.contains("AlphaRequest")
            && errors.0[0].message.contains("AlphaRequestBeta"),
        "the diagnostic must name the contested type and both owners, got: {}",
        errors.0[0].message,
    );
}

#[test]
fn a_message_type_holds_its_name_against_a_nested_struct() {
    // Message types and nested structs share one namespace, so the assertion
    // has to cover both. A struct qualifying to exactly some message's name
    // collides with it just as surely as with another struct.
    let messages = vec![
        lower(
            r#"{ "apiKey": 1, "type": "request", "name": "AlphaRequest",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Beta", "type": "BetaGamma", "versions": "0+", "fields": [
                     { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
        ),
        lower(
            r#"{ "type": "data", "name": "AlphaRequestBetaGamma",
                 "validVersions": "0", "flexibleVersions": "none",
                 "fields": [ { "name": "Id", "type": "int32", "versions": "0+" } ] }"#,
        ),
    ];

    let errors = validate_struct_names(&messages)
        .expect_err("a message name and a qualified struct name must not coincide");

    assert_eq!(errors.0[0].code, "KAFKA_SCHEMA_QUALIFIED_STRUCT_COLLISION");
    assert!(
        errors.0[0]
            .message
            .contains("message `AlphaRequestBetaGamma`"),
        "the diagnostic must say a message claims the name, got: {}",
        errors.0[0].message,
    );
}

#[test]
fn distinct_messages_with_distinct_structs_pass_the_assertion() {
    let messages = vec![
        lower(
            r#"{ "apiKey": 52, "type": "request", "name": "VoteRequest",
                 "validVersions": "0", "flexibleVersions": "0+",
                 "fields": [
                   { "name": "Topics", "type": "[]PartitionData", "versions": "0+", "fields": [
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

    // The same upstream spelling in both directions of one API key is the case
    // the earlier flat naming rule exists for, and it must pass rather than merely not crash.
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
