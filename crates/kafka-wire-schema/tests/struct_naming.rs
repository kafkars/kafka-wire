//! the module-scoped naming rule: what name does a nested struct get, and who owns it?
//!
//! Scenario: lower one message, then read the identities its struct table and
//! its field types now carry. Every case here fixes a generated type name, and
//! a type name cannot be revisited after mass generation without breaking every
//! consumer that imports it — so each case asserts the name itself, never that
//! some name was produced.
//!
//! The rule has one arm now: keep upstream's spelling, and let the message's
//! module be the scope. What these cases pin down is that the owner is still
//! *recorded* even though it no longer appears in the name — it is what selects
//! the module, so a regression that dropped it would emit two structs into one
//! module and only surface as rustc `E0428` on generated code. The name and the
//! owner are therefore asserted separately, and a case where two messages
//! declare one spelling asserts that they differ by owner and by module alone.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{
    Message, Qualification, SourceFile, StructOrigin, lower_message, parse_jsonc,
};

#[test]
fn a_nested_struct_keeps_the_spelling_upstream_gave_it() {
    // API key 0, the message the prior-art analysis makes the scope target. Upstream's field
    // name `TopicData` and its struct name `TopicProduceData` are already
    // distinct, and only the struct name participates: the emitted name is the
    // declared one, never the field that carries the reference.
    let message = lower(
        r#"{ "apiKey": 0, "type": "request", "name": "ProduceRequest",
             "validVersions": "0-2", "flexibleVersions": "none",
             "fields": [
               { "name": "TopicData", "type": "[]TopicProduceData", "versions": "0+",
                 "fields": [
                   { "name": "Name", "type": "string", "versions": "0+", "mapKey": true },
                   { "name": "PartitionData", "type": "[]PartitionProduceData",
                     "versions": "0+", "fields": [
                       { "name": "Index", "type": "int32", "versions": "0+" } ] } ] } ] }"#,
    );

    assert_eq!(
        emitted(&message),
        vec![
            ("TopicProduceData", "TopicProduceData"),
            ("PartitionProduceData", "PartitionProduceData"),
        ],
    );
    // The owner survives the name it no longer prefixes: it is what puts both
    // of these into `produce_request` rather than beside some other message's
    // `TopicProduceData`.
    for declaration in message.structs.declarations() {
        assert_eq!(declaration.name.owner(), "ProduceRequest");
    }
}

#[test]
fn a_name_upstream_already_qualified_is_left_exactly_as_written() {
    // Upstream hand-qualifies forty of the corpus's declarations, and this is
    // the longest of them at 42 characters. the earlier flat naming rule had to detect and elide the
    // repeat to keep it from becoming 75; the module-scoped naming rule has nothing to elide, so this
    // spelling is simply the corpus's longest name.
    let message = lower(
        r#"{ "apiKey": 90, "type": "response",
             "name": "DescribeShareGroupOffsetsResponse",
             "validVersions": "0", "flexibleVersions": "0+",
             "fields": [
               { "name": "Responses", "type": "[]DescribeShareGroupOffsetsResponsePartition",
                 "versions": "0+", "fields": [
                   { "name": "PartitionIndex", "type": "int32", "versions": "0+" } ] } ] }"#,
    );

    let partition = &message.structs.declarations()[0].name;

    assert_eq!(
        partition.rust_type(),
        "DescribeShareGroupOffsetsResponsePartition",
    );
    assert_eq!(partition.qualification(), Qualification::ModuleScoped);
    assert_eq!(partition.owner(), "DescribeShareGroupOffsetsResponse");
}

#[test]
fn a_name_that_merely_resembles_its_owner_is_not_touched_either() {
    // `VoteRequestor` begins with `VoteRequest` as bytes while naming something
    // else. the earlier flat naming rule needed a camel-case boundary test to keep from eliding
    // there and merging two types; no rule inspects the name at all now, so the
    // hazard is gone rather than guarded. Both spellings pass through whole.
    let message = lower(
        r#"{ "apiKey": 52, "type": "request", "name": "VoteRequest",
             "validVersions": "0", "flexibleVersions": "0+",
             "fields": [
               { "name": "Requestor", "type": "VoteRequestor", "versions": "0+",
                 "fields": [ { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
    );

    assert_eq!(emitted(&message), vec![("VoteRequestor", "VoteRequestor")]);
    for declaration in message.structs.declarations() {
        assert_eq!(
            declaration.name.qualification(),
            Qualification::ModuleScoped
        );
    }
}

#[test]
fn nesting_depth_never_reaches_the_name() {
    // `AlterPartitionRequest` is the corpus's deepest chain. A struct three
    // levels down is spelled exactly as upstream spelled it, which bounds every
    // generated name at `len(struct)` however deep upstream nests.
    let message = lower(
        r#"{ "apiKey": 56, "type": "request", "name": "AlterPartitionRequest",
             "validVersions": "0-3", "flexibleVersions": "0+",
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "0+", "fields": [
                 { "name": "Partitions", "type": "[]PartitionData", "versions": "0+",
                   "fields": [
                     { "name": "NewIsrWithEpochs", "type": "[]BrokerState", "versions": "0+",
                       "fields": [
                         { "name": "BrokerId", "type": "int32", "versions": "0+" } ] } ] } ] } ] }"#,
    );

    assert_eq!(
        emitted(&message),
        vec![
            ("TopicData", "TopicData"),
            ("PartitionData", "PartitionData"),
            ("BrokerState", "BrokerState"),
        ],
    );
}

#[test]
fn the_two_directions_of_one_api_key_differ_by_module_not_by_spelling() {
    // API key 52, the canonical collision. Upstream declares `PartitionData` in
    // both directions with genuinely different fields. Under the earlier flat naming rule they were
    // two different type names in one module; under the module-scoped naming rule they are the *same*
    // type name in two different modules — which is why the owner has to survive
    // qualification, and why the module can never be per API key.
    let request = lower(
        r#"{ "apiKey": 52, "type": "request", "name": "VoteRequest",
             "validVersions": "0-1", "flexibleVersions": "0+",
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "0+", "fields": [
                 { "name": "Partitions", "type": "[]PartitionData", "versions": "0+",
                   "fields": [
                     { "name": "PreVote", "type": "bool", "versions": "1+" } ] } ] } ] }"#,
    );
    let response = lower(
        r#"{ "apiKey": 52, "type": "response", "name": "VoteResponse",
             "validVersions": "0-1", "flexibleVersions": "0+",
             "fields": [
               { "name": "Topics", "type": "[]TopicData", "versions": "0+", "fields": [
                 { "name": "Partitions", "type": "[]PartitionData", "versions": "0+",
                   "fields": [
                     { "name": "VoteGranted", "type": "bool", "versions": "0+" } ] } ] } ] }"#,
    );

    // Identical spellings, in both directions.
    let names = vec![
        ("TopicData", "TopicData"),
        ("PartitionData", "PartitionData"),
    ];
    assert_eq!(emitted(&request), names);
    assert_eq!(emitted(&response), names);

    // Separated by the owner each declaration records, and by the module that
    // owner names. Nothing else keeps these four types apart.
    for declaration in request.structs.declarations() {
        assert_eq!(declaration.name.owner(), "VoteRequest");
    }
    for declaration in response.structs.declarations() {
        assert_eq!(declaration.name.owner(), "VoteResponse");
    }
    assert_eq!(request.name.rust_module(), "vote_request");
    assert_eq!(response.name.rust_module(), "vote_response");
    assert_ne!(request.name.rust_module(), response.name.rust_module());
}

#[test]
fn a_common_struct_takes_the_ordinary_rule_with_no_special_case() {
    // `commonStructs` is a top-level block of one message file, so it is scoped
    // to one direction and is not shared with the opposite one. It lands in the
    // same module an inline declaration lands in, under the same spelling.
    let message = lower(
        r#"{ "apiKey": 55, "type": "response", "name": "DescribeQuorumResponse",
             "validVersions": "0-2", "flexibleVersions": "0+",
             "commonStructs": [
               { "name": "ReplicaState", "versions": "0+", "fields": [
                 { "name": "ReplicaId", "type": "int32", "versions": "0+" } ] } ],
             "fields": [
               { "name": "CurrentVoters", "type": "[]ReplicaState", "versions": "0+" },
               { "name": "Observers", "type": "[]ReplicaState", "versions": "0+" } ] }"#,
    );

    assert_eq!(message.common_structs[0].name.rust_type(), "ReplicaState");
    assert_eq!(
        message.common_structs[0].name.owner(),
        "DescribeQuorumResponse",
    );
    assert_eq!(emitted(&message), vec![("ReplicaState", "ReplicaState")]);
}

#[test]
fn the_struct_table_unifies_both_declaration_forms_in_source_order() {
    // One table, two spellings. Each entry keeps its effective version window:
    // the declaration or carrying field intersected with its owner.
    let message = lower(
        r#"{ "apiKey": 1, "type": "request", "name": "ExampleRequest",
             "validVersions": "0-4", "flexibleVersions": "0+",
             "commonStructs": [
               { "name": "Hoisted", "versions": "2+", "fields": [
                 { "name": "Id", "type": "int32", "versions": "2+" } ] } ],
             "fields": [
               { "name": "Common", "type": "[]Hoisted", "versions": "2+" },
               { "name": "Nested", "type": "[]Inline", "versions": "3+", "fields": [
                 { "name": "Name", "type": "string", "versions": "3+" } ] } ] }"#,
    );

    let declarations = message.structs.declarations();

    assert_eq!(declarations.len(), 2);
    assert_eq!(declarations[0].name.declared(), "Hoisted");
    assert_eq!(declarations[0].origin, StructOrigin::Common);
    assert_eq!(declarations[0].versions.to_string(), "2-4");
    assert_eq!(declarations[1].name.declared(), "Inline");
    assert_eq!(declarations[1].origin, StructOrigin::Inline);
    assert_eq!(declarations[1].versions.to_string(), "3-4");

    assert_eq!(
        message
            .structs
            .resolve("Inline")
            .expect("an inline declaration must resolve by its upstream spelling")
            .name
            .rust_type(),
        "Inline",
    );
    assert!(message.structs.resolve("Absent").is_none());
}

/// Returns each declared struct as the pair (upstream spelling, emitted type).
fn emitted(message: &Message) -> Vec<(&str, &str)> {
    message
        .structs
        .declarations()
        .iter()
        .map(|declaration| (declaration.name.declared(), declaration.name.rust_type()))
        .collect()
}

fn lower(source: &str) -> Message {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    lower_message(raw, PathBuf::from("fixture.json")).expect("fixture must lower")
}
