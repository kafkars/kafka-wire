//! the earlier flat naming rule: what name does a nested struct get, and who owns it?
//!
//! Scenario: lower one message, then read the identities its struct table and
//! its field types now carry. Every case here fixes a generated type name, and
//! a type name cannot be revisited after mass generation without breaking every
//! consumer that imports it — so each case asserts the name itself, never that
//! some name was produced.
//!
//! The rule has two arms: prefix the owning message, or elide it where upstream
//! already wrote it. Forty of the pinned corpus's 308 declarations take the
//! second arm, and a regression that collapsed the rule to a pure prefix would
//! still emit names — just names 75 characters long with the owner spelled
//! twice. Both arms are therefore asserted by the names they yield.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{
    Message, Qualification, SourceFile, StructOrigin, lower_message, parse_jsonc,
};

#[test]
fn a_nested_struct_is_named_by_the_message_that_declares_it() {
    // API key 0, the message the prior-art analysis makes the scope target. Upstream's field
    // name `TopicData` and its struct name `TopicProduceData` are already
    // distinct, and only the struct name participates: qualification is by the
    // owning message, never by the field that carries the reference.
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
            ("TopicProduceData", "ProduceRequestTopicProduceData"),
            ("PartitionProduceData", "ProduceRequestPartitionProduceData"),
        ],
    );
}

#[test]
fn a_name_upstream_already_qualified_is_not_qualified_twice() {
    // The stutter-elision arm. Re-prefixing here would emit
    // `DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition`
    // at 75 characters; eliding the repeat is what bounds the corpus at 74.
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
    assert_eq!(partition.qualification(), Qualification::AlreadyQualified);
    assert_eq!(partition.owner(), "DescribeShareGroupOffsetsResponse");
}

#[test]
fn elision_needs_a_name_boundary_rather_than_a_text_prefix() {
    // `VoteRequestor` begins with `VoteRequest` as bytes while naming something
    // else, and a struct spelled exactly like its own message is not qualified
    // at all by keeping it — it would collide with the message type in the
    // module the two share. Neither is a leading segment of the OWNER, so
    // neither is left alone.
    //
    // Both do open on the API stem `Vote` at a name boundary, so both take the
    // stem-deduplicated arm rather than repeating it: the qualified name is the
    // owner plus what follows the stem. `VoteRequestVoteRequestor` would have
    // been the alternative, and the corpus shows where that leads — prefixing
    // the whole owner produced a seventy-character type that said
    // `DescribeUserScramCredentials` twice.
    let message = lower(
        r#"{ "apiKey": 52, "type": "request", "name": "VoteRequest",
             "validVersions": "0", "flexibleVersions": "0+",
             "fields": [
               { "name": "Requestor", "type": "VoteRequestor", "versions": "0+",
                 "fields": [ { "name": "Id", "type": "int32", "versions": "0+" } ] },
               { "name": "Echo", "type": "VoteRequest", "versions": "0+",
                 "fields": [ { "name": "Id", "type": "int32", "versions": "0+" } ] } ] }"#,
    );

    assert_eq!(
        emitted(&message),
        vec![
            ("VoteRequestor", "VoteRequestRequestor"),
            ("VoteRequest", "VoteRequestRequest"),
        ],
    );
    for declaration in message.structs.declarations() {
        assert_eq!(
            declaration.name.qualification(),
            Qualification::StemDeduplicated,
        );
    }
}

#[test]
fn nesting_depth_never_reaches_the_name() {
    // `AlterPartitionRequest` is the corpus's deepest chain. A struct three
    // levels down is qualified by its message and nothing else, which bounds
    // every name at `len(message) + len(struct)` however deep upstream nests.
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
            ("TopicData", "AlterPartitionRequestTopicData"),
            ("PartitionData", "AlterPartitionRequestPartitionData"),
            ("BrokerState", "AlterPartitionRequestBrokerState"),
        ],
    );
}

#[test]
fn the_two_directions_of_one_api_key_stop_colliding() {
    // API key 52, the canonical collision. Upstream declares `PartitionData` in
    // both directions with genuinely different fields, and `kafka-wire-codegen`
    // renders both into one module. Before qualification this pair was two
    // rustc E0428s; the point of the rule is that it is now four types.
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

    assert_eq!(
        emitted(&request),
        vec![
            ("TopicData", "VoteRequestTopicData"),
            ("PartitionData", "VoteRequestPartitionData"),
        ],
    );
    assert_eq!(
        emitted(&response),
        vec![
            ("TopicData", "VoteResponseTopicData"),
            ("PartitionData", "VoteResponsePartitionData"),
        ],
    );
}

#[test]
fn a_common_struct_takes_the_ordinary_rule_with_no_special_case() {
    // `commonStructs` is a top-level block of one message file, so it is scoped
    // to one direction and is not shared with the opposite one. It gets the
    // same owner qualification an inline declaration gets.
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

    assert_eq!(
        message.common_structs[0].name.rust_type(),
        "DescribeQuorumResponseReplicaState",
    );
    assert_eq!(
        emitted(&message),
        vec![("ReplicaState", "DescribeQuorumResponseReplicaState")],
    );
}

#[test]
fn the_struct_table_unifies_both_declaration_forms_in_source_order() {
    // One table, two spellings. Each entry keeps its own version window: a
    // `commonStructs` entry states one, and an inline body takes the presence
    // window of the field that carries it, because that is exactly where it
    // exists.
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
    assert_eq!(declarations[0].versions.to_string(), "2+");
    assert_eq!(declarations[1].name.declared(), "Inline");
    assert_eq!(declarations[1].origin, StructOrigin::Inline);
    assert_eq!(declarations[1].versions.to_string(), "3+");

    assert_eq!(
        message
            .structs
            .resolve("Inline")
            .expect("an inline declaration must resolve by its upstream spelling")
            .name
            .rust_type(),
        "ExampleRequestInline",
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
