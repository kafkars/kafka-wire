//! Corpus-probe failures stay local to the schemas that caused them.
//!
//! Scenarios: an incomplete API pair and a generated namespace collision are
//! classified without suppressing an unrelated complete API pair.

#![allow(clippy::expect_used)]

use std::{collections::BTreeMap, path::PathBuf};

use kafka_wire_schema::SourceFile;

use crate::{CorpusOutcome, corpus_classify::classify_semantics, source::MessageSource};

fn source(filename: &str, schema: &str) -> MessageSource {
    let file = SourceFile::new(PathBuf::from(filename), schema);
    let message = kafka_wire_schema::load_source(file).expect("fixture schema must load");
    MessageSource {
        message,
        filename: filename.to_owned(),
        sha256: "fixture".to_owned(),
    }
}

fn directional(filename: &str, api_key: i16, kind: &str, name: &str) -> MessageSource {
    source(
        filename,
        &format!(
            r#"{{
                "apiKey": {api_key},
                "type": "{kind}",
                "name": "{name}",
                "validVersions": "0",
                "flexibleVersions": "none",
                "fields": []
            }}"#
        ),
    )
}

fn good_pair() -> [MessageSource; 2] {
    [
        directional("GoodRequest.json", 1, "request", "GoodRequest"),
        directional("GoodResponse.json", 1, "response", "GoodResponse"),
    ]
}

#[test]
fn an_incomplete_pair_does_not_abort_unrelated_pair_classification() {
    let mut sources = good_pair().to_vec();
    sources.push(directional(
        "BrokenRequest.json",
        2,
        "request",
        "BrokenRequest",
    ));
    let mut outcomes = BTreeMap::new();

    let grouped = classify_semantics(sources, &mut outcomes).expect("probe classification");

    assert_eq!(grouped.api.len(), 1);
    assert_eq!(grouped.api[0].name.protocol_stem(), "Good");
    assert!(matches!(
        outcomes.get("BrokenRequest.json"),
        Some(CorpusOutcome::NotRendered { reason }) if reason.contains("no response schema")
    ));
}

#[test]
fn a_namespace_failure_does_not_abort_unrelated_pair_classification() {
    let mut sources = good_pair().to_vec();
    sources.push(source(
        "Message.json",
        r#"{
            "type": "data",
            "name": "Message",
            "validVersions": "0",
            "flexibleVersions": "none",
            "fields": []
        }"#,
    ));
    let mut outcomes = BTreeMap::new();

    let grouped = classify_semantics(sources, &mut outcomes).expect("probe classification");

    assert_eq!(grouped.api.len(), 1);
    assert!(grouped.unkeyed.is_empty());
    assert!(matches!(
        outcomes.get("Message.json"),
        Some(CorpusOutcome::NotRendered { reason })
            if reason.contains("handwritten private crate-root module")
    ));
}
