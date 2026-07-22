//! API-pair construction failure scenarios.
//!
//! These tests feed normalized-but-not-yet-validated IR directly into grouping
//! to prove every hostile pair returns a structured diagnostic and never uses
//! the trusted-name panic path.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{SourceFile, lower_message, parse_jsonc};

use crate::{GenerationError, PairError, group::group_sources, source::MessageSource};

#[test]
fn normalized_empty_api_names_never_panic_grouping() {
    for (request, response) in [
        ("---Request", "---Response"),
        ("_Request", "_Response"),
        (" Request", " Response"),
        ("...Request", "...Response"),
    ] {
        let error = group_sources(vec![
            source("request", request, "0-2", "2+", false),
            source("response", response, "0-2", "2+", false),
        ])
        .expect_err("malformed API identity must fail");
        assert!(
            matches!(
                error,
                GenerationError::Pair(PairError::InvalidApiName { .. })
            ),
            "{request}/{response} produced {error:?}"
        );
    }
}

#[test]
fn a_pair_requires_identical_supported_versions() {
    let error = group_sources(vec![
        source("request", "ExampleRequest", "0-2", "2+", false),
        source("response", "ExampleResponse", "0-1", "1+", false),
    ])
    .expect_err("mismatched supported versions must fail");

    assert!(matches!(
        error,
        GenerationError::Pair(PairError::SupportedVersions {
            api_key: 1,
            request,
            response,
        }) if request == "0-2" && response == "0-1"
    ));
}

#[test]
fn a_pair_requires_identical_effective_flexible_versions() {
    let error = group_sources(vec![
        source("request", "ExampleRequest", "0-2", "2+", false),
        source("response", "ExampleResponse", "0-2", "1+", false),
    ])
    .expect_err("mismatched flexible versions must fail");

    assert!(matches!(
        error,
        GenerationError::Pair(PairError::FlexibleVersions {
            api_key: 1,
            request,
            response,
        }) if request == "2" && response == "1-2"
    ));
}

#[test]
fn response_metadata_cannot_claim_request_negotiation_policy() {
    let error = group_sources(vec![
        source("request", "ExampleRequest", "0-2", "2+", false),
        source("response", "ExampleResponse", "0-2", "2+", true),
    ])
    .expect_err("response-side unstable policy must fail");

    assert!(matches!(
        error,
        GenerationError::Pair(PairError::UnstablePolicy {
            api_key: 1,
            response,
        }) if response == "ExampleResponse"
    ));
}

fn source(
    kind: &str,
    name: &str,
    valid_versions: &str,
    flexible_versions: &str,
    latest_version_unstable: bool,
) -> MessageSource {
    let unstable = if latest_version_unstable {
        r#", "latestVersionUnstable": true"#
    } else {
        ""
    };
    let json = format!(
        r#"{{ "apiKey": 1, "type": "{kind}", "name": "{name}",
          "validVersions": "{valid_versions}", "flexibleVersions": "{flexible_versions}"
          {unstable}, "fields": [] }}"#
    );
    let filename = format!("{name}.json");
    let file = SourceFile::new(&filename, json);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    let message = lower_message(raw, PathBuf::from(&filename)).expect("fixture must lower");
    MessageSource {
        message,
        filename,
        sha256: "fixture".to_owned(),
    }
}
