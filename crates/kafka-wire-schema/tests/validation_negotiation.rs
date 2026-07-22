//! Request ownership of pair-level version-negotiation policy.
//!
//! Scenarios: an ordinary response is valid, while a response that claims the
//! request-side unstable-version flag receives one stable semantic diagnostic.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{SourceFile, lower_message, parse_jsonc, validate_message};

const RESPONSE: &str = r#"
{ "apiKey": 1, "type": "response", "name": "ExampleResponse",
  "validVersions": "0-2", "flexibleVersions": "2+", "fields": [] }
"#;

#[test]
fn only_a_request_owns_unstable_version_negotiation_policy() {
    assert_eq!(codes(RESPONSE), Vec::<&str>::new());
    assert_eq!(
        codes(&RESPONSE.replace(
            r#""flexibleVersions": "2+""#,
            r#""flexibleVersions": "2+", "latestVersionUnstable": true"#,
        )),
        ["KAFKA_SCHEMA_UNEXPECTED_UNSTABLE_POLICY"]
    );
}

fn codes(source: &str) -> Vec<&'static str> {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    let message = lower_message(raw, PathBuf::from("fixture.json")).expect("fixture must lower");
    validate_message(&message).map_or_else(
        |errors| errors.0.into_iter().map(|error| error.code).collect(),
        |()| Vec::new(),
    )
}
