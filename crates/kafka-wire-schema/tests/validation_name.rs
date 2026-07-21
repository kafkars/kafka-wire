//! Request and response names retain a nonempty API identity before grouping.
//!
//! Scenarios: the literal names `Request` and `Response` are rejected with one
//! stable semantic diagnostic, while adding the same API stem repairs both.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use kafka_wire_schema::{SourceFile, lower_message, parse_jsonc, validate_message};

#[test]
fn request_and_response_names_must_leave_a_nonempty_api_stem() {
    for (kind, name) in [("request", "Request"), ("response", "Response")] {
        assert_codes(&schema(kind, name), &["KAFKA_SCHEMA_EMPTY_API_STEM"]);
        assert_codes(&schema(kind, &format!("Example{name}")), &[]);
    }
}

fn schema(kind: &str, name: &str) -> String {
    format!(
        r#"{{ "apiKey": 1, "type": "{kind}", "name": "{name}",
          "validVersions": "0-2", "flexibleVersions": "2+", "fields": [] }}"#
    )
}

fn assert_codes(source: &str, expected: &[&str]) {
    let file = SourceFile::new("fixture.json", source);
    let raw = parse_jsonc(&file).expect("fixture must parse");
    let message = lower_message(raw, PathBuf::from("fixture.json")).expect("fixture must lower");
    let actual = validate_message(&message).map_or_else(
        |errors| errors.0.iter().map(|error| error.code).collect::<Vec<_>>(),
        |()| Vec::new(),
    );
    assert_eq!(actual, expected, "for schema:\n{source}");
}
