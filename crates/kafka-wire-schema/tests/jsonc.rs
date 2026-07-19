//! JSONC parsing preserves comment-like text inside strings.

#![allow(clippy::unwrap_used)]

use kafka_wire_schema::{SourceFile, parse_jsonc};

#[test]
fn strips_line_and_block_comments_without_touching_strings() {
    let source = SourceFile::new(
        "fixture.json",
        r#"{
          // comment
          "apiKey": 1,
          "type": "request",
          "name": "ExampleRequest",
          "validVersions": "0-1",
          "flexibleVersions": "none",
          "listeners": ["broker"],
          "fields": [
            { "name": "Url", "type": "string", "versions": "0+", "about": "https://example.test/*not-comment*/" }
          ]
        }"#,
    );

    let message = parse_jsonc(&source).unwrap();

    assert_eq!(message.name, "ExampleRequest");
    assert_eq!(
        message.fields[0].about,
        "https://example.test/*not-comment*/"
    );
}
