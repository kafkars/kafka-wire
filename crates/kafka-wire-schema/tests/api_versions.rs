//! The pinned `ApiVersions` request lowers into typed semantics.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use kafka_wire_schema::{DefaultValue, FieldType, load_message};

#[test]
fn loads_the_pinned_api_versions_request() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let path = root.join(
        "spec/upstream/apache-kafka/678c0e07e4733c5a592e52046dc2c4e1625587f1/message/ApiVersionsRequest.json",
    );

    let message = load_message(path).unwrap();

    assert_eq!(message.api_key, Some(18));
    assert_eq!(message.valid_versions.to_string(), "0-5");
    assert_eq!(message.effective_flexible_versions().to_string(), "3-5");
    assert_eq!(message.fields.len(), 4);
    assert_eq!(message.fields[0].ty, FieldType::String);
    assert_eq!(message.fields[3].default, DefaultValue::Integer(-1));
}
