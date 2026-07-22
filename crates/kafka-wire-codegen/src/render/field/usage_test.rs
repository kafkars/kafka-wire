//! Version-parameter names derive from field semantics rather than Rust text.
//!
//! Scenarios: fixed-width values stay version-free across a flexible boundary,
//! while length-prefix changes, nested codecs, and presence gates require it.

use kafka_wire_schema::FieldType;

use super::{
    probe::{field, message, struct_type},
    usage::{encoded_value_uses_version, inline_write_uses_version},
};

#[test]
fn encoded_value_usage_follows_wire_shape() {
    for (ty, expected) in [
        (FieldType::Int32, false),
        (FieldType::String, true),
        (FieldType::Array(Box::new(FieldType::Int32)), true),
        (struct_type("Nested"), true),
    ] {
        let message = message("0-1", "1+", vec![field("Probe", ty, "0+")]);
        assert_eq!(
            encoded_value_uses_version(&message.fields[0], &message),
            expected
        );
    }
}

#[test]
fn an_inline_presence_gate_uses_version_even_for_a_fixed_width_value() {
    let message = message("0-1", "none", vec![field("Probe", FieldType::Int32, "1+")]);
    assert!(inline_write_uses_version(&message.fields[0], &message));
}
