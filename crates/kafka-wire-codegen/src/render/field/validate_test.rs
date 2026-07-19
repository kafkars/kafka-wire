//! The refusal half of the field-emission table.
//!
//! Scenario: for every normalized construct the first Rust backend does not
//! implement, assert that generation stops and that the diagnostic names the
//! message, the field, and the construct. This is the boundary that keeps the
//! emission table honest — every shape the table does not cover must be
//! refused here, or it silently reaches an emitter with no rule for it.
//!
//! Each case is paired with the accepting shape it departs from, so a rule that
//! stops firing and a rule that fires too widely both fail.

use kafka_wire_schema::{DefaultValue, Field, FieldType};

use super::{
    probe::{field, message, nullable, struct_type, versions},
    validate::validate_supported,
};

/// Returns the diagnostic for a message the backend must refuse.
fn refusal(message: &kafka_wire_schema::Message, situation: &str) -> String {
    validate_supported(message)
        .err()
        .unwrap_or_else(|| panic!("{situation} was accepted by the backend capability boundary"))
        .to_string()
}

/// Asserts one refusal names its cause, its message, and its field.
fn assert_refused(fields: Vec<Field>, valid: &str, flexible: &str, situation: &str, cause: &str) {
    let message = message(valid, flexible, fields);
    let diagnostic = refusal(&message, situation);

    assert!(
        diagnostic.contains(cause),
        "the refusal for {situation} must say `{cause}`: {diagnostic}"
    );
    assert!(
        diagnostic.contains("ProbeRequest."),
        "the refusal for {situation} must name the message and field: {diagnostic}"
    );
}

#[test]
fn the_supported_shape_is_accepted() {
    // Without this, every assertion below could pass because the boundary
    // refuses everything.
    let message = message(
        "0-4",
        "3+",
        vec![
            field("Probe", FieldType::String, "0+"),
            field("Count", FieldType::Int32, "2+"),
            nullable(field("Note", FieldType::String, "0+")),
        ],
    );

    assert!(
        validate_supported(&message).is_ok(),
        "the backend refused a message inside its own documented slice"
    );
}

#[test]
fn a_message_without_one_bounded_valid_interval_is_refused() {
    let mut open = message("0+", "none", vec![field("Probe", FieldType::Int32, "0+")]);
    open.valid_versions = versions("0+");
    assert!(
        validate_supported(&open).err().is_some_and(|error| error
            .to_string()
            .contains("one bounded valid version interval")),
        "an open-ended validVersions was accepted"
    );

    let disjoint = message(
        "0-1,3-4",
        "none",
        vec![field("Probe", FieldType::Int32, "0+")],
    );
    assert!(
        validate_supported(&disjoint).err().is_some_and(|error| {
            error
                .to_string()
                .contains("one bounded valid version interval")
        }),
        "a disjoint validVersions was accepted"
    );
}

#[test]
fn a_message_without_one_bounded_flexible_interval_is_refused() {
    let mut message = message("0-4", "none", vec![field("Probe", FieldType::Int32, "0+")]);
    message.flexible_versions = versions("0-1,3-4");

    let diagnostic = refusal(&message, "a disjoint flexibleVersions");
    assert!(
        diagnostic.contains("one bounded flexible version interval"),
        "the refusal must name the flexible interval: {diagnostic}"
    );
}

#[test]
fn a_field_with_a_disjoint_presence_interval_is_refused() {
    assert_refused(
        vec![field("Probe", FieldType::Int32, "0-1,3-4")],
        "0-4",
        "none",
        "a field present in two disjoint version ranges",
        "one bounded field-presence interval",
    );
}

#[test]
fn a_field_present_in_no_valid_version_is_refused_by_name() {
    assert_refused(
        vec![field("Probe", FieldType::Int32, "0")],
        "1-2",
        "none",
        "a field retired before the message's first supported version",
        "declared in no version this message supports",
    );
}

#[test]
fn a_known_tagged_field_is_refused() {
    let mut tagged = field("Probe", FieldType::Int32, "0+");
    tagged.tag = Some(0);
    tagged.tagged_versions = versions("0+");
    assert_refused(
        vec![tagged],
        "0-4",
        "0+",
        "a field with an assigned flexible tag",
        "known tagged fields are not implemented yet",
    );
}

#[test]
fn an_inline_struct_in_a_flexible_message_is_refused() {
    // Structs themselves are emitted now. What this backend cannot yet write is
    // the tagged-field section a struct carries inside a flexible message, so
    // the refusal moved from the construct to that one combination.
    let mut parent = field("Probe", struct_type("TopicData"), "0+");
    parent.fields = vec![field("Name", FieldType::String, "0+")];
    assert_refused(
        vec![parent],
        "0-4",
        "0+",
        "an inline struct in a flexible message",
        "structs in flexible messages are not implemented yet",
    );
}

#[test]
fn an_inline_struct_in_a_legacy_message_is_accepted() {
    // The paired accepting shape: without this, the refusal above could pass
    // because the boundary rejects every struct.
    let mut parent = field("Probe", struct_type("TopicData"), "0+");
    parent.fields = vec![field("Name", FieldType::String, "0+")];
    let message = message("0-4", "none", vec![parent]);

    assert!(
        validate_supported(&message).is_ok(),
        "the backend refused an inline struct in a non-flexible message"
    );
}

#[test]
fn partial_version_nullability_is_refused() {
    // A field that is nullable in some of the versions it appears in needs the
    // null case gated by version, which this backend cannot express.
    let mut partial = field("Probe", FieldType::String, "0+");
    partial.nullable_versions = versions("2+");
    partial.default = DefaultValue::Null;
    assert_refused(
        vec![partial],
        "0-4",
        "none",
        "a field nullable in only some of its versions",
        "partial-version nullability is not implemented yet",
    );
}

#[test]
fn a_nullable_field_with_a_non_null_default_is_refused() {
    let mut probe = nullable(field("Probe", FieldType::String, "0+"));
    probe.default = DefaultValue::String("PLAINTEXT".to_owned());
    assert_refused(
        vec![probe],
        "0-4",
        "none",
        "a nullable field defaulting to a literal",
        "nullable fields currently require a null protocol default",
    );
}

#[test]
fn an_array_outside_the_legacy_non_null_slice_is_refused() {
    let array = || field("Probe", FieldType::Array(Box::new(FieldType::String)), "0+");
    let cause = "the initial array backend supports only non-null legacy arrays";

    let mut gated = array();
    gated.versions = versions("2+");
    assert_refused(
        vec![gated],
        "0-4",
        "none",
        "a string array added at a later version",
        cause,
    );

    assert_refused(
        vec![nullable(array())],
        "0-4",
        "none",
        "a nullable string array",
        cause,
    );

    assert_refused(
        vec![array()],
        "0-4",
        "0+",
        "a string array in a flexible message",
        cause,
    );
}

#[test]
fn every_field_type_outside_the_slice_is_refused_by_name() {
    for ty in [
        FieldType::Uint16,
        FieldType::Uint32,
        FieldType::Float64,
        FieldType::Bytes,
        FieldType::Records,
    ] {
        let message = message("0-4", "none", vec![field("Probe", ty.clone(), "0+")]);
        let diagnostic = refusal(&message, &format!("a {ty:?} field"));

        assert!(
            diagnostic.contains("outside the initial backend slice")
                && diagnostic.contains(&format!("{ty:?}")),
            "the refusal for {ty:?} must name the type and the slice: {diagnostic}"
        );
    }
}
