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
    probe::{field, message, nullable, struct_type, tagged, versions},
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
fn a_known_tagged_field_inside_the_flexible_window_is_accepted() {
    // The shape upstream actually writes, and the paired positive for the three
    // refusals below: `versions` and `taggedVersions` agree and both sit inside
    // the window where the section exists at all.
    let message = message(
        "0-4",
        "0+",
        vec![tagged(field("Probe", FieldType::Int32, "0+"), 0)],
    );

    assert!(
        validate_supported(&message).is_ok(),
        "the backend refused a well-formed known tagged field"
    );
}

#[test]
fn a_tagged_field_reaching_outside_the_flexible_window_is_refused() {
    // The tagged section exists only in flexible versions, which is what makes
    // every tagged value compact. A tag present at v0 of a message flexible
    // from v3 would be handed to the same codec and rendered in the legacy
    // form — plausible bytes in the wrong format rather than a failure.
    assert_refused(
        vec![tagged(field("Probe", FieldType::Int32, "0+"), 0)],
        "0-4",
        "3+",
        "a tag present before its message became flexible",
        "present in versions the message is not flexible in",
    );
}

#[test]
fn a_field_tagged_in_only_some_of_its_versions_is_refused() {
    // The generated gate is built from `versions`. If `taggedVersions` were
    // narrower, the field would be written into the section in versions where
    // upstream says it belongs inline.
    let mut partial = tagged(field("Probe", FieldType::Int32, "0+"), 0);
    partial.tagged_versions = versions("2+");
    assert_refused(
        vec![partial],
        "0-4",
        "0+",
        "a field tagged for only part of its life",
        "tagged in only some of the versions it appears in",
    );
}

#[test]
fn tagged_versions_without_a_tag_number_is_refused() {
    // Upstream spells one construct with two keys and the emitter keys on the
    // number, so a field carrying only the window has no slot to write to.
    let mut numberless = field("Probe", FieldType::Int32, "0+");
    numberless.tagged_versions = versions("0+");
    assert_refused(
        vec![numberless],
        "0-4",
        "0+",
        "a field declaring taggedVersions with no tag",
        "taggedVersions without a tag number",
    );
}

#[test]
fn an_inline_struct_is_accepted_in_either_encoding_regime() {
    // A struct carries its own tagged-field section in a flexible message and
    // none in a legacy one. Both are emitted, so both must be accepted here —
    // this is the paired positive for the nullable-struct refusal below.
    for flexible in ["none", "0+"] {
        let mut parent = field("Probe", struct_type("TopicData"), "0+");
        parent.fields = vec![field("Name", FieldType::String, "0+")];
        let message = message("0-4", flexible, vec![parent]);

        assert!(
            validate_supported(&message).is_ok(),
            "the backend refused an inline struct with flexibleVersions {flexible}"
        );
    }
}

#[test]
fn an_inline_nullable_struct_is_accepted_in_either_encoding_regime() {
    // The presence marker ahead of the body is a raw int8 whichever regime the
    // message is in, so both sides of the flexible boundary are emitted and
    // both must be accepted. This is the paired positive for the tagged
    // refusal below.
    for flexible in ["none", "0+"] {
        let mut parent = nullable(field("Probe", struct_type("TopicData"), "0+"));
        parent.fields = vec![field("Name", FieldType::String, "0+")];
        parent.default = DefaultValue::Null;
        let message = message("0-4", flexible, vec![parent]);

        assert!(
            validate_supported(&message).is_ok(),
            "the backend refused an inline nullable struct with flexibleVersions {flexible}"
        );
    }
}

#[test]
fn a_nullable_tagged_struct_field_is_refused() {
    // Apache Kafka spells the marker as a varint when the struct travels in the
    // tagged section and as an int8 when it is inline, and no pinned schema
    // declares the tagged form. Emitting it would be a guess about bytes
    // nothing in this repository can check.
    let mut parent = nullable(tagged(field("Probe", struct_type("TopicData"), "0+"), 0));
    parent.fields = vec![field("Name", FieldType::String, "0+")];
    parent.default = DefaultValue::Null;
    assert_refused(
        vec![parent],
        "0-4",
        "0+",
        "a nullable struct field carrying a tag",
        "nullable tagged struct fields are not implemented yet",
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
fn a_nullable_field_may_default_to_a_real_value() {
    // Upstream writes both `Option<T>` holding a value and `None`. Collapsing
    // them would encode an absent field where the protocol declares a present
    // one, so the backend carries the distinction rather than refusing it.
    let mut probe = nullable(field("Probe", FieldType::String, "0+"));
    probe.default = DefaultValue::String("PLAINTEXT".to_owned());
    let message = message("0-4", "none", vec![probe]);

    assert!(
        validate_supported(&message).is_ok(),
        "the backend refused a nullable field defaulting to a literal"
    );
}

#[test]
fn arrays_are_accepted_gated_compact_and_nullable_alike() {
    let array = || field("Probe", FieldType::Array(Box::new(FieldType::String)), "0+");

    // The length prefix follows the encoding regime, and the nullable readers
    // return Option<usize>, so an absent array stays distinct from an empty one.
    let mut gated = array();
    gated.versions = versions("2+");
    let mut null_default = nullable(array());
    null_default.default = DefaultValue::Null;
    for (fields, situation) in [
        (vec![gated], "a string array added at a later version"),
        (vec![array()], "a string array in a flexible message"),
        (vec![null_default], "a nullable string array"),
    ] {
        let message = message("0-4", "0+", fields);
        assert!(
            validate_supported(&message).is_ok(),
            "the backend refused {situation}"
        );
    }
}

#[test]
fn an_array_of_an_unsupported_element_is_refused_by_name() {
    // Every scalar the protocol declares now has a codec, so the last shape
    // outside the backend is an array whose element has none: an array of
    // arrays would need a second length prefix the element path never emits.
    let nested = FieldType::Array(Box::new(FieldType::Array(Box::new(FieldType::Int32))));
    let message = message("0-4", "none", vec![field("Probe", nested, "0+")]);
    let diagnostic = refusal(&message, "an array of arrays");

    assert!(
        diagnostic.contains("outside the initial backend slice"),
        "the refusal must name the slice: {diagnostic}"
    );
}
