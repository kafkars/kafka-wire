//! Explicit capability boundary for the first Rust rendering backend.

use kafka_wire_schema::{FieldType, Message};

use crate::GenerationError;

pub(crate) fn validate_supported(message: &Message) -> Result<(), GenerationError> {
    if message.valid_versions.single_bounded().is_none() {
        return unsupported(
            message,
            "<message>",
            "the initial backend requires one bounded valid version interval",
        );
    }

    let flexible = message.effective_flexible_versions();
    if !flexible.is_empty() && flexible.single_bounded().is_none() {
        return unsupported(
            message,
            "<message>",
            "the initial backend requires one bounded flexible version interval",
        );
    }

    validate_fields(&message.fields, message)
}

/// Checks one field list, recursing into the members of any struct it declares.
///
/// Struct members are versioned against the same message, so they face exactly
/// the same presence, nullability, and type rules as a root field.
fn validate_fields(
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<(), GenerationError> {
    for field in fields {
        let present = field.versions.intersection(&message.valid_versions);
        // Named before the shape check below, which would otherwise report a
        // field that exists nowhere as merely having the wrong number of
        // intervals. Upstream does write these: a field kept in the schema
        // after every version carrying it was retired has an empty presence.
        if present.is_empty() {
            return unsupported(
                message,
                field.name.protocol(),
                "the field is declared in no version this message supports",
            );
        }
        if present.single_bounded().is_none() {
            return unsupported(
                message,
                field.name.protocol(),
                "the initial backend requires one bounded field-presence interval",
            );
        }
        validate_tag(field, message, &present)?;
        let nullable = field
            .nullable_versions
            .intersection(&message.valid_versions);
        if !nullable.is_empty() && nullable != present {
            return unsupported(
                message,
                field.name.protocol(),
                "partial-version nullability is not implemented yet",
            );
        }
        // Checked before the shape match so that the scalar arm stays a plain
        // type list: folding this into a guard there would drop nullable
        // strings, which the backend does support, into the refusal arm.
        //
        // An inline nullable struct is emitted: it carries a one-byte presence
        // marker ahead of its body. A tagged one spells that marker as a varint
        // rather than an int8, and no pinned schema declares one — all nine
        // nullable structs in the corpus are inline — so it stays refused
        // rather than emitted on a guess about bytes nothing here can check.
        if !nullable.is_empty() && matches!(field.ty, FieldType::Struct(_)) && field.tag.is_some() {
            return unsupported(
                message,
                field.name.protocol(),
                "nullable tagged struct fields are not implemented yet",
            );
        }

        match &field.ty {
            FieldType::String
            | FieldType::Bool
            | FieldType::Int8
            | FieldType::Int16
            | FieldType::Uint16
            | FieldType::Uint32
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::Uuid
            | FieldType::Float64
            | FieldType::Bytes
            | FieldType::Records
            | FieldType::Struct(_) => {}
            FieldType::Array(element) if is_supported_element(element) => {}
            other @ FieldType::Array(_) => {
                return unsupported(
                    message,
                    field.name.protocol(),
                    &format!("field type {other:?} is outside the initial backend slice"),
                );
            }
        }

        validate_fields(&field.fields, message)?;
    }
    Ok(())
}

/// Checks the three things the tagged-field emitter assumes about a known tag.
///
/// Each one is silent if it is wrong. The emitter reads a tag's presence window
/// to decide its version gate, and the codec picks compact or legacy from that
/// same window — so a tag whose windows disagree with the section it travels in
/// would encode plausible bytes in the wrong format rather than fail.
fn validate_tag(
    field: &kafka_wire_schema::Field,
    message: &Message,
    present: &kafka_wire_schema::VersionSet,
) -> Result<(), GenerationError> {
    // Upstream spells one construct with two keys, and the emitter keys on the
    // number. A field carrying only `taggedVersions` has no slot to write to.
    let Some(_) = field.tag else {
        if field.tagged_versions.is_empty() {
            return Ok(());
        }
        return unsupported(
            message,
            field.name.protocol(),
            "the field declares taggedVersions without a tag number",
        );
    };

    // The section exists only in flexible versions, which is what makes every
    // tagged value compact. A tag reachable outside that window would be
    // rendered by the same codec in the legacy form and read as garbage.
    let flexible = message.effective_flexible_versions();
    if !present.is_subset_of(&flexible) {
        return unsupported(
            message,
            field.name.protocol(),
            "a tagged field is present in versions the message is not flexible in",
        );
    }

    // The generated gate is built from `versions`; if `taggedVersions` were
    // narrower, the field would be written into the section in versions where
    // upstream says it belongs inline.
    let tagged = field.tagged_versions.intersection(&message.valid_versions);
    if &tagged != present {
        return unsupported(
            message,
            field.name.protocol(),
            "a tagged field is tagged in only some of the versions it appears in",
        );
    }
    Ok(())
}

/// Whether the array backend has an element codec for this type.
///
/// Nested arrays are absent: an array of arrays needs a second length prefix
/// the element path does not emit, so it stays refused by name.
fn is_supported_element(element: &FieldType) -> bool {
    matches!(
        element,
        FieldType::String
            | FieldType::Bool
            | FieldType::Int8
            | FieldType::Int16
            | FieldType::Uint16
            | FieldType::Uint32
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::Uuid
            | FieldType::Float64
            | FieldType::Bytes
            | FieldType::Struct(_)
    )
}

fn unsupported<T>(message: &Message, field: &str, reason: &str) -> Result<T, GenerationError> {
    Err(GenerationError::unsupported(message, field, reason))
}
