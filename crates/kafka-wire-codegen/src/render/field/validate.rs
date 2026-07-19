//! Explicit capability boundary for the first Rust rendering backend.

use kafka_wire_schema::{DefaultValue, FieldType, Message};

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

    // A struct in a flexible message carries a tagged-field section of its own,
    // which this backend does not emit yet. Refusing at the message rather than
    // at each member keeps the reason singular and the field walk simple.
    if !flexible.is_empty() && declares_struct(message) {
        return unsupported(
            message,
            "<message>",
            "structs in flexible messages are not implemented yet",
        );
    }

    validate_fields(&message.fields, message)
}

/// Whether this message declares any struct, by either spelling.
fn declares_struct(message: &Message) -> bool {
    !message.common_structs.is_empty() || fields_declare_struct(&message.fields)
}

fn fields_declare_struct(fields: &[kafka_wire_schema::Field]) -> bool {
    fields
        .iter()
        .any(|field| !field.fields.is_empty() || fields_declare_struct(&field.fields))
}

/// Checks one field list, recursing into the members of any struct it declares.
///
/// Struct members are versioned against the same message, so they face exactly
/// the same presence, nullability, and type rules as a root field.
fn validate_fields(
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<(), GenerationError> {
    let flexible = message.effective_flexible_versions();
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
        if field.tag.is_some() || !field.tagged_versions.is_empty() {
            return unsupported(
                message,
                field.name.protocol(),
                "known tagged fields are not implemented yet",
            );
        }
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
        if !nullable.is_empty() && field.default != DefaultValue::Null {
            return unsupported(
                message,
                field.name.protocol(),
                "nullable fields currently require a null protocol default",
            );
        }

        // Checked before the shape match so that the scalar arm stays a plain
        // type list: folding this into a guard there would drop nullable
        // strings, which the backend does support, into the refusal arm.
        if !nullable.is_empty() && matches!(field.ty, FieldType::Struct(_)) {
            return unsupported(
                message,
                field.name.protocol(),
                "nullable struct fields are not implemented yet",
            );
        }

        match &field.ty {
            FieldType::String
            | FieldType::Bool
            | FieldType::Int8
            | FieldType::Int16
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::Uuid
            | FieldType::Struct(_) => {}
            FieldType::Array(element) if is_supported_element(element) => {
                let flexible_presence = present.intersection(&flexible);
                if present != message.valid_versions
                    || !nullable.is_empty()
                    || !flexible_presence.is_empty()
                {
                    return unsupported(
                        message,
                        field.name.protocol(),
                        "the initial array backend supports only non-null legacy arrays present in every version",
                    );
                }
            }
            other => {
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
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::Uuid
            | FieldType::Struct(_)
    )
}

fn unsupported<T>(message: &Message, field: &str, reason: &str) -> Result<T, GenerationError> {
    Err(GenerationError::unsupported(message, field, reason))
}
