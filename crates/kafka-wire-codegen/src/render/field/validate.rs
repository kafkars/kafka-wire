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

    for field in &message.fields {
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
        if !field.fields.is_empty() {
            return unsupported(
                message,
                field.name.protocol(),
                "inline structs are not implemented yet",
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

        match &field.ty {
            FieldType::String | FieldType::Int16 | FieldType::Int32 => {}
            FieldType::Array(element) if matches!(element.as_ref(), FieldType::String) => {
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
    }
    Ok(())
}

fn unsupported<T>(message: &Message, field: &str, reason: &str) -> Result<T, GenerationError> {
    Err(GenerationError::unsupported(message, field, reason))
}
