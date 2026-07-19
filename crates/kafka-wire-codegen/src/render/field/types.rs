//! Rust type, default, and value-comparison rendering for one normalized field.
//!
//! This file owns the mapping from a normalized field to the Rust text that
//! declares it, initializes it, and decides whether it still holds its protocol
//! default. A construct with no mapping is a named error, never a rendered
//! comment: `/* unsupported */` in a struct field position is a syntax error
//! caught by rustfmt, but in a default or comparison position it can compile
//! and silently change what the encoder writes.
//!
//! It deliberately owns no version reasoning beyond nullability, which decides
//! `Option`; presence gates belong to `version.rs`.

use kafka_wire_schema::{DefaultValue, Field, FieldType, Message};

use crate::GenerationError;

pub(crate) fn rust_type(field: &Field, message: &Message) -> Result<String, GenerationError> {
    let nullable = !field
        .nullable_versions
        .intersection(&message.valid_versions)
        .is_empty();
    let base = match &field.ty {
        FieldType::String => "StrBytes".to_owned(),
        FieldType::Int16 => "i16".to_owned(),
        FieldType::Int32 => "i32".to_owned(),
        FieldType::Array(element) if matches!(element.as_ref(), FieldType::String) => {
            "Vec<StrBytes>".to_owned()
        }
        other => {
            return Err(GenerationError::unsupported(
                message,
                field.name.protocol(),
                format!("field type {other:?} has no Rust type in this backend"),
            ));
        }
    };
    if nullable {
        Ok(format!("Option<{base}>"))
    } else {
        Ok(base)
    }
}

pub(crate) fn default_expression(
    field: &Field,
    message: &Message,
) -> Result<String, GenerationError> {
    match &field.default {
        DefaultValue::Null => Ok("None".to_owned()),
        DefaultValue::Bool(value) => Ok(value.to_string()),
        DefaultValue::Integer(value) => Ok(value.to_string()),
        DefaultValue::String(value) if value.is_empty() => Ok("StrBytes::default()".to_owned()),
        DefaultValue::String(value) => Ok(format!("StrBytes::from({value:?})")),
        DefaultValue::Empty => match &field.ty {
            FieldType::Array(_) => Ok("Vec::new()".to_owned()),
            _ => Ok("Default::default()".to_owned()),
        },
        // `validate_supported` restricts the slice to string, int16, int32, and
        // legacy string arrays, so no field carrying one of these defaults
        // should reach the renderer. Should is not a guarantee, so widening the
        // slice without widening this match fails generation instead of
        // emitting an initializer that is not the protocol default.
        other => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            format!("protocol default {other:?} has no Rust initializer in this backend"),
        )),
    }
}

pub(crate) fn non_default_condition(
    field: &Field,
    message: &Message,
) -> Result<String, GenerationError> {
    let name = field.name.rust_field();
    match &field.default {
        DefaultValue::Null => Ok(format!("self.{name}.is_some()")),
        DefaultValue::Bool(value) => Ok(format!("self.{name} != {value}")),
        DefaultValue::Integer(value) => Ok(format!("self.{name} != {value}")),
        DefaultValue::String(value) if value.is_empty() => Ok(format!("!self.{name}.is_empty()")),
        DefaultValue::String(value) => Ok(format!("self.{name}.as_str() != {value:?}")),
        DefaultValue::Empty => Ok(format!("!self.{name}.is_empty()")),
        other => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            format!("protocol default {other:?} has no equality test in this backend"),
        )),
    }
}

pub(crate) fn uses_rust_default(field: &Field) -> bool {
    matches!(
        (&field.ty, &field.default),
        (FieldType::String, DefaultValue::String(value)) if value.is_empty()
    ) || matches!(
        (&field.ty, &field.default),
        (
            FieldType::Int16 | FieldType::Int32,
            DefaultValue::Integer(0)
        )
    ) || matches!(
        (&field.ty, &field.default),
        (FieldType::Array(_), DefaultValue::Empty)
    ) || matches!(&field.default, DefaultValue::Null)
}

pub(crate) fn is_legacy_string_array(field: &Field) -> bool {
    matches!(
        &field.ty,
        FieldType::Array(element) if matches!(element.as_ref(), FieldType::String)
    )
}
