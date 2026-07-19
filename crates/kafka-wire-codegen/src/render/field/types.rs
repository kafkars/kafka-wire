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

/// Groups an integer literal in threes, as the lints on checked-in output ask.
fn separated(value: i64) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            grouped.push('_');
        }
        grouped.push(digit);
    }
    if negative {
        format!("-{grouped}")
    } else {
        grouped
    }
}

/// Whether the field is declared nullable anywhere the message supports, which
/// is what decides `Option`.
fn is_nullable(field: &Field, message: &Message) -> bool {
    !field
        .nullable_versions
        .intersection(&message.valid_versions)
        .is_empty()
}

pub(crate) fn rust_type(field: &Field, message: &Message) -> Result<String, GenerationError> {
    let nullable = is_nullable(field, message);
    let base = type_name(&field.ty, field, message)?;
    if nullable {
        Ok(format!("Option<{base}>"))
    } else {
        Ok(base)
    }
}

/// Maps one type to its Rust spelling, recursing through array elements.
///
/// A struct reference is emitted under the owner-qualified name the earlier flat naming rule
/// resolved during lowering, never a name this file re-derives.
fn type_name(ty: &FieldType, field: &Field, message: &Message) -> Result<String, GenerationError> {
    match ty {
        FieldType::String => Ok("StrBytes".to_owned()),
        FieldType::Bool => Ok("bool".to_owned()),
        FieldType::Int8 => Ok("i8".to_owned()),
        FieldType::Int16 => Ok("i16".to_owned()),
        FieldType::Uint16 => Ok("u16".to_owned()),
        FieldType::Uint32 => Ok("u32".to_owned()),
        FieldType::Int32 => Ok("i32".to_owned()),
        FieldType::Int64 => Ok("i64".to_owned()),
        FieldType::Uuid => Ok("Uuid".to_owned()),
        FieldType::Bytes => Ok("Bytes".to_owned()),
        FieldType::Struct(reference) => Ok(reference.rust_type().to_owned()),
        FieldType::Array(element) => Ok(format!("Vec<{}>", type_name(element, field, message)?)),
        other => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            format!("field type {other:?} has no Rust type in this backend"),
        )),
    }
}

pub(crate) fn default_expression(
    field: &Field,
    message: &Message,
) -> Result<String, GenerationError> {
    if matches!(field.default, DefaultValue::Null) {
        return Ok("None".to_owned());
    }
    let value = default_value(field, message)?;
    if is_nullable(field, message) {
        // A nullable field declaring a real default is `Option<T>` holding that
        // value, not `None`: upstream writes both, and collapsing them would
        // encode an absent field where the protocol says a present one.
        return Ok(format!("Some({value})"));
    }
    Ok(value)
}

/// The default as the underlying type spells it, before nullability wraps it.
fn default_value(field: &Field, message: &Message) -> Result<String, GenerationError> {
    match &field.default {
        DefaultValue::Null => Ok("None".to_owned()),
        DefaultValue::Bool(value) => Ok(value.to_string()),
        DefaultValue::Integer(value) => Ok(separated(*value)),
        DefaultValue::String(value) if value.is_empty() => Ok("StrBytes::default()".to_owned()),
        DefaultValue::String(value) => Ok(format!("StrBytes::from({value:?})")),
        DefaultValue::Uuid(bytes) if *bytes == [0_u8; 16] => Ok("Uuid::ZERO".to_owned()),
        DefaultValue::Uuid(bytes) => Ok(format!("Uuid::from_bytes({bytes:?})")),
        // A non-nullable struct field is absent from a version as every member
        // at its own default, which is what the generated struct derives.
        DefaultValue::StructDefaults => Ok(format!(
            "{}::default()",
            type_name(&field.ty, field, message)?
        )),
        // Named by type rather than inferred: `Default::default()` in an
        // initializer position is correct but says less than the type does.
        DefaultValue::Empty => match &field.ty {
            FieldType::Array(_) => Ok("Vec::new()".to_owned()),
            FieldType::Bytes => Ok("Bytes::default()".to_owned()),
            FieldType::String => Ok("StrBytes::default()".to_owned()),
            _ => Ok("Default::default()".to_owned()),
        },
        // `validate_supported` restricts the slice to string, int16, int32, and
        // legacy string arrays, so no field carrying one of these defaults
        // should reach the renderer. Should is not a guarantee, so widening the
        // slice without widening this match fails generation instead of
        // emitting an initializer that is not the protocol default.
        other @ DefaultValue::Float(_) => Err(GenerationError::unsupported(
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
    if !matches!(field.default, DefaultValue::Null) && is_nullable(field, message) {
        let value = default_value(field, message)?;
        return Ok(format!("self.{name} != Some({value})"));
    }
    match &field.default {
        DefaultValue::Null => Ok(format!("self.{name}.is_some()")),
        // `self.x != false` and `self.x != true` are a negation and an
        // identity; the lints on checked-in output reject both spellings.
        DefaultValue::Bool(false) => Ok(format!("self.{name}")),
        DefaultValue::Bool(true) => Ok(format!("!self.{name}")),
        DefaultValue::Integer(value) => Ok(format!("self.{name} != {}", separated(*value))),
        DefaultValue::String(value) if value.is_empty() => Ok(format!("!self.{name}.is_empty()")),
        DefaultValue::String(value) => Ok(format!("self.{name}.as_str() != {value:?}")),
        DefaultValue::Uuid(bytes) if *bytes == [0_u8; 16] => {
            Ok(format!("self.{name} != Uuid::ZERO"))
        }
        DefaultValue::Uuid(bytes) => Ok(format!("self.{name} != Uuid::from_bytes({bytes:?})")),
        DefaultValue::StructDefaults => Ok(format!(
            "self.{name} != {}::default()",
            type_name(&field.ty, field, message)?
        )),
        DefaultValue::Empty => Ok(format!("!self.{name}.is_empty()")),
        other @ DefaultValue::Float(_) => Err(GenerationError::unsupported(
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
            FieldType::Int8
                | FieldType::Int16
                | FieldType::Uint16
                | FieldType::Int32
                | FieldType::Uint32
                | FieldType::Int64,
            DefaultValue::Integer(0)
        )
    ) || matches!(
        (&field.ty, &field.default),
        (FieldType::Bool, DefaultValue::Bool(false))
    ) || matches!(
        (&field.ty, &field.default),
        (FieldType::Uuid, DefaultValue::Uuid(bytes)) if *bytes == [0_u8; 16]
    ) || matches!(
        (&field.ty, &field.default),
        (
            FieldType::Array(_) | FieldType::Bytes | FieldType::String,
            DefaultValue::Empty
        )
    ) || matches!(
        (&field.ty, &field.default),
        (FieldType::Struct(_), DefaultValue::StructDefaults)
    ) || matches!(&field.default, DefaultValue::Null)
}

/// Whether any field this message emits carries the named wire type, directly,
/// as an array element, or inside a struct it declares.
///
/// The file import list pulls a type in only when it is used, so a message
/// built from integers and strings alone does not name one it never writes.
pub(crate) fn uses_type(message: &Message, wanted: &FieldType) -> bool {
    fields_use_type(&message.fields, wanted)
        || message
            .common_structs
            .iter()
            .any(|common| fields_use_type(&common.fields, wanted))
}

fn fields_use_type(fields: &[Field], wanted: &FieldType) -> bool {
    fields
        .iter()
        .any(|field| ty_uses_type(&field.ty, wanted) || fields_use_type(&field.fields, wanted))
}

fn ty_uses_type(ty: &FieldType, wanted: &FieldType) -> bool {
    match ty {
        FieldType::Array(element) => ty_uses_type(element, wanted),
        other => std::mem::discriminant(other) == std::mem::discriminant(wanted),
    }
}
