//! Rust type, default, and value-comparison rendering for one normalized field.

use kafka_wire_schema::{DefaultValue, Field, FieldType, Message};

pub(crate) fn rust_type(field: &Field, message: &Message) -> String {
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
        other => format!("/* unsupported {other:?} */"),
    };
    if nullable {
        format!("Option<{base}>")
    } else {
        base
    }
}

pub(crate) fn default_expression(field: &Field) -> String {
    match &field.default {
        DefaultValue::Null => "None".to_owned(),
        DefaultValue::Bool(value) => value.to_string(),
        DefaultValue::Integer(value) => value.to_string(),
        DefaultValue::String(value) if value.is_empty() => "StrBytes::default()".to_owned(),
        DefaultValue::String(value) => format!("StrBytes::from({value:?})"),
        DefaultValue::Empty => match &field.ty {
            FieldType::Array(_) => "Vec::new()".to_owned(),
            _ => "Default::default()".to_owned(),
        },
    }
}

pub(crate) fn non_default_condition(field: &Field) -> String {
    let name = field.name.rust_field();
    match &field.default {
        DefaultValue::Null => format!("self.{name}.is_some()"),
        DefaultValue::Bool(value) => format!("self.{name} != {value}"),
        DefaultValue::Integer(value) => format!("self.{name} != {value}"),
        DefaultValue::String(value) if value.is_empty() => format!("!self.{name}.is_empty()"),
        DefaultValue::String(value) => format!("self.{name}.as_str() != {value:?}"),
        DefaultValue::Empty => format!("!self.{name}.is_empty()"),
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
