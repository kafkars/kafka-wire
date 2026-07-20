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

/// Renders a double so the emitted literal round-trips as `f64`.
fn float_literal(value: f64) -> String {
    let rendered = format!("{value}");
    // An integral double formats without a point, which would emit an integer
    // literal into an `f64` position. Decided on the rendered text rather than
    // by comparing the double, which the lints reject and which says nothing
    // useful about how it will be written.
    if rendered
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'-')
    {
        return format!("{rendered}.0");
    }
    rendered
}

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

pub(crate) fn rust_type(field: &Field, message: &Message) -> String {
    let nullable = is_nullable(field, message);
    let base = type_name(&field.ty);
    if nullable {
        format!("Option<{base}>")
    } else {
        base
    }
}

/// Maps one type to its Rust spelling, recursing through array elements.
///
/// Total: every normalized type now has a Rust spelling, so this cannot fail.
/// What a type is called and whether the backend can *encode* it are different
/// questions, and the second belongs to `validate`.
///
/// A struct reference is emitted under the owner-qualified name the earlier flat naming rule
/// resolved during lowering, never a name this file re-derives.
fn type_name(ty: &FieldType) -> String {
    match ty {
        FieldType::String => "StrBytes".to_owned(),
        FieldType::Bool => "bool".to_owned(),
        FieldType::Int8 => "i8".to_owned(),
        FieldType::Int16 => "i16".to_owned(),
        FieldType::Uint16 => "u16".to_owned(),
        FieldType::Uint32 => "u32".to_owned(),
        FieldType::Int32 => "i32".to_owned(),
        FieldType::Int64 => "i64".to_owned(),
        FieldType::Uuid => "Uuid".to_owned(),
        FieldType::Float64 => "f64".to_owned(),
        // A `records` field is a byte blob on the wire: the length prefix is the
        // same, and what sits inside it is a RecordBatch this crate does not yet
        // parse. Carrying it as the bytes it is keeps the message honest and
        // leaves the batch to a layer above.
        FieldType::Bytes | FieldType::Records => "Bytes".to_owned(),
        FieldType::Struct(reference) => reference.rust_type().to_owned(),
        FieldType::Array(element) => format!("Vec<{}>", type_name(element)),
    }
}

pub(crate) fn default_expression(field: &Field, message: &Message) -> String {
    if matches!(field.default, DefaultValue::Null) {
        return "None".to_owned();
    }
    let value = default_value(field);
    if is_nullable(field, message) {
        // A nullable field declaring a real default is `Option<T>` holding that
        // value, not `None`: upstream writes both, and collapsing them would
        // encode an absent field where the protocol says a present one.
        return format!("Some({value})");
    }
    value
}

/// The default as the underlying type spells it, before nullability wraps it.
fn default_value(field: &Field) -> String {
    match &field.default {
        DefaultValue::Null => "None".to_owned(),
        DefaultValue::Bool(value) => value.to_string(),
        DefaultValue::Integer(value) => separated(*value),
        DefaultValue::String(value) if value.is_empty() => "StrBytes::default()".to_owned(),
        DefaultValue::String(value) => format!("StrBytes::from({value:?})"),
        DefaultValue::Uuid(bytes) if *bytes == [0_u8; 16] => "Uuid::ZERO".to_owned(),
        DefaultValue::Uuid(bytes) => format!("Uuid::from_bytes({bytes:?})"),
        // A non-nullable struct field is absent from a version as every member
        // at its own default, which is what the generated struct derives.
        DefaultValue::StructDefaults => format!("{}::default()", type_name(&field.ty)),
        // Named by type rather than inferred: `Default::default()` in an
        // initializer position is correct but says less than the type does.
        DefaultValue::Empty => match &field.ty {
            FieldType::Array(_) => "Vec::new()".to_owned(),
            FieldType::Bytes | FieldType::Records => "Bytes::default()".to_owned(),
            FieldType::String => "StrBytes::default()".to_owned(),
            _ => "Default::default()".to_owned(),
        },
        // Rendered so the literal always carries a decimal point and parses
        // back as `f64`; an integral default would otherwise emit as an int.
        DefaultValue::Float(value) => float_literal(value.get()),
    }
}

pub(crate) fn non_default_condition(field: &Field, message: &Message) -> String {
    let name = field.name.rust_field();
    if !matches!(field.default, DefaultValue::Null) && is_nullable(field, message) {
        let value = default_value(field);
        return format!("self.{name} != Some({value})");
    }
    match &field.default {
        DefaultValue::Null => format!("self.{name}.is_some()"),
        // `self.x != false` and `self.x != true` are a negation and an
        // identity; the lints on checked-in output reject both spellings.
        DefaultValue::Bool(false) => format!("self.{name}"),
        DefaultValue::Bool(true) => format!("!self.{name}"),
        DefaultValue::Integer(value) => format!("self.{name} != {}", separated(*value)),
        DefaultValue::String(value) if value.is_empty() => format!("!self.{name}.is_empty()"),
        DefaultValue::String(value) => format!("self.{name}.as_str() != {value:?}"),
        DefaultValue::Uuid(bytes) if *bytes == [0_u8; 16] => {
            format!("self.{name} != Uuid::ZERO")
        }
        DefaultValue::Uuid(bytes) => format!("self.{name} != Uuid::from_bytes({bytes:?})"),
        DefaultValue::StructDefaults => {
            format!("self.{name} != {}::default()", type_name(&field.ty))
        }
        DefaultValue::Empty => format!("!self.{name}.is_empty()"),
        // A float default compares by bits rather than by `==`: the protocol
        // question is whether the value was left alone, and NaN is not equal to
        // itself under the operator the lints would otherwise demand.
        DefaultValue::Float(value) => format!(
            "self.{name}.to_bits() != {}_f64.to_bits()",
            float_literal(value.get())
        ),
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

/// Whether any field this message emits is declared as `kafka_wire_core::Bytes`.
///
/// Two protocol types answer to one Rust type: `bytes` and `records` both become
/// `Bytes`, because a records field is a length-prefixed blob whose contents this
/// crate does not yet parse. An import decided by `uses_type(.., Bytes)` alone
/// therefore leaves a records-only message naming a type it never imported —
/// which is exactly what the compile probe caught. The mapping lives here, next
/// to the `type_name` arm that makes it true, so the two cannot drift apart.
pub(crate) fn uses_bytes(message: &Message) -> bool {
    uses_type(message, &FieldType::Bytes) || uses_type(message, &FieldType::Records)
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
