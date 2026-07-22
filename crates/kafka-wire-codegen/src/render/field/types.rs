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

use crate::render::api::{ExternalSymbol as S, spell};

/// Renders one exact IEEE-754 payload as a grouped `u64` literal.
fn float_bits(value: kafka_wire_schema::FloatDefault) -> String {
    let bits = value.get().to_bits();
    format!(
        "0x{:04x}_{:04x}_{:04x}_{:04x}_u64",
        bits >> 48,
        (bits >> 32) & 0xffff,
        (bits >> 16) & 0xffff,
        bits & 0xffff
    )
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
    let base = type_name(&field.ty, message);
    if nullable {
        format!("{}<{base}>", spell(message, S::Option))
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
/// A struct reference is emitted under the spelling lowering resolved, never a
/// name this file re-derives. Module-scoped naming keeps upstream's spelling,
/// and the reference is a sibling in the same module, so it needs no path.
///
/// A wire type does need one, in the rare module that declares a struct of the
/// same name; `spell` is what decides, and it is asked here rather than by the
/// caller so no type-position spelling can escape the check.
fn type_name(ty: &FieldType, message: &Message) -> String {
    match ty {
        FieldType::String => spell(message, S::StrBytes),
        FieldType::Bool => "bool".to_owned(),
        FieldType::Int8 => "i8".to_owned(),
        FieldType::Int16 => "i16".to_owned(),
        FieldType::Uint16 => "u16".to_owned(),
        FieldType::Uint32 => "u32".to_owned(),
        FieldType::Int32 => "i32".to_owned(),
        FieldType::Int64 => "i64".to_owned(),
        FieldType::Uuid => spell(message, S::Uuid),
        FieldType::Float64 => "f64".to_owned(),
        // A `records` field is a byte blob on the wire: the length prefix is the
        // same, and what sits inside it is a RecordBatch this crate does not yet
        // parse. Carrying it as the bytes it is keeps the message honest and
        // leaves the batch to a layer above.
        FieldType::Bytes | FieldType::Records => spell(message, S::Bytes),
        FieldType::Struct(reference) => reference.rust_type().to_owned(),
        FieldType::Array(element) => {
            format!(
                "{}<{}>",
                spell(message, S::Vec),
                type_name(element, message)
            )
        }
    }
}

pub(crate) fn default_expression(field: &Field, message: &Message) -> String {
    if matches!(field.default, DefaultValue::Null) {
        return spell(message, S::None);
    }
    let value = default_value(field, message);
    if is_nullable(field, message) {
        // A nullable field declaring a real default is `Option<T>` holding that
        // value, not `None`: upstream writes both, and collapsing them would
        // encode an absent field where the protocol says a present one.
        return format!("{}({value})", spell(message, S::Some));
    }
    value
}

/// The default as the underlying type spells it, before nullability wraps it.
fn default_value(field: &Field, message: &Message) -> String {
    match &field.default {
        DefaultValue::Null => spell(message, S::None),
        DefaultValue::Bool(value) => value.to_string(),
        DefaultValue::Integer(value) => separated(*value),
        DefaultValue::String(value) if value.is_empty() => {
            format!("{}::default()", spell(message, S::StrBytes))
        }
        DefaultValue::String(value) => {
            format!("{}::from({value:?})", spell(message, S::StrBytes))
        }
        DefaultValue::Uuid(bytes) if *bytes == [0_u8; 16] => {
            format!("{}::ZERO", spell(message, S::Uuid))
        }
        DefaultValue::Uuid(bytes) => {
            format!("{}::from_bytes({bytes:?})", spell(message, S::Uuid))
        }
        // A non-nullable struct field is absent from a version as every member
        // at its own default, which is what the generated struct derives.
        DefaultValue::StructDefaults => format!("{}::default()", type_name(&field.ty, message)),
        // Named by type rather than inferred: `Default::default()` in an
        // initializer position is correct but says less than the type does.
        DefaultValue::Empty => match &field.ty {
            FieldType::Array(_) => format!("{}::new()", spell(message, S::Vec)),
            FieldType::Bytes | FieldType::Records => {
                format!("{}::default()", spell(message, S::Bytes))
            }
            FieldType::String => format!("{}::default()", spell(message, S::StrBytes)),
            _ => format!("{}::default()", spell(message, S::Default)),
        },
        DefaultValue::Float(value) => format!("f64::from_bits({})", float_bits(*value)),
    }
}

pub(crate) fn non_default_condition(field: &Field, message: &Message) -> String {
    let name = field.name.rust_field();
    if !matches!(field.default, DefaultValue::Null) && is_nullable(field, message) {
        let value = default_value(field, message);
        if matches!(field.default, DefaultValue::StructDefaults) {
            return format!(
                "!{}::protocol_eq(&self.{name}, &{}({value}))",
                spell(message, S::ProtocolEq),
                spell(message, S::Some),
            );
        }
        return format!("self.{name} != {}({value})", spell(message, S::Some));
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
            format!("self.{name} != {}::ZERO", spell(message, S::Uuid))
        }
        DefaultValue::Uuid(bytes) => format!(
            "self.{name} != {}::from_bytes({bytes:?})",
            spell(message, S::Uuid)
        ),
        DefaultValue::StructDefaults => {
            format!(
                "!{}::is_protocol_default(&self.{name})",
                spell(message, S::ProtocolEq)
            )
        }
        DefaultValue::Empty => format!("!self.{name}.is_empty()"),
        // A float default compares by bits rather than by `==`: the protocol
        // question is whether the value was left alone, and NaN is not equal to
        // itself under the operator the lints would otherwise demand.
        DefaultValue::Float(value) => {
            format!("self.{name}.to_bits() != {}", float_bits(*value))
        }
    }
}

/// Whether this field's protocol default is also Rust's, so `Default` derives.
///
/// A nullable field is asked about its *wrapped* default. `Option`'s Rust
/// default is `None`, so a nullable field whose protocol default is a real
/// value — which `default_expression` renders as `Some(value)` — does not
/// derive, however ordinary that value looks unwrapped. No pinned schema
/// declares one today; the arm exists so the derive cannot silently disagree
/// with the initializer the moment upstream adds one.
pub(crate) fn uses_rust_default(field: &Field, message: &Message) -> bool {
    if is_nullable(field, message) {
        return matches!(field.default, DefaultValue::Null);
    }
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
        // `Records` is spelled beside `Bytes` here for the same reason it is in
        // `default_value` and `uses_bytes`: it renders as `Bytes`, so its empty
        // default is `Bytes::default()` and the struct can derive `Default`.
        (
            FieldType::Array(_) | FieldType::Bytes | FieldType::Records | FieldType::String,
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
/// crate does not yet parse. This mapping stays beside `type_name`, so importing
/// the shared Rust type cannot drift from declaring it.
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
