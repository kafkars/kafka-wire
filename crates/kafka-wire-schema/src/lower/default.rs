//! Interpretation of the `default` property against a field's type.
//!
//! This file owns the source-language question — what does this JSON literal
//! mean for a field of this type — including the spellings upstream permits for
//! a number. It deliberately does not own whether the resulting value is legal:
//! range and nullability checks belong to `validate/default.rs`.

use std::path::Path;

use serde_json::Value;

use crate::{DefaultValue, FieldType, FloatDefault};

use super::LowerError;

/// Lowers one `default` literal, or the type's implicit default when absent.
pub(super) fn lower_default(
    path: &Path,
    field: &str,
    ty: &FieldType,
    value: Option<&Value>,
) -> Result<DefaultValue, LowerError> {
    let invalid = |reason: String| LowerError::Default {
        path: path.to_path_buf(),
        field: field.to_owned(),
        reason,
    };

    match (ty, value) {
        // Upstream spells the null literal as the string "null" for every
        // nullable type, so this arm has to precede the string default below.
        (_, Some(Value::String(literal))) if literal == "null" => Ok(DefaultValue::Null),

        (FieldType::Bool, Some(Value::Bool(literal))) => Ok(DefaultValue::Bool(*literal)),
        (FieldType::Bool, Some(Value::String(literal))) => literal
            .parse::<bool>()
            .map(DefaultValue::Bool)
            .map_err(|error| invalid(error.to_string())),
        (FieldType::Bool, None) => Ok(DefaultValue::Bool(false)),

        (FieldType::String, Some(Value::String(literal))) => {
            Ok(DefaultValue::String(literal.clone()))
        }
        (FieldType::String, None) => Ok(DefaultValue::String(String::new())),

        (FieldType::Uuid, Some(Value::String(literal))) => parse_uuid(literal)
            .map(DefaultValue::Uuid)
            .ok_or_else(|| invalid(format!("`{literal}` is not a hyphenated UUID"))),
        // Kafka's own generator defaults an unspecified uuid field to the zero
        // UUID rather than to null; a uuid is not a nullable wire type.
        (FieldType::Uuid, None) => Ok(DefaultValue::Uuid([0; 16])),

        (FieldType::Float64, Some(Value::Number(literal))) => literal
            .as_f64()
            .map(|value| DefaultValue::Float(FloatDefault::new(value)))
            .ok_or_else(|| invalid(format!("`{literal}` is not a double"))),
        (FieldType::Float64, Some(Value::String(literal))) => literal
            .parse::<f64>()
            .map(|value| DefaultValue::Float(FloatDefault::new(value)))
            .map_err(|error| invalid(error.to_string())),
        (FieldType::Float64, None) => Ok(DefaultValue::Float(FloatDefault::new(0.0))),

        (FieldType::Array(_), Some(Value::Array(elements))) if elements.is_empty() => {
            Ok(DefaultValue::Empty)
        }
        (FieldType::Bytes | FieldType::Records, Some(Value::String(literal)))
            if literal.is_empty() =>
        {
            Ok(DefaultValue::Empty)
        }
        (FieldType::Array(_) | FieldType::Bytes | FieldType::Records, None) => {
            Ok(DefaultValue::Empty)
        }

        // A struct field that upstream leaves undefaulted defaults to a struct
        // whose members are themselves defaulted, not to null. Only an explicit
        // `"null"` — matched above — makes a struct field absent.
        (FieldType::Struct(_), None) => Ok(DefaultValue::StructDefaults),

        (ty, Some(Value::Number(literal))) if is_integer(ty) => literal
            .as_i64()
            .map(DefaultValue::Integer)
            .ok_or_else(|| invalid(format!("`{literal}` is not a signed integer"))),
        (ty, Some(Value::String(literal))) if is_integer(ty) => parse_integer(literal)
            .map(DefaultValue::Integer)
            .ok_or_else(|| invalid(format!("`{literal}` is not a signed integer"))),
        (ty, None) if is_integer(ty) => Ok(DefaultValue::Integer(0)),

        (_, Some(Value::Null) | None) => Ok(DefaultValue::Null),
        (_, Some(literal)) => Err(invalid(format!(
            "value {literal} is incompatible with type {ty:?}"
        ))),
    }
}

fn is_integer(ty: &FieldType) -> bool {
    matches!(
        ty,
        FieldType::Int8
            | FieldType::Int16
            | FieldType::Uint16
            | FieldType::Int32
            | FieldType::Uint32
            | FieldType::Int64
    )
}

/// Parses the integer spellings upstream uses in `default`.
///
/// Decimal covers almost every case, but the fetch APIs write their sentinel
/// limits in hex (`"0x7fffffff"`), and a decimal-only parser silently rejects
/// them as malformed defaults rather than reading `i32::MAX`.
fn parse_integer(literal: &str) -> Option<i64> {
    let literal = literal.trim();
    let (negative, digits) = match literal.strip_prefix('-') {
        Some(rest) => (true, rest.trim_start()),
        None => (false, literal),
    };

    let magnitude = match digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        Some(hex) => i64::from_str_radix(hex, 16).ok()?,
        None => digits.parse::<i64>().ok()?,
    };

    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}

/// Parses the canonical hyphenated UUID form into sixteen big-endian bytes.
fn parse_uuid(literal: &str) -> Option<[u8; 16]> {
    const GROUPS: [usize; 5] = [8, 4, 4, 4, 12];

    let mut groups = literal.split('-');
    let mut digits = String::with_capacity(32);
    for width in GROUPS {
        let group = groups.next()?;
        if group.len() != width || !group.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        digits.push_str(group);
    }
    if groups.next().is_some() {
        return None;
    }

    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let pair = digits.get(index * 2..index * 2 + 2)?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(bytes)
}
