//! Type compatibility and range checks for normalized protocol defaults.

use crate::{DefaultValue, Field, FieldType, Message};

use super::{ValidationError, error::diagnostic};

pub(super) fn validate_default(
    message: &Message,
    field: &Field,
    nullable: &crate::VersionSet,
    errors: &mut Vec<ValidationError>,
) {
    let valid = match (&field.ty, &field.default) {
        (_, DefaultValue::Null) => !nullable.is_empty() && field.ty.permits_null(),
        (FieldType::Bool, DefaultValue::Bool(_))
        | (FieldType::String, DefaultValue::String(_))
        | (FieldType::Array(_) | FieldType::Bytes | FieldType::Records, DefaultValue::Empty) => {
            true
        }
        (ty, DefaultValue::Integer(value)) => integer_fits(ty, *value),
        _ => false,
    };

    if !valid {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_DEFAULT_TYPE",
            "default value is incompatible with the field type or nullability",
        ));
    }
}

fn integer_fits(ty: &FieldType, value: i64) -> bool {
    match ty {
        FieldType::Int8 => i8::try_from(value).is_ok(),
        FieldType::Int16 => i16::try_from(value).is_ok(),
        FieldType::Uint16 => u16::try_from(value).is_ok(),
        FieldType::Int32 => i32::try_from(value).is_ok(),
        FieldType::Uint32 => u32::try_from(value).is_ok(),
        FieldType::Int64 => true,
        _ => false,
    }
}
