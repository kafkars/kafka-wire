//! Field-level type, version, and default lowering.

use std::path::Path;

use serde_json::Value;

use crate::{DefaultValue, Field, FieldName, FieldType, RawField, VersionSet};

use super::LowerError;

pub(super) fn lower_field(raw: RawField, path: &Path) -> Result<Field, LowerError> {
    if !raw.extra.is_empty() {
        return Err(LowerError::FieldProperties {
            path: path.to_path_buf(),
            field: raw.name,
            properties: raw.extra.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }

    let ty = FieldType::parse(&raw.field_type);
    let versions = parse_versions(path, "presence", &raw.name, &raw.versions)?;
    let nullable_versions = parse_versions(
        path,
        "nullable",
        &raw.name,
        raw.nullable_versions.as_deref().unwrap_or("none"),
    )?;
    let tagged_versions = parse_versions(
        path,
        "tagged",
        &raw.name,
        raw.tagged_versions.as_deref().unwrap_or("none"),
    )?;
    let default = lower_default(path, &raw.name, &ty, raw.default.as_ref())?;
    let fields = raw
        .fields
        .into_iter()
        .map(|field| lower_field(field, path))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Field {
        name: FieldName::new(raw.name),
        ty,
        versions,
        nullable_versions,
        tagged_versions,
        tag: raw.tag,
        default,
        ignorable: raw.ignorable,
        map_key: raw.map_key,
        about: normalize_docs(&raw.about),
        fields,
    })
}

pub(super) fn parse_versions(
    path: &Path,
    role: &'static str,
    owner: &str,
    value: &str,
) -> Result<VersionSet, LowerError> {
    value
        .parse::<VersionSet>()
        .map_err(|error| LowerError::Versions {
            path: path.to_path_buf(),
            role,
            owner: owner.to_owned(),
            value: value.to_owned(),
            reason: error.to_string(),
        })
}

fn lower_default(
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
        (_, Some(Value::String(value))) if value == "null" => Ok(DefaultValue::Null),
        (FieldType::Bool, Some(Value::Bool(value))) => Ok(DefaultValue::Bool(*value)),
        (FieldType::Bool, Some(Value::String(value))) => value
            .parse::<bool>()
            .map(DefaultValue::Bool)
            .map_err(|error| invalid(error.to_string())),
        (FieldType::String, Some(Value::String(value))) => Ok(DefaultValue::String(value.clone())),
        (FieldType::Array(_), Some(Value::Array(values))) if values.is_empty() => {
            Ok(DefaultValue::Empty)
        }
        (FieldType::Bytes | FieldType::Records, Some(Value::String(value))) if value.is_empty() => {
            Ok(DefaultValue::Empty)
        }
        (ty, Some(Value::Number(value))) if is_integer(ty) => value
            .as_i64()
            .map(DefaultValue::Integer)
            .ok_or_else(|| invalid(format!("`{value}` is not a signed integer"))),
        (ty, Some(Value::String(value))) if is_integer(ty) => value
            .parse::<i64>()
            .map(DefaultValue::Integer)
            .map_err(|error| invalid(error.to_string())),
        (FieldType::Bool, None) => Ok(DefaultValue::Bool(false)),
        (FieldType::String, None) => Ok(DefaultValue::String(String::new())),
        (FieldType::Array(_) | FieldType::Bytes | FieldType::Records, None) => {
            Ok(DefaultValue::Empty)
        }
        (ty, None) if is_integer(ty) => Ok(DefaultValue::Integer(0)),
        (_, Some(Value::Null) | None) => Ok(DefaultValue::Null),
        (_, Some(value)) => Err(invalid(format!(
            "value {value} is incompatible with type {ty:?}"
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

fn normalize_docs(source: &str) -> String {
    source.split_whitespace().collect::<Vec<_>>().join(" ")
}
