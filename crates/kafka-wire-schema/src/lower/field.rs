//! Field-level type, version, and metadata lowering.
//!
//! This file owns the walk from a raw field tree to a normalized one, including
//! the bound on how deep that walk may go. It deliberately does not own default
//! interpretation (`default.rs`), the struct-naming rule
//! (`ir/struct_ref.rs`), or any cross-field invariant (`validate/`).
//!
//! The owning message is threaded through the whole walk because a struct
//! spelling at any depth is qualified by its message and by nothing else — not
//! by the field that carries it and not by its ancestors — so the same owner
//! that names a root field's struct also names one five levels down.

use std::path::Path;

use crate::{EntityType, Field, FieldName, FieldType, MessageName, RawField, VersionSet};

use super::{LowerError, default::lower_default};

/// Deepest inline field nesting this adapter will lower.
///
/// The pinned corpus nests five levels. The cap is well above that so it never
/// rejects a real schema, and exists only so a crafted file cannot drive the
/// recursion below into a stack overflow — this walk is the one place where an
/// input file chooses how deep the front end recurses.
const NESTING_LIMIT: usize = 32;

pub(super) fn lower_field(
    raw: RawField,
    owner: &MessageName,
    valid_versions: &VersionSet,
    path: &Path,
) -> Result<Field, LowerError> {
    lower_nested_field(raw, owner, valid_versions, path, 0)
}

fn lower_nested_field(
    raw: RawField,
    owner: &MessageName,
    valid_versions: &VersionSet,
    path: &Path,
    depth: usize,
) -> Result<Field, LowerError> {
    if depth > NESTING_LIMIT {
        return Err(LowerError::NestingDepth {
            path: path.to_path_buf(),
            field: raw.name,
            limit: NESTING_LIMIT,
        });
    }
    if !raw.extra.is_empty() {
        return Err(LowerError::FieldProperties {
            path: path.to_path_buf(),
            field: raw.name,
            properties: raw.extra.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }

    let ty = FieldType::parse(&raw.field_type, owner).map_err(|error| LowerError::FieldType {
        path: path.to_path_buf(),
        field: raw.name.clone(),
        reason: error.to_string(),
    })?;
    let entity_type = lower_entity_type(path, &raw.name, raw.entity_type.as_deref())?;

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
    let flexible_versions = raw
        .flexible_versions
        .as_deref()
        .map(|value| parse_versions(path, "flexible", &raw.name, value))
        .transpose()?;

    // Whether this field's Rust type is `Option`, which is the same window the
    // renderer decides it on: nullable anywhere the message is supported. A
    // records field's default hinges on it, because Kafka defaults every records
    // field to null and only an `Option` can hold that.
    let nullable_in_range = !nullable_versions.intersection(valid_versions).is_empty();
    let default = lower_default(
        path,
        &raw.name,
        &ty,
        raw.default.as_ref(),
        nullable_in_range,
    )?;
    let fields = raw
        .fields
        .into_iter()
        .map(|field| lower_nested_field(field, owner, valid_versions, path, depth + 1))
        .collect::<Result<Vec<_>, _>>()?;

    let name = FieldName::try_new(raw.name).map_err(|error| LowerError::Identifier {
        path: path.to_path_buf(),
        kind: "field",
        name: error.input.clone(),
        reason: error.to_string(),
    })?;

    Ok(Field {
        name,
        ty,
        versions,
        nullable_versions,
        tagged_versions,
        tag: raw.tag,
        default,
        ignorable: raw.ignorable,
        map_key: raw.map_key,
        entity_type,
        zero_copy: raw.zero_copy,
        flexible_versions,
        about: normalize_docs(&raw.about),
        fields,
    })
}

fn lower_entity_type(
    path: &Path,
    field: &str,
    spelling: Option<&str>,
) -> Result<Option<EntityType>, LowerError> {
    spelling
        .map(|spelling| {
            spelling
                .parse::<EntityType>()
                .map_err(|error| LowerError::EntityType {
                    path: path.to_path_buf(),
                    field: field.to_owned(),
                    reason: error.to_string(),
                })
        })
        .transpose()
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

fn normalize_docs(source: &str) -> String {
    source.split_whitespace().collect::<Vec<_>>().join(" ")
}
