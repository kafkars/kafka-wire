//! Sibling names, version relations, tags, defaults, and nested field shape.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Field, FieldType, Message};

use super::{ValidationError, default::validate_default, error::diagnostic};

pub(super) fn validate_fields(
    message: &Message,
    fields: &[Field],
    depth: usize,
    errors: &mut Vec<ValidationError>,
) {
    let mut protocol_names = BTreeSet::new();
    let mut rust_names = BTreeSet::new();
    let mut tags = BTreeMap::new();

    for field in fields {
        validate_sibling_names(message, field, &mut protocol_names, &mut rust_names, errors);
        validate_field(message, field, depth, &mut tags, errors);
    }
}

fn validate_sibling_names<'a>(
    message: &Message,
    field: &'a Field,
    protocol_names: &mut BTreeSet<&'a str>,
    rust_names: &mut BTreeSet<&'a str>,
    errors: &mut Vec<ValidationError>,
) {
    if !protocol_names.insert(field.name.protocol()) {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_DUPLICATE_FIELD",
            "duplicate sibling protocol field name",
        ));
    }
    if !rust_names.insert(field.name.rust_field()) {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_RUST_NAME_COLLISION",
            "multiple sibling fields normalize to the same Rust identifier",
        ));
    }
}

fn validate_field<'a>(
    message: &Message,
    field: &'a Field,
    depth: usize,
    tags: &mut BTreeMap<u32, &'a str>,
    errors: &mut Vec<ValidationError>,
) {
    let present = field.versions.intersection(&message.valid_versions);
    if present.is_empty() {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_UNUSED_FIELD",
            "field is absent from every valid message version",
        ));
    }

    let nullable = field
        .nullable_versions
        .intersection(&message.valid_versions);
    validate_nullability(message, field, &present, &nullable, errors);
    validate_tag(message, field, &present, tags, errors);
    validate_default(message, field, &nullable, errors);
    validate_nested_shape(message, field, errors);

    if field.map_key && depth == 0 {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_ROOT_MAP_KEY",
            "mapKey is meaningful only inside a structured array element",
        ));
    }
    if !field.fields.is_empty() {
        validate_fields(message, &field.fields, depth + 1, errors);
    }
}

fn validate_nullability(
    message: &Message,
    field: &Field,
    present: &crate::VersionSet,
    nullable: &crate::VersionSet,
    errors: &mut Vec<ValidationError>,
) {
    if !nullable.is_subset_of(present) {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_NULLABLE_OUTSIDE_FIELD",
            "nullable versions are not a subset of field-presence versions",
        ));
    }
    if !nullable.is_empty() && !field.ty.permits_null() {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_ILLEGAL_NULLABLE_TYPE",
            "this field type cannot be nullable",
        ));
    }
}

fn validate_tag<'a>(
    message: &Message,
    field: &'a Field,
    present: &crate::VersionSet,
    tags: &mut BTreeMap<u32, &'a str>,
    errors: &mut Vec<ValidationError>,
) {
    let tagged = field.tagged_versions.intersection(&message.valid_versions);
    match (field.tag, tagged.is_empty()) {
        (Some(tag), false) => {
            if let Some(previous) = tags.insert(tag, field.name.protocol()) {
                errors.push(diagnostic(
                    message,
                    Some(field),
                    "KAFKA_SCHEMA_DUPLICATE_TAG",
                    &format!("tag {tag} is already owned by sibling {previous}"),
                ));
            }
            if !tagged.is_subset_of(present) {
                errors.push(diagnostic(
                    message,
                    Some(field),
                    "KAFKA_SCHEMA_TAG_OUTSIDE_FIELD",
                    "tagged versions are not a subset of field-presence versions",
                ));
            }
            if !tagged.is_subset_of(&message.effective_flexible_versions()) {
                errors.push(diagnostic(
                    message,
                    Some(field),
                    "KAFKA_SCHEMA_TAG_OUTSIDE_FLEXIBLE",
                    "tagged versions are not a subset of flexible versions",
                ));
            }
            if !is_one_open_range(&field.tagged_versions) {
                errors.push(diagnostic(
                    message,
                    Some(field),
                    "KAFKA_SCHEMA_TAG_NOT_OPEN_ENDED",
                    "taggedVersions must be one open-ended range so a tag is never reused",
                ));
            }
        }
        (Some(_), true) => errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_UNUSED_TAG",
            "tag is present but taggedVersions is empty",
        )),
        (None, false) => errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_MISSING_TAG",
            "taggedVersions is present but tag is missing",
        )),
        (None, true) => {}
    }
}

fn validate_nested_shape(message: &Message, field: &Field, errors: &mut Vec<ValidationError>) {
    if field.fields.is_empty() {
        return;
    }
    let owns_struct = match &field.ty {
        FieldType::Struct(_) => true,
        FieldType::Array(element) => matches!(element.as_ref(), FieldType::Struct(_)),
        _ => false,
    };
    if !owns_struct {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_UNEXPECTED_NESTED_FIELDS",
            "inline fields require a struct or array-of-struct type",
        ));
    }
}

fn is_one_open_range(versions: &crate::VersionSet) -> bool {
    matches!(versions.ranges(), [range] if range.end().is_none())
}
