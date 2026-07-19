//! Sibling names, version relations, and nested field shape.
//!
//! This file owns the presence window every per-field invariant is judged
//! against, and the checks that depend directly on it. It deliberately does not
//! own the tagged-field contract (`tag.rs`), annotation placement
//! (`annotation.rs`), default ranges (`default.rs`), or struct resolution
//! (`structs.rs`).

use std::collections::{BTreeMap, BTreeSet};

use crate::{Field, Message, VersionSet};

use super::{
    ValidationError, annotation::validate_annotations, default::validate_default,
    error::diagnostic, tag::validate_tag,
};

/// Deepest field nesting validation will walk.
///
/// Lowering already rejects anything deeper, so reaching this bound means the
/// two limits drifted apart rather than that a schema is legitimately deep.
const NESTING_LIMIT: usize = 32;

/// The version window a field is judged against.
///
/// A nested field exists only where its parent exists, so `versions` for a
/// field two levels down means "of the versions where my parent is present".
/// Judging it against the message instead silently accepts a child declared
/// `0+` under a parent introduced at version 3, and then reports the resulting
/// absence as a message-level fault or not at all.
pub(super) struct Presence<'a> {
    /// Versions in which the enclosing scope exists.
    pub(super) parent: &'a VersionSet,
    /// How deep this scope sits below the message root.
    pub(super) depth: usize,
}

impl<'a> Presence<'a> {
    /// Returns the presence window at the message root.
    pub(super) const fn root(valid_versions: &'a VersionSet) -> Self {
        Self {
            parent: valid_versions,
            depth: 0,
        }
    }

    /// Returns the presence window inside a struct declared at message level.
    ///
    /// A `commonStructs` body is reached only through a field that refers to
    /// it, so its members sit one level down even though the declaration is
    /// written at the top of the file.
    pub(super) const fn member(versions: &'a VersionSet) -> Self {
        Self {
            parent: versions,
            depth: 1,
        }
    }
}

pub(super) fn validate_fields(
    message: &Message,
    fields: &[Field],
    scope: &Presence<'_>,
    errors: &mut Vec<ValidationError>,
) {
    if scope.depth > NESTING_LIMIT {
        return;
    }

    let mut protocol_names = BTreeSet::new();
    let mut rust_names = BTreeSet::new();
    let mut tags = BTreeMap::new();

    for field in fields {
        validate_sibling_names(message, field, &mut protocol_names, &mut rust_names, errors);
        validate_field(message, field, scope, &mut tags, errors);
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
    scope: &Presence<'_>,
    tags: &mut BTreeMap<u32, &'a str>,
    errors: &mut Vec<ValidationError>,
) {
    let present = field.versions.intersection(scope.parent);
    // Absence is only a defect when the parent is reachable at all. Under a
    // retired message or an already-unused parent every descendant is trivially
    // absent, and reporting each one buries the one fault that is real.
    if present.is_empty() && !scope.parent.is_empty() {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_UNUSED_FIELD",
            "field is absent from every version in which its parent exists",
        ));
    }

    let nullable = field.nullable_versions.intersection(scope.parent);
    validate_nullability(message, field, &present, &nullable, errors);
    validate_tag(message, field, &present, tags, errors);
    validate_default(message, field, &nullable, errors);
    validate_nested_shape(message, field, errors);
    validate_annotations(message, field, errors);

    if field.map_key && scope.depth == 0 {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_ROOT_MAP_KEY",
            "mapKey is meaningful only inside a structured array element",
        ));
    }

    if field.declares_struct() {
        let nested = Presence {
            parent: &present,
            depth: scope.depth + 1,
        };
        validate_fields(message, &field.fields, &nested, errors);
    }
}

fn validate_nullability(
    message: &Message,
    field: &Field,
    present: &VersionSet,
    nullable: &VersionSet,
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

fn validate_nested_shape(message: &Message, field: &Field, errors: &mut Vec<ValidationError>) {
    if !field.declares_struct() {
        return;
    }
    if field.ty.struct_reference().is_none() {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_UNEXPECTED_NESTED_FIELDS",
            "inline fields require a struct or array-of-struct type",
        ));
    }
}
