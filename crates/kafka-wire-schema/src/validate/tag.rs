//! The tagged-field contract: tag identity, ownership, and version windows.
//!
//! This file owns the invariants that make a tag number safe to send. It
//! deliberately does not own field presence or nullability, which constrain the
//! field itself rather than its tagged encoding.

use std::collections::BTreeMap;

use crate::{Field, Message, VersionSet};

use super::{ValidationError, error::diagnostic};

pub(super) fn validate_tag<'a>(
    message: &Message,
    field: &'a Field,
    present: &VersionSet,
    tags: &mut BTreeMap<u32, &'a str>,
    errors: &mut Vec<ValidationError>,
) {
    let tagged = field.tagged_versions.intersection(&message.valid_versions);

    match (field.tag, tagged.is_empty()) {
        (Some(tag), false) => validate_tagged_field(message, field, tag, present, tags, errors),
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

fn validate_tagged_field<'a>(
    message: &Message,
    field: &'a Field,
    tag: u32,
    present: &VersionSet,
    tags: &mut BTreeMap<u32, &'a str>,
    errors: &mut Vec<ValidationError>,
) {
    let tagged = field.tagged_versions.intersection(&message.valid_versions);

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

/// A tag number is permanent, so the versions carrying it must never close.
///
/// A closed or split range would let a later version reuse the number for a
/// different field, and a peer that skipped the intervening versions would
/// decode the new field as the old one.
fn is_one_open_range(versions: &VersionSet) -> bool {
    matches!(versions.ranges(), [range] if range.end().is_none())
}
