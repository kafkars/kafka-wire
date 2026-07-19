//! Field annotations that describe a value rather than shape the wire.
//!
//! This file owns the check that `entityType` and `zeroCopy` are attached to
//! types that can carry them. It deliberately does not own their meaning:
//! what a `topicName` implies for routing, or whether a decoder actually
//! borrows a zero-copy buffer, are decisions above the schema front end.

use crate::{Field, FieldType, Message};

use super::{ValidationError, error::diagnostic};

pub(super) fn validate_annotations(
    message: &Message,
    field: &Field,
    errors: &mut Vec<ValidationError>,
) {
    // Both annotations describe the element, not the container: `[]string` with
    // `entityType: topicName` is a list of topic names, not a topic-named list.
    let element = match &field.ty {
        FieldType::Array(element) => element.as_ref(),
        ty => ty,
    };

    if field.zero_copy && !matches!(element, FieldType::Bytes | FieldType::Records) {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_ZERO_COPY_TYPE",
            "zeroCopy promises a decoder may alias the wire buffer, \
             which only means anything for bytes and records",
        ));
    }
    if field.entity_type.is_some() && matches!(element, FieldType::Struct(_)) {
        errors.push(diagnostic(
            message,
            Some(field),
            "KAFKA_SCHEMA_ENTITY_TYPE_ON_STRUCT",
            "entityType names what a scalar value refers to, not a struct shape",
        ));
    }
}
