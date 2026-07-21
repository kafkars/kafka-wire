//! Message-level invariants and validation phase orchestration.

use std::collections::BTreeSet;

use crate::{Message, MessageKind};

use super::{
    SchemaExceptions, ValidationErrors,
    error::diagnostic,
    field::{Presence, validate_fields},
    structs::validate_structs,
};

/// Validates one normalized message with every invariant enforced.
pub fn validate_message(message: &Message) -> Result<(), ValidationErrors> {
    validate_message_with(message, &SchemaExceptions::none())
}

/// Validates one normalized message, accepting documented upstream defects.
///
/// Exceptions are subtracted from the collected diagnostics rather than
/// consulted while checking, so every rule still runs against every message and
/// an exception can only ever hide the one finding it names.
pub fn validate_message_with(
    message: &Message,
    exceptions: &SchemaExceptions,
) -> Result<(), ValidationErrors> {
    let mut errors = Vec::new();

    validate_versions(message, &mut errors);
    validate_api_key(message, &mut errors);
    validate_kind_name(message, &mut errors);
    validate_listeners(message, &mut errors);
    validate_structs(message, &mut errors);

    // A common struct's fields are struct members, not root message fields, so
    // they are judged inside the declaration's own version window and `mapKey`
    // is meaningful on them.
    for common in &message.common_structs {
        let effective = common.versions.intersection(&message.valid_versions);
        let scope = Presence::member(&effective);
        validate_fields(message, &common.fields, &scope, &mut errors);
    }
    validate_fields(
        message,
        &message.fields,
        &Presence::root(&message.valid_versions),
        &mut errors,
    );

    errors.retain(|error| !exceptions.accepts(message.name.protocol(), error));

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors(errors))
    }
}

/// Requires `validVersions` to be one interval, or explicitly no versions.
///
/// `"none"` is how upstream retires an API while keeping its schema on record —
/// `ControlledShutdown`, `LeaderAndIsr`, `StopReplica`, and `UpdateMetadata` all
/// sit that way after Apache Kafka 4.0. The interval rule exists to reject a
/// *disjoint* set such as `0-2,5-7`, where "supports first through last" stops
/// being true; an empty set does not make that claim and stays representable.
fn validate_versions(message: &Message, errors: &mut Vec<super::ValidationError>) {
    if message.valid_versions.is_empty() {
        return;
    }

    let Some((first, _)) = message.valid_versions.single_bounded() else {
        errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_VALID_RANGE",
            "validVersions must normalize to one bounded interval",
        ));
        return;
    };
    if first < 0 {
        errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_NEGATIVE_VERSION",
            "message versions must be non-negative",
        ));
    }
}

/// Ties the presence of `apiKey` to what the schema kind is dispatched by.
///
/// A request or response without a key cannot be routed; a header or data
/// schema with one claims an API number it does not own, which would collide
/// with the real message at that key.
fn validate_api_key(message: &Message, errors: &mut Vec<super::ValidationError>) {
    match (message.kind.carries_api_key(), message.api_key) {
        (true, None) => errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_MISSING_API_KEY",
            "requests and responses must declare apiKey",
        )),
        (false, Some(api_key)) => errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_UNEXPECTED_API_KEY",
            &format!("this schema kind is not dispatched, so apiKey {api_key} is meaningless"),
        )),
        (true, Some(api_key)) if api_key < 0 => errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_NEGATIVE_API_KEY",
            "apiKey must be non-negative",
        )),
        _ => {}
    }
}

fn validate_kind_name(message: &Message, errors: &mut Vec<super::ValidationError>) {
    let Some(suffix) = message.kind.name_suffix() else {
        return;
    };
    if !message.name.protocol().ends_with(suffix) {
        errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_DIRECTION_NAME",
            &format!("message name does not end with `{suffix}` as its kind requires"),
        ));
    }
}

/// Listeners scope a request to the sockets that accept it.
///
/// Only requests are accepted on a listener, so the annotation is meaningless
/// on anything else and must not be silently carried forward.
fn validate_listeners(message: &Message, errors: &mut Vec<super::ValidationError>) {
    if !message.listeners.is_empty() && message.kind != MessageKind::Request {
        errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_UNEXPECTED_LISTENERS",
            "listeners scope an accepted request, so only requests may declare them",
        ));
    }

    let mut listeners = BTreeSet::new();
    for listener in &message.listeners {
        if listener.trim().is_empty() {
            errors.push(diagnostic(
                message,
                None,
                "KAFKA_SCHEMA_EMPTY_LISTENER",
                "listener names must not be empty",
            ));
        } else if !listeners.insert(listener) {
            errors.push(diagnostic(
                message,
                None,
                "KAFKA_SCHEMA_DUPLICATE_LISTENER",
                &format!("listener `{listener}` appears more than once"),
            ));
        }
    }
}
