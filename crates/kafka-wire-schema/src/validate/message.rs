//! Message-level invariants and validation phase orchestration.

use std::collections::BTreeSet;

use crate::{Message, MessageKind};

use super::{ValidationErrors, error::diagnostic, field::validate_fields};

/// Validates one normalized message and collects independent diagnostics.
pub fn validate_message(message: &Message) -> Result<(), ValidationErrors> {
    let mut errors = Vec::new();

    validate_versions(message, &mut errors);
    validate_direction_name(message, &mut errors);
    validate_listeners(message, &mut errors);
    validate_fields(message, &message.fields, 0, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors(errors))
    }
}

fn validate_versions(message: &Message, errors: &mut Vec<super::ValidationError>) {
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

fn validate_direction_name(message: &Message, errors: &mut Vec<super::ValidationError>) {
    let suffix_matches = match message.kind {
        MessageKind::Request => message.name.protocol().ends_with("Request"),
        MessageKind::Response => message.name.protocol().ends_with("Response"),
    };
    if !suffix_matches {
        errors.push(diagnostic(
            message,
            None,
            "KAFKA_SCHEMA_DIRECTION_NAME",
            "message name suffix does not match request/response direction",
        ));
    }
}

fn validate_listeners(message: &Message, errors: &mut Vec<super::ValidationError>) {
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
