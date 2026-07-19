//! Uniqueness of the Rust type names owner qualification produces.
//!
//! This file owns the earlier flat naming rule's fourth clause: after qualification, assert that no
//! two generated types claim one Rust identifier — across every message type
//! and every owner-qualified nested struct in the set being generated.
//!
//! It deliberately does not own the naming rule (`ir/struct_ref.rs`) or the
//! shape of any single message's table (`structs.rs`). Those two decide what a
//! name *is*; this decides whether the corpus as a whole can carry those names,
//! which no single message can answer about itself.
//!
//! This is a check, not an assumption. The invariant that makes message-level
//! qualification sufficient — no message declaring one name with two shapes — is
//! a property of today's pinned corpus, and upstream changes it. A future schema
//! that breaks it must produce a diagnostic naming both declarations, never
//! emitted Rust that fails to compile and never a silently merged type.

use std::{collections::BTreeMap, path::PathBuf};

use crate::Message;

use super::{ValidationError, ValidationErrors};

/// A prior claim on one Rust type name.
struct Claim {
    path: PathBuf,
    description: String,
}

/// Asserts that every generated type name across `messages` is distinct.
///
/// Message types participate alongside nested structs. `kafka-wire-codegen` renders
/// one module per API key holding both directions, and the crate facade
/// re-exports every generated type flat, so a nested struct that qualified to
/// exactly some message's name would collide with it just as surely as with
/// another struct.
///
/// The check is global rather than per module because the flat facade is what
/// consumers import through; a name unique within its module but repeated
/// across two would be unexportable, which is a generation failure discovered
/// one layer too late.
pub fn validate_struct_names(messages: &[Message]) -> Result<(), ValidationErrors> {
    let mut claimed: BTreeMap<String, Claim> = BTreeMap::new();
    let mut errors = Vec::new();

    for message in messages {
        claim(
            &mut claimed,
            &mut errors,
            message,
            message.name.rust_type().to_owned(),
            format!("message `{}`", message.name.protocol()),
        );

        for declaration in message.structs.declarations() {
            claim(
                &mut claimed,
                &mut errors,
                message,
                declaration.name.rust_type().to_owned(),
                format!(
                    "struct `{}` declared {} by `{}`",
                    declaration.name.declared(),
                    declaration.origin.describe(),
                    declaration.name.owner(),
                ),
            );
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors(errors))
    }
}

/// Records one claim, or reports the clash with the claim already holding it.
///
/// The first claimant keeps the name so that a name claimed three times reports
/// two clashes against one stable original, rather than a chain whose text
/// depends on iteration order.
fn claim(
    claimed: &mut BTreeMap<String, Claim>,
    errors: &mut Vec<ValidationError>,
    message: &Message,
    rust_type: String,
    description: String,
) {
    if let Some(previous) = claimed.get(&rust_type) {
        errors.push(ValidationError {
            code: "KAFKA_SCHEMA_QUALIFIED_STRUCT_COLLISION",
            path: message.source.clone(),
            field: None,
            message: format!(
                "generated type `{rust_type}` is claimed by {description}, \
                 and already by {} from {}",
                previous.description,
                previous.path.display(),
            ),
        });
        return;
    }

    claimed.insert(
        rust_type,
        Claim {
            path: message.source.clone(),
            description,
        },
    );
}
