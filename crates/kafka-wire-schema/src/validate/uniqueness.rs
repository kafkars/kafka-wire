//! Uniqueness of the Rust type names emission produces, in each of its scopes.
//!
//! This file owns collision checks for the two emitted name scopes. Getting
//! them backwards is the whole failure mode this file exists to prevent:
//!
//! * **The module.** `kafka-wire-codegen` emits one `pub mod` per message, holding
//!   the message type and every struct that message declares under upstream's
//!   own spelling. Two items of one name there are rustc `E0428`. This scope is
//!   per message and is reset for each one.
//! * **The crate root.** Only message types are re-exported flat, so that
//!   `kafka_wire::ProduceRequest` needs no module path. Two message types of
//!   one name would be an unexportable pair. Nested structs do *not* participate
//!   — that isolation is the reason for the module boundary. Putting nested
//!   names back into the global map would reject `EntryData` in both directions of
//!   `AlterClientQuotas`, which is legal and generated today.
//!
//! Scoping the global map too widely rejects correct schemas; scoping the module
//! map too loosely — per API key, say, rather than per message — passes schemas
//! whose generated Rust then fails to compile, because a request and its
//! response share an API key and eight keys declare a differently-shaped struct
//! of one name in each direction. A guard scoped more loosely than the namespace
//! it protects is a guard that passes while the output fails to compile.
//!
//! It deliberately does not own the naming rule (`ir/struct_ref.rs`) or the
//! shape of any single message's table (`structs.rs`). Those two decide what a
//! name *is*; this decides whether the scopes it lands in can carry it.
//!
//! There is a third scope no measurement of the schemas can find, because it
//! depends on what the emitter imports rather than on what upstream declares: a
//! declared struct can collide with a name its module imports.
//! `ApiVersionsResponse` declares `ApiVersion`. That one belongs to
//! `kafka-wire-codegen`, which owns the import list, and is resolved there by
//! qualifying the wire type at its point of use.

use std::{collections::BTreeMap, path::PathBuf};

use crate::Message;

use super::{ValidationError, ValidationErrors};

/// A prior claim on one Rust type name.
struct Claim {
    path: PathBuf,
    description: String,
}

/// Asserts that no generated type name collides in the scope it is emitted into.
///
/// Message types are checked globally, because they are re-exported flat.
/// Declared structs are checked against their own message's module only, which
/// is where they are emitted and the only place they can clash. The message
/// type participates in its own module too: a struct named exactly like the
/// message that declares it would be a second item of that name inside the
/// `pub mod`, whatever the crate root thinks.
pub fn validate_struct_names(messages: &[Message]) -> Result<(), ValidationErrors> {
    let mut exported: BTreeMap<String, Claim> = BTreeMap::new();
    let mut errors = Vec::new();

    for message in messages {
        let message_type = message.name.rust_type().to_owned();
        let describe_message = format!("message `{}`", message.name.protocol());

        claim(
            &mut exported,
            &mut errors,
            message,
            message_type.clone(),
            describe_message.clone(),
        );

        // Fresh per message: this is the module scope, and a name claimed in one
        // message says nothing about any other.
        let mut module: BTreeMap<String, Claim> = BTreeMap::new();
        claim(
            &mut module,
            &mut errors,
            message,
            message_type,
            describe_message,
        );

        for declaration in message.structs.declarations() {
            claim(
                &mut module,
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
