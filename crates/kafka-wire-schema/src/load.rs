//! Narrow front-end orchestration from one file to one validated message.

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{LowerError, Message, SchemaExceptions, SourceError, SourceFile, ValidationErrors};

/// Front-end failure grouped by compiler phase.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaError {
    /// Source loading or JSONC parsing failed.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Raw source could not be lowered.
    #[error(transparent)]
    Lower(#[from] LowerError),
    /// Normalized semantics violated one or more invariants.
    #[error(transparent)]
    Validation(#[from] ValidationErrors),
}

/// Reads, parses, lowers, and validates one Kafka message definition.
pub fn load_message(path: impl AsRef<Path>) -> Result<Message, SchemaError> {
    load_message_with(path, &SchemaExceptions::none())
}

/// Loads one message, accepting the documented upstream defects in `exceptions`.
pub fn load_message_with(
    path: impl AsRef<Path>,
    exceptions: &SchemaExceptions,
) -> Result<Message, SchemaError> {
    let path = path.as_ref().to_path_buf();
    let source = SourceFile::read(&path).map_err(|source| SourceError::Read {
        path: path.clone(),
        source,
    })?;

    let raw = crate::parse_jsonc(&source)?;
    let mut message = crate::lower_message(raw, PathBuf::from(source.path()))?;
    crate::validate_message_with(&message, exceptions)?;
    prune_unreachable_fields(&mut message);

    Ok(message)
}

/// Drops fields that exist in no version the message supports.
///
/// A field whose presence never intersects its parent's is not a field of this
/// message: it can never appear on the wire, and Apache Kafka's own generator
/// emits no serialization for it. `ShareFetchRequest.PartitionMaxBytes` is
/// declared `"versions": "0"` under a message declaring `"validVersions": "1-2"`,
/// left behind when KIP-932 dropped `ShareFetch` v0.
///
/// Safe to do unconditionally *because* it runs after validation, never before.
/// `KAFKA_SCHEMA_UNUSED_FIELD` rejects exactly this shape, so a field still
/// standing here has been reviewed and accepted by name in
/// `spec/overrides/schema_exceptions.toml`. A mistyped version string is caught
/// by the diagnostic rather than silently losing a field from the wire — which
/// is the failure this ordering exists to prevent, and the one no self-round-trip
/// test could ever see.
fn prune_unreachable_fields(message: &mut Message) {
    fn prune(fields: &mut Vec<crate::Field>, parent: &crate::VersionSet) {
        fields.retain(|field| !field.versions.intersection(parent).is_empty());
        for field in fields {
            let present = field.versions.intersection(parent);
            prune(&mut field.fields, &present);
        }
    }

    let valid = message.valid_versions.clone();
    prune(&mut message.fields, &valid);
    for common in &mut message.common_structs {
        prune(&mut common.fields, &valid);
    }
}
