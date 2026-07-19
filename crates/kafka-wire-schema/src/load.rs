//! Narrow front-end orchestration from one file to one validated message.

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{LowerError, Message, SourceError, SourceFile, ValidationErrors};

/// Front-end failure grouped by compiler phase.
#[derive(Debug, Error)]
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
    let path = path.as_ref().to_path_buf();
    let source = SourceFile::read(&path).map_err(|source| SourceError::Read {
        path: path.clone(),
        source,
    })?;
    let raw = crate::parse_jsonc(&source)?;
    let message = crate::lower_message(raw, PathBuf::from(source.path()))?;
    crate::validate_message(&message)?;
    Ok(message)
}
