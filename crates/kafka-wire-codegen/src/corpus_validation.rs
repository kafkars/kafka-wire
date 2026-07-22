//! Cross-schema validation at the loaded-corpus boundary.
//!
//! Individual source loading owns one-message invariants. This module owns the
//! global symbol proof that can run only after every accepted message exists,
//! and deliberately performs no grouping or rendering.

use crate::{GenerationError, source::MessageSource};

/// Validates invariants whose scope is the complete loaded schema corpus.
pub(crate) fn validate_source_corpus(sources: &[MessageSource]) -> Result<(), GenerationError> {
    let messages = sources
        .iter()
        .map(|source| source.message.clone())
        .collect::<Vec<_>>();
    kafka_wire_schema::validate_struct_names(&messages)?;
    Ok(())
}
