//! Fail-closed extraction of facts proven by backend validation.
//!
//! Renderers use this seam instead of inventing plausible fallback metadata.
//! If phase ordering or validation changes, an incomplete proof becomes an
//! explicit compiler error rather than valid-looking generated Rust.

use kafka_wire_schema::{Message, VersionSet};

use crate::GenerationError;

pub(crate) fn bounded(
    message: &Message,
    versions: &VersionSet,
    concept: &str,
) -> Result<(i16, i16), GenerationError> {
    versions
        .single_bounded()
        .ok_or_else(|| GenerationError::InternalInvariant {
            message: message.name.protocol().to_owned(),
            invariant: format!("{concept} is not one bounded interval"),
        })
}

pub(crate) fn optional_bounded(
    message: &Message,
    versions: &VersionSet,
    concept: &str,
) -> Result<Option<(i16, i16)>, GenerationError> {
    if versions.is_empty() {
        Ok(None)
    } else {
        bounded(message, versions, concept).map(Some)
    }
}
