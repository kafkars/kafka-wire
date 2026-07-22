//! Structural proof that both generated tag-ownership paths match the IR.
//!
//! Validation and encoding render separately, so this module compares the tag
//! IDs each renderer actually emitted rather than trusting shared source text.

use kafka_wire_schema::{Field, Message};

use crate::{GenerationError, render::api::tagged::known_tags};

/// Tag IDs recorded beside one generated construct.
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct RenderedKnownTags {
    tags: Vec<u32>,
}

impl RenderedKnownTags {
    /// Records one tag at the statement that renders it.
    pub(super) fn record(&mut self, tag: u32) {
        self.tags.push(tag);
    }

    /// Returns whether this renderer emitted no tag-specific construct.
    pub(super) fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    pub(super) fn matches(&self, expected: &[u32]) -> bool {
        self.tags == expected
    }
}

/// Requires IR ownership, validation checks, and runtime claims to be identical.
pub(super) fn verify_known_tag_rendering(
    fields: &[Field],
    message: &Message,
    owner: &str,
    validated: &RenderedKnownTags,
    claimed: &RenderedKnownTags,
) -> Result<(), GenerationError> {
    let expected = known_tags(fields)
        .into_iter()
        .map(|(tag, _, _)| tag)
        .collect::<Vec<_>>();
    if validated.matches(&expected) && claimed.matches(&expected) {
        return Ok(());
    }
    Err(GenerationError::InternalInvariant {
        message: message.name.protocol().to_owned(),
        invariant: format!(
            "known-tag rendering diverged for {owner}: IR {expected:?}, validation {:?}, claims {:?}",
            validated.tags, claimed.tags
        ),
    })
}
