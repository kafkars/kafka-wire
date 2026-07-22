//! Structural proof that every generated known-tag phase matches one typed plan.
//!
//! Each emitter records tag identity and activation versions beside the Rust
//! statement it writes. This module rejects any omitted or divergent phase.

use kafka_wire_schema::{Message, VersionSet};

use crate::{GenerationError, render::tag_plan::KnownTagPlan};

/// One proof fact emitted beside a phase-specific Rust statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RenderedTag {
    pub(super) tag: u32,
    pub(super) active_versions: VersionSet,
}

impl RenderedTag {
    fn from_plan(plan: &KnownTagPlan<'_>) -> Self {
        Self {
            tag: plan.tag(),
            active_versions: plan.active_versions().clone(),
        }
    }
}

/// Ordered proof facts recorded by one generated phase.
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct RenderedKnownTags {
    pub(super) tags: Vec<RenderedTag>,
}

impl RenderedKnownTags {
    /// Records the exact plan at the statement that renders it.
    pub(super) fn record(&mut self, plan: &KnownTagPlan<'_>) {
        self.tags.push(RenderedTag::from_plan(plan));
    }

    /// Returns whether this renderer emitted no tag-specific construct.
    pub(super) fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    pub(super) fn matches(&self, expected: &[RenderedTag]) -> bool {
        self.tags == expected
    }
}

/// Claim and measurement statements emitted by the encoding phase.
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct RenderedTagEncoding {
    claims: RenderedKnownTags,
    measurements: RenderedKnownTags,
}

impl RenderedTagEncoding {
    pub(super) fn record_claim(&mut self, plan: &KnownTagPlan<'_>) {
        self.claims.record(plan);
    }

    pub(super) fn record_measurement(&mut self, plan: &KnownTagPlan<'_>) {
        self.measurements.record(plan);
    }
}

/// Requires the IR plan and every emitted tag phase to be identical.
pub(super) fn verify_known_tag_rendering(
    plans: &[KnownTagPlan<'_>],
    message: &Message,
    owner: &str,
    decoded: &RenderedKnownTags,
    validated: &RenderedKnownTags,
    encoded: &RenderedTagEncoding,
) -> Result<(), GenerationError> {
    let expected = plans.iter().map(RenderedTag::from_plan).collect::<Vec<_>>();
    if decoded.matches(&expected)
        && validated.matches(&expected)
        && encoded.claims.matches(&expected)
        && encoded.measurements.matches(&expected)
    {
        return Ok(());
    }
    Err(GenerationError::InternalInvariant {
        message: message.name.protocol().to_owned(),
        invariant: format!(
            "known-tag rendering diverged for {owner}: IR {expected:?}, decode {:?}, validation \
             {:?}, claims {:?}, measurements {:?}",
            decoded.tags, validated.tags, encoded.claims.tags, encoded.measurements.tags
        ),
    })
}
