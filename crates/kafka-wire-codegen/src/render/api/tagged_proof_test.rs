//! Renderer-owned tag metadata compares exact ordered, versioned plans.
//!
//! Scenario: omissions, additions, reordering, and version drift must all be
//! visible to the structural generation proof.

use kafka_wire_schema::VersionSet;

use super::tagged_proof::{RenderedKnownTags, RenderedTag};

fn tag(tag: u32, versions: &str) -> RenderedTag {
    RenderedTag {
        tag,
        active_versions: versions
            .parse::<VersionSet>()
            .unwrap_or_else(|error| panic!("parse versions: {error}")),
    }
}

#[test]
fn rendered_tag_sets_match_only_the_exact_ir_plans() {
    let rendered = RenderedKnownTags {
        tags: vec![tag(0, "1-3"), tag(2, "2-3")],
    };

    assert!(rendered.matches(&[tag(0, "1-3"), tag(2, "2-3")]));
    assert!(!rendered.matches(&[tag(0, "1-3")]));
    assert!(!rendered.matches(&[tag(0, "1-3"), tag(1, "1-3"), tag(2, "2-3")]));
    assert!(!rendered.matches(&[tag(2, "2-3"), tag(0, "1-3")]));
    assert!(!rendered.matches(&[tag(0, "0-3"), tag(2, "2-3")]));
}
