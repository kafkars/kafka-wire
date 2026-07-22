//! Renderer-owned tag metadata compares exact ordered sets.
//!
//! Scenario: omissions, additions, and reordering must all be visible to the
//! structural generation proof.

use super::tagged_proof::RenderedKnownTags;

#[test]
fn rendered_tag_sets_match_only_the_exact_ir_sequence() {
    let mut rendered = RenderedKnownTags::default();
    rendered.record(0);
    rendered.record(2);

    assert!(rendered.matches(&[0, 2]));
    assert!(!rendered.matches(&[0]));
    assert!(!rendered.matches(&[0, 1, 2]));
    assert!(!rendered.matches(&[2, 0]));
}
