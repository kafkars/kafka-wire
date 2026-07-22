//! Generated ownership-phase assertions for every known tagged field.
//!
//! These tests run inside the crate so they can isolate generated validation
//! from unrelated version-representability defaults.

#[path = "generated/tag_claims.rs"]
mod generated_tag_claims;

#[path = "generated/tag_boundaries.rs"]
mod generated_tag_boundaries;

#[test]
fn every_generated_known_tag_has_an_active_validation_check() {
    generated_tag_claims::assert_all_active_tag_claims();
}

#[test]
fn every_delayed_known_tag_changes_ownership_at_its_activation_version() {
    generated_tag_boundaries::assert_all_tag_activation_boundaries();
}
