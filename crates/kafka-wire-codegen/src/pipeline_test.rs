//! Final generated-path claims are unique across fixed and schema outputs.
//!
//! Scenario: reserve the registry path, then let a normalized API claim the
//! same filename and require a diagnostic that names both producers.

#![allow(clippy::expect_used)]

use std::collections::BTreeMap;

use crate::{GenerationError, pipeline::claim_output_path};

#[test]
fn a_generated_path_collision_reports_both_producers() {
    let mut claimed = BTreeMap::new();
    claim_output_path(&mut claimed, "registry.rs", "fixed API registry")
        .unwrap_or_else(|error| panic!("reserve fixed path: {error}"));

    let error = claim_output_path(&mut claimed, "registry.rs", "API key 900 (RegistryRequest)")
        .expect_err("a schema output replaced a fixed generated file");

    assert!(
        matches!(
            error,
            GenerationError::GeneratedPathCollision {
                ref path,
                ref first,
                ref second,
            } if path == "registry.rs"
                && first == "fixed API registry"
                && second == "API key 900 (RegistryRequest)"
        ),
        "collision diagnostic lost one producer: {error:?}"
    );
}
