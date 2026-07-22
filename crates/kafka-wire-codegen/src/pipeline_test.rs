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

#[test]
fn windows_device_stems_are_rejected_before_output_is_staged() {
    for path in ["con.rs", "PRN.rsi", "nested/aux.json", "Com1.rs", "lpt9.rs"] {
        let mut claimed = BTreeMap::new();
        let error = claim_output_path(&mut claimed, path, "adversarial API")
            .expect_err("a reserved Windows device filename reached output staging");
        assert!(
            matches!(
                error,
                GenerationError::NonPortableGeneratedPath {
                    path: ref rejected,
                    ref producer,
                    ..
                } if rejected == path && producer == "adversarial API"
            ),
            "portable-path diagnostic lost its producer: {error:?}"
        );
        assert!(claimed.is_empty(), "the rejected path was still claimed");
    }

    for path in ["connection.rs", "com0.rs", "lpt10.rs"] {
        let mut claimed = BTreeMap::new();
        claim_output_path(&mut claimed, path, "ordinary API")
            .unwrap_or_else(|error| panic!("portable path {path} was rejected: {error}"));
    }
}
