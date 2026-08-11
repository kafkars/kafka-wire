//! A test that inspects nothing must fail, never quietly report success.
//!
//! Scenario: every path-based ratchet is only as trustworthy as the walk that
//! feeds it. The traversal previously returned early when a configured root did
//! not exist, so mistyping `crates` as `crate` in architecture.toml made the
//! file-size, facade, module-contract, capability, and source-hygiene tests
//! all pass over zero files and exit clean.
//!
//! Two independent defences are asserted here: a configured root that does not
//! exist is a hard failure, and a live-workspace walk that returns an
//! implausibly small set is treated as a misconfiguration rather than a clean
//! tree.

#![allow(clippy::unwrap_used)]

mod support;

use support::{load_policy, rust_files, workspace_root};

#[test]
fn the_live_workspace_walk_reaches_a_substantial_source_tree() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let files = rust_files(&workspace, &config);

    assert!(
        files.len() > 50,
        "the workspace walk reached only {} Rust files",
        files.len()
    );
    assert!(
        files
            .iter()
            .any(|path| path.ends_with("crates/kafka-wire-core/src/lib.rs")),
        "the workspace walk did not reach the wire kernel facade"
    );
}

#[test]
#[should_panic(expected = "does not exist")]
fn a_configured_root_that_does_not_exist_fails_the_walk() {
    let workspace = workspace_root();
    let mut config = load_policy(&workspace);
    // The exact one-character typo from the audit: `crates` written as `crate`.
    config.paths.rust_roots = vec!["crate".to_owned(), "xtask/src".to_owned()];

    let _ = rust_files(&workspace, &config);
}

#[test]
#[should_panic(expected = "plausibility floor")]
fn a_walk_that_finds_almost_nothing_fails_rather_than_passing() {
    let workspace = workspace_root();
    let mut config = load_policy(&workspace);
    // A root that exists but holds a single file: never a clean workspace.
    config.paths.rust_roots = vec!["xtask/src".to_owned()];

    let _ = rust_files(&workspace, &config);
}

#[test]
fn architecture_fixtures_are_not_mistaken_for_workspace_source() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let files = rust_files(&workspace, &config);

    assert!(
        !files
            .iter()
            .any(|path| path.components().any(|part| part.as_os_str() == "fixtures")),
        "deliberately broken test fixtures leaked into the reviewed workspace walk"
    );
}
