//! Rollback behavior for portable generated-directory replacement.
//!
//! A failed staged rename must restore the prior complete tree rather than
//! leaving the output path missing or partly installed.

#![allow(clippy::unwrap_used)]

use std::{fs, path::Path};

use crate::output::replace_directory;

#[test]
fn a_failed_staged_install_restores_the_prior_tree() {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join("target/output-transaction-rollback");
    if parent.exists() {
        fs::remove_dir_all(&parent)
            .unwrap_or_else(|error| panic!("clear {}: {error}", parent.display()));
    }
    let root = parent.join("generated");
    fs::create_dir_all(&root).unwrap_or_else(|error| panic!("create {}: {error}", root.display()));
    fs::write(root.join("old.rs"), "old").unwrap_or_else(|error| panic!("write old tree: {error}"));

    let missing_staging = parent.join("missing-staging");
    let error = replace_directory(&root, &missing_staging)
        .err()
        .unwrap_or_else(|| panic!("a missing staged tree was installed"));

    assert!(
        error.to_string().contains("rollback: succeeded"),
        "failed replacement did not report a successful rollback: {error}"
    );
    assert_eq!(
        fs::read_to_string(root.join("old.rs")).unwrap(),
        "old",
        "the prior generated tree was not restored"
    );
}
