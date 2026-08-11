//! Every Rust module states ownership before implementation.
//!
//! Scenario: collect the Rust files of a tree, then assert each one opens with
//! a `//!` contract. The live workspace must be clean, and a fixture module
//! that opens straight into code must be rejected — the rejection is what
//! proves the detector inspects anything at all.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};

use support::{display_path, fixture_files, load_policy, read, rust_files, workspace_root};

/// Files that begin with code instead of an ownership or scenario contract.
fn modules_without_a_contract(root: &Path, files: &[PathBuf]) -> Vec<String> {
    files
        .iter()
        .filter(|path| !read(path).trim_start().starts_with("//!"))
        .map(|path| {
            format!(
                "{} must begin with a `//!` ownership or scenario contract",
                display_path(root, path)
            )
        })
        .collect()
}

#[test]
fn rust_modules_begin_with_a_contract() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = modules_without_a_contract(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "module contract violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_module_that_opens_with_code_is_rejected() {
    let (root, files) = fixture_files("module_without_contract");
    let violations = modules_without_a_contract(&root, &files);

    assert!(
        violations
            .iter()
            .any(|violation| violation.starts_with("src/undocumented.rs")),
        "the module-contract detector accepted a file with no `//!` contract: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.starts_with("src/documented.rs")),
        "the module-contract detector rejected a properly documented file: {violations:?}"
    );
}
