//! Production reading paths exclude embedded tests and unfinished escape hatches.
//!
//! Scenario: read every production file in a tree and reject placeholder macros
//! and inline `#[cfg(test)] mod { .. }` bodies. The live workspace must be
//! clean, and a fixture carrying both must be rejected for both reasons.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};

use support::{display_path, fixture_files, load_policy, read, rust_files, workspace_root};

/// Placeholder macros and embedded test bodies in production reading paths.
fn source_hygiene_violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();

    for path in files {
        let relative = display_path(root, path);
        if is_test_path(&relative) {
            continue;
        }

        let source = read(path);
        for forbidden in ["todo!", "unimplemented!", "dbg!"] {
            if source.contains(forbidden) {
                violations.push(format!("{relative} contains forbidden `{forbidden}`"));
            }
        }
        if embeds_test_module(&source) {
            violations.push(format!(
                "{relative} embeds a test body; move it to a sibling `*_test.rs` file \
                 and declare it with `#[cfg(test)] mod ..;`"
            ));
        }
    }

    violations
}

#[test]
fn production_sources_have_no_inline_test_bodies_or_placeholder_macros() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = source_hygiene_violations(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "source hygiene violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn an_inline_test_body_and_placeholder_macro_are_rejected() {
    let (root, files) = fixture_files("inline_test_body");
    let violations = source_hygiene_violations(&root, &files);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("embeds a test body")),
        "the source-hygiene detector accepted an inline `#[cfg(test)] mod` body: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("todo!")),
        "the source-hygiene detector accepted a `todo!` placeholder: {violations:?}"
    );
}

fn is_test_path(relative: &str) -> bool {
    relative.contains("/tests/")
        || relative.ends_with("/tests.rs")
        || relative.ends_with("_test.rs")
}

fn embeds_test_module(source: &str) -> bool {
    let lines = source.lines().collect::<Vec<_>>();
    for (index, line) in lines.iter().enumerate() {
        if line.trim() != "#[cfg(test)]" {
            continue;
        }
        let next = lines
            .iter()
            .skip(index + 1)
            .map(|candidate| candidate.trim())
            .find(|candidate| !candidate.is_empty());
        if next.is_some_and(|candidate| candidate.starts_with("mod ") && candidate.contains('{')) {
            return true;
        }
    }
    false
}
