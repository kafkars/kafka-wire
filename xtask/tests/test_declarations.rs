//! Every sibling `*_test.rs` unit test is actually compiled and actually runs.
//!
//! Scenario: production files may not embed `#[cfg(test)] mod tests { .. }`
//! bodies, so private unit tests live in sibling `*_test.rs` files. Rust does
//! not discover those files on its own. An undeclared `src/**/*_test.rs`
//! compiles to nothing, runs zero assertions, and reports a green build — the
//! precise silent failure the inline-test ban is supposed to prevent.
//!
//! This test closes that hole: every `src/**/*_test.rs` must be declared in
//! its sibling facade as `#[cfg(test)] mod <stem>;`, and the declaration must
//! carry the `cfg(test)` gate so test code never reaches a production build.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};

use support::{display_path, fixture_files, load_policy, read, rust_files, workspace_root};
use syn::{Attribute, Item};

/// Facade filenames that may declare a sibling module.
const FACADE_NAMES: [&str; 3] = ["mod.rs", "lib.rs", "main.rs"];

/// Sibling unit tests that no facade declares, or that are declared ungated.
fn undeclared_unit_tests(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();

    for path in files.iter().filter(|path| is_sibling_unit_test(path)) {
        let relative = display_path(root, path);
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        let Some(facade) = sibling_facade(path) else {
            violations.push(format!(
                "{relative} has no sibling `mod.rs`, `lib.rs`, or `main.rs` that could \
                 declare it, so it compiles to nothing and runs zero tests"
            ));
            continue;
        };

        let facade_relative = display_path(root, &facade);
        match declaration_of(&read(&facade), stem) {
            Declaration::GatedForTest => {}
            Declaration::Ungated => violations.push(format!(
                "{facade_relative} declares `mod {stem};` without `#[cfg(test)]`; \
                 gate it so unit tests never reach a production build"
            )),
            Declaration::Absent => violations.push(format!(
                "{relative} is never declared, so it compiles to nothing and runs zero \
                 tests; add `#[cfg(test)] mod {stem};` to {facade_relative}"
            )),
        }
    }

    violations
}

/// How a facade declares one sibling module, if it declares it at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Declaration {
    GatedForTest,
    Ungated,
    Absent,
}

fn is_sibling_unit_test(path: &Path) -> bool {
    let is_unit_test = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with("_test.rs"));
    let under_src = path
        .components()
        .any(|component| component.as_os_str() == "src");

    is_unit_test && under_src
}

fn sibling_facade(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    FACADE_NAMES
        .iter()
        .map(|name| parent.join(name))
        .find(|candidate| candidate.is_file())
}

fn declaration_of(facade: &str, stem: &str) -> Declaration {
    let Ok(syntax) = syn::parse_file(facade) else {
        return Declaration::Absent;
    };

    for item in &syntax.items {
        let Item::Mod(module) = item else {
            continue;
        };
        // Only a declaration (`mod name;`) refers to a sibling file; an inline
        // `mod name { .. }` body is a different construct the hygiene test owns.
        if module.ident != stem || module.content.is_some() {
            continue;
        }
        return if module.attrs.iter().any(is_cfg_test) {
            Declaration::GatedForTest
        } else {
            Declaration::Ungated
        };
    }

    Declaration::Absent
}

fn is_cfg_test(attr: &Attribute) -> bool {
    attr.path().is_ident("cfg")
        && attr
            .parse_args::<syn::Path>()
            .is_ok_and(|path| path.is_ident("test"))
}

#[test]
fn every_sibling_unit_test_is_declared() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = undeclared_unit_tests(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "undeclared unit-test violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn an_undeclared_sibling_unit_test_is_rejected() {
    let (root, files) = fixture_files("undeclared_unit_test");
    let violations = undeclared_unit_tests(&root, &files);

    assert!(
        violations.iter().any(|violation| {
            violation.contains("orphan_test.rs") && violation.contains("runs zero")
        }),
        "the declaration detector accepted an undeclared `*_test.rs`: {violations:?}"
    );
}

#[test]
fn an_ungated_declaration_is_rejected() {
    let (root, files) = fixture_files("undeclared_unit_test");
    let violations = undeclared_unit_tests(&root, &files);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("without `#[cfg(test)]`")),
        "the declaration detector accepted `mod ungated_test;` with no cfg gate: {violations:?}"
    );
}

#[test]
fn a_properly_declared_sibling_unit_test_is_accepted() {
    let (root, files) = fixture_files("declared_unit_test");
    let violations = undeclared_unit_tests(&root, &files);

    assert!(
        violations.is_empty(),
        "the declaration detector rejected a correctly declared unit test: {violations:?}"
    );
}
