//! Core crates cannot silently acquire filesystem, process, network, or runtime powers.
//!
//! Scenario: parse every source file below a ruled root, resolve the paths it
//! names into fully-qualified form, and reject any that lies under a forbidden
//! capability or under a capability owned by a different file.
//!
//! What this test sees. It reads Rust syntax, not text, so it sees through
//! nested use groups (`use std::{net::TcpStream, io::Write as _};`), module and
//! item aliases, glob imports, `extern crate`, fully-qualified inline paths with
//! no `use` at all, and paths inside macro arguments. A re-export such as
//! `pub use std::net::TcpStream;` is rejected at the file that writes it, which
//! is what closes laundering: the re-export must itself live under the ruled
//! root to be reachable from it. A file that does not parse is rejected rather
//! than assumed clean.
//!
//! What this test cannot see. It never opens a second file, so it does not
//! follow a name across modules or into a dependency; that boundary is held by
//! the dependency test instead. It does not expand macros, so a capability
//! reached only through a macro defined elsewhere, or assembled from fragments
//! at expansion time, is invisible. It does not follow `include!`. Paths in
//! comments and string literals are correctly ignored, which is a deliberate
//! narrowing: naming a capability in a diagnostic is not using one.

#![allow(clippy::unwrap_used)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use support::{
    CapabilityOwnerRule, CapabilityRule, PathReach, compiled_files, display_path, fixture_files,
    lies_under, load_policy, path_reach, read, rust_files, workspace_root,
};

/// Forbidden capability paths and owned capabilities used outside their owners.
fn capability_violations(
    root: &Path,
    files: &[PathBuf],
    rules: &[CapabilityRule],
    owner_rules: &[CapabilityOwnerRule],
) -> Vec<String> {
    let reach = resolve_covered_files(root, files, rules, owner_rules);
    let mut violations = Vec::new();

    for (path, reach) in &reach {
        if matches!(reach, PathReach::Unparseable) {
            violations.push(format!(
                "{} is not parseable Rust, so its capability reach cannot be proven; \
                 a test that cannot read a file must not vouch for it",
                display_path(root, path)
            ));
        }
    }

    for rule in rules {
        for path in covered_files(root, &rule.root, files) {
            let Some(reach) = reach.get(&path) else {
                continue;
            };
            for capability in &rule.forbidden {
                let Some(reached) = first_reach(reach, capability) else {
                    continue;
                };
                violations.push(format!(
                    "{} reaches `{reached}`, which lies under forbidden capability `{capability}`",
                    display_path(root, &path)
                ));
            }
        }
    }

    for rule in owner_rules {
        let owners = rule
            .allowed
            .iter()
            .map(|relative| root.join(relative))
            .collect::<Vec<_>>();
        for path in covered_files(root, &rule.root, files) {
            if owners.iter().any(|owner| owner == &path) {
                continue;
            }
            let Some(reach) = reach.get(&path) else {
                continue;
            };
            let Some(reached) = first_reach(reach, &rule.token) else {
                continue;
            };
            violations.push(format!(
                "{} reaches `{reached}`; capability `{}` is owned by {}",
                display_path(root, &path),
                rule.token,
                rule.allowed.join(", ")
            ));
        }
    }

    violations
}

/// The files one rule judges: directory members under its root, plus every file
/// the crate actually compiles by following `mod` and `#[path]` from its entry.
///
/// The module tree is what closes a `#[path]` that pulls a non-`.rs` file the
/// directory walk skips, or one living outside the physical prefix the rule is
/// written against. The union never loses the directory walk's coverage — an
/// orphan or unparseable file under the root is still judged.
fn covered_files(root: &Path, rule_root: &str, dir_files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let ruled_root = root.join(rule_root);
    let mut covered = dir_files
        .iter()
        .filter(|path| path.starts_with(&ruled_root))
        .cloned()
        .collect::<BTreeSet<_>>();
    covered.extend(compiled_files(&ruled_root));
    covered
}

/// Parse each file some rule covers exactly once, since parsing is the cost.
fn resolve_covered_files(
    root: &Path,
    files: &[PathBuf],
    rules: &[CapabilityRule],
    owner_rules: &[CapabilityOwnerRule],
) -> BTreeMap<PathBuf, PathReach> {
    let mut covered = BTreeSet::new();
    for rule in rules {
        covered.extend(covered_files(root, &rule.root, files));
    }
    for rule in owner_rules {
        covered.extend(covered_files(root, &rule.root, files));
    }

    covered
        .into_iter()
        .map(|path| {
            let reach = path_reach(&read(&path));
            (path, reach)
        })
        .collect()
}

fn first_reach<'a>(reach: &'a PathReach, capability: &str) -> Option<&'a String> {
    reach
        .named()
        .iter()
        .find(|named| lies_under(named, capability))
}

/// Every fixture module whose capability must be resolved despite its disguise.
const EVASIONS: [&str; 8] = [
    "connects.rs",
    "nested_group.rs",
    "module_alias.rs",
    "renamed_import.rs",
    "inline_path.rs",
    "glob_import.rs",
    "reexport_origin.rs",
    "macro_body.rs",
];

fn network_violations() -> Vec<String> {
    let (root, files) = fixture_files("forbidden_capability");
    let rules = vec![CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec![
            "std::net".to_owned(),
            "std::os::unix::net".to_owned(),
            "std::os::unix::fs".to_owned(),
            "std::os::wasi".to_owned(),
            "tokio".to_owned(),
        ],
    }];

    capability_violations(&root, &files, &rules, &[])
}

fn mentions(violations: &[String], file: &str) -> bool {
    violations.iter().any(|violation| violation.contains(file))
}

#[test]
fn crate_capabilities_stay_with_documented_owners() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = capability_violations(
        &workspace,
        &rust_files(&workspace, &config),
        &config.capability_rules,
        &config.capability_owner_rules,
    );

    assert!(
        violations.is_empty(),
        "capability boundary violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn every_networking_evasion_is_rejected() {
    let violations = network_violations();

    for evasion in EVASIONS {
        assert!(
            mentions(&violations, evasion),
            "the capability detector accepted networking disguised in {evasion}: {violations:?}"
        );
    }
}

#[test]
fn the_exfiltration_probe_is_rejected() {
    let violations = network_violations();

    assert!(
        violations.iter().any(|violation| {
            violation.contains("nested_group.rs") && violation.contains("std::net::TcpStream")
        }),
        "the capability detector accepted the probe that split `std::net` across a use group, \
         naming the resolved path it reached: {violations:?}"
    );
}

#[test]
fn a_capability_hidden_in_a_macro_definition_is_rejected() {
    let violations = network_violations();

    assert!(
        mentions(&violations, "macro_rules_body.rs"),
        "the capability detector accepted networking inside an unparseable `macro_rules!` \
         body instead of falling back to a conservative scan: {violations:?}"
    );
}

#[test]
fn the_unix_socket_exfiltration_probe_is_rejected() {
    let violations = network_violations();

    assert!(
        violations.iter().any(|violation| {
            violation.contains("unix_socket.rs") && violation.contains("std::os::unix::net")
        }),
        "the capability detector let a `std::os::unix::net::UnixStream` exfiltration path through; \
         `std::os::unix::net` sits outside the `std::net` prefix and must be forbidden in its own \
         right: {violations:?}"
    );
}

#[test]
fn a_char_literal_in_a_macro_body_does_not_hide_a_capability() {
    let violations = network_violations();

    assert!(
        mentions(&violations, "macro_char_literal.rs"),
        "the capability detector was desynced by a `'\"'` char literal in a macro body, \
         blanking the real path that followed it: {violations:?}"
    );
}

#[test]
fn a_parent_glob_ties_a_bare_child_path_to_its_capability() {
    let violations = network_violations();

    assert!(
        violations.iter().any(|violation| {
            violation.contains("parent_glob.rs") && violation.contains("std::net")
        }),
        "the capability detector left `net::TcpStream` bare under `use std::*;`, failing to \
         resolve the child segment against the recorded parent glob: {violations:?}"
    );
}

/// A `#[path]` module escapes both a `.rs`-only walk and a physical-prefix rule.
///
/// `payload.inc` compiles under a non-`.rs` extension the directory walk never
/// visits; `../hidden/payload.rs` compiles from outside the crate's `src`
/// prefix the rule is written against. Binding the rule to the module tree the
/// crate actually compiles, rather than a directory glob, catches both.
#[test]
fn a_module_pulled_in_by_a_path_attribute_is_judged() {
    let (root, files) = fixture_files("path_module_evasion");
    let rules = vec![CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec!["std::net".to_owned()],
    }];
    let violations = capability_violations(&root, &files, &rules, &[]);

    assert!(
        mentions(&violations, "payload.inc"),
        "a `#[path]` module with a non-`.rs` extension escaped the capability test: {violations:?}"
    );
    assert!(
        mentions(&violations, "payload.rs"),
        "a `#[path]` module outside the crate's src prefix escaped the capability test: \
         {violations:?}"
    );
}

#[test]
fn an_unparseable_file_is_rejected_rather_than_assumed_clean() {
    let violations = network_violations();

    assert!(
        violations.iter().any(|violation| {
            violation.contains("unparseable.rs") && violation.contains("not parseable Rust")
        }),
        "the capability detector vouched for a file it could not read: {violations:?}"
    );
}

#[test]
fn a_file_that_only_names_a_capability_is_accepted() {
    let violations = network_violations();

    assert!(
        !mentions(&violations, "pure.rs"),
        "the capability detector rejected a file with no capability at all: {violations:?}"
    );
    assert!(
        !mentions(&violations, "mentions_only.rs"),
        "the capability detector rejected a file that names `std::net` only in prose; \
         parsing must tell a sentence from a socket: {violations:?}"
    );
}

/// The consumer half of a laundered re-export is out of a single file's reach.
///
/// This asserts the documented boundary rather than a capability. Resolving it
/// would require reading `reexport_origin.rs` while judging this file, and that
/// file is already rejected, so the laundering path stays closed.
#[test]
fn a_laundered_re_export_is_rejected_at_its_origin_only() {
    let violations = network_violations();

    assert!(
        mentions(&violations, "reexport_origin.rs"),
        "the capability detector accepted `pub use std::net::TcpStream;`: {violations:?}"
    );
    assert!(
        !mentions(&violations, "reexport_consumer.rs"),
        "the capability detector claimed cross-module resolution it does not perform; \
         if this now passes, tighten the documented boundary instead: {violations:?}"
    );
}

#[test]
fn an_owned_capability_used_outside_its_owner_is_rejected() {
    let (root, files) = fixture_files("forbidden_capability");
    let owner_rules = vec![CapabilityOwnerRule {
        root: "src".to_owned(),
        token: "std::fs".to_owned(),
        allowed: vec!["src/owner.rs".to_owned()],
    }];
    let violations = capability_violations(&root, &files, &[], &owner_rules);

    assert!(
        mentions(&violations, "trespasser.rs"),
        "the capability detector accepted an owned capability used outside its owner: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.starts_with("src/owner.rs")),
        "the capability detector rejected the declared capability owner: {violations:?}"
    );
}
