//! Policy that architecture.toml does not define is rejected, never ignored.
//!
//! Scenario: an exception a contributor believes they wrote is worse than no
//! exception at all. The repository contract documents a hard-limit exception
//! as `[[size_exceptions]]`, while the loader reads `[[budgets.allow]]`; under
//! a permissive parser the documented spelling parsed cleanly and did nothing.
//!
//! Every policy struct denies unknown fields, so any key outside the schema —
//! a wrong table name, a misspelled threshold, a stale field — fails the parse
//! and names the offending key.

#![allow(clippy::unwrap_used)]

mod support;

use support::{load_policy, parse_policy, workspace_root};

/// A complete, valid policy document that each rejection case then corrupts.
const VALID_POLICY: &str = r#"
schema = 1

[paths]
rust_roots = ["crates"]
generated_roots = []
generated_manifest = "MANIFEST.json"

[budgets.facade]
target = 80
soft = 120
hard = 180

[budgets.implementation]
target = 240
soft = 360
hard = 500

[budgets.generated]
target = 350
soft = 600
hard = 900

[budgets.test]
target = 300
soft = 500
hard = 700

[budgets.auxiliary]
target = 300
soft = 500
hard = 700
"#;

fn rejection_for(extra: &str) -> String {
    let source = format!("{VALID_POLICY}\n{extra}");
    match parse_policy(&source) {
        Ok(_) => panic!("policy parsed cleanly despite `{extra}`; unknown keys must be rejected"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn the_repository_policy_document_is_valid() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);

    assert_eq!(config.schema, 1, "unexpected policy schema revision");
    assert!(
        parse_policy(VALID_POLICY).is_ok(),
        "the reference policy document in this test no longer parses"
    );
}

#[test]
fn a_size_exception_written_with_the_wrong_table_name_is_rejected() {
    let error = rejection_for(
        "[[size_exceptions]]\n\
         path = \"crates/kafka-wire-schema/src/ir/version.rs\"\n\
         reason = \"one coherent proof\"\n\
         owner = \"schema\"\n\
         issue = \"#1\"\n",
    );

    assert!(
        error.contains("size_exceptions"),
        "the parser accepted or mis-reported an unknown top-level table: {error}"
    );
}

#[test]
fn a_misspelled_threshold_is_rejected() {
    let error = rejection_for("[budgets.docs]\ntarget = 1\nsoft = 2\nhard = 3\n");

    assert!(
        error.contains("docs"),
        "the parser accepted a budget class that does not exist: {error}"
    );
}

#[test]
fn a_baseline_entry_missing_its_reason_is_rejected() {
    let source =
        format!("{VALID_POLICY}\n[[budgets.baseline]]\npath = \"crates/x.rs\"\nlines = 300\n");

    assert!(
        parse_policy(&source).is_err(),
        "a baseline recording with no reason must not parse"
    );
}

#[test]
fn an_unknown_key_inside_a_known_table_is_rejected() {
    let error =
        rejection_for("[[capability_rules]]\nroot = \"crates\"\nforbidden = []\nexempt = []\n");

    assert!(
        error.contains("exempt"),
        "the parser accepted an unknown key inside a capability rule: {error}"
    );
}
