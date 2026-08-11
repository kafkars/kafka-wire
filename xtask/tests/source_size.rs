//! Role-specific file budgets keep reading paths bounded and facades tiny.
//!
//! Scenario: classify every Rust file, then judge its length against the budget
//! for its role. All three tiers are executable: a file above target is frozen
//! at a recorded `[[budgets.baseline]]` size, a file above its hard limit needs
//! a tracked `[[budgets.allow]]` exception, and either recording fails once it
//! stops describing the tree.
//!
//! Target and soft findings were previously printed with `eprintln!` from a
//! passing test, which cargo swallows, so neither tier could fail a build. A
//! ratchet that cannot fail is not executable architecture.

#![allow(clippy::unwrap_used)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use support::{
    BudgetBaseline, Budgets, FileClass, Limits, SizeAllow, classify, display_path, fixture_files,
    load_policy, read, rust_files, workspace_root,
};

/// Size findings for one tree, including recordings that have gone stale.
fn size_violations(
    root: &Path,
    files: &[PathBuf],
    budgets: &Budgets,
    generated_roots: &[String],
) -> Vec<String> {
    let baseline = budgets
        .baseline
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let allow = budgets
        .allow
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut reached = BTreeSet::new();
    let mut violations = Vec::new();

    for path in files {
        let relative = display_path(root, path);
        let lines = read(path).lines().count();
        let class = classify(root, path, generated_roots);
        let limits = limits_for(class, budgets);
        reached.insert(relative.clone());

        if lines > limits.hard {
            violations.extend(judge_hard_limit(
                &relative,
                lines,
                class,
                limits,
                allow.get(relative.as_str()),
            ));
            continue;
        }

        if lines > limits.target {
            violations.extend(judge_baseline(
                &relative,
                lines,
                class,
                limits,
                baseline.get(relative.as_str()),
            ));
            continue;
        }

        if allow.contains_key(relative.as_str()) {
            violations.push(format!(
                "{relative}: hard-limit exception is no longer necessary; \
                 remove the `[[budgets.allow]]` entry"
            ));
        }
        if baseline.contains_key(relative.as_str()) {
            violations.push(format!(
                "{relative}: file is back within its {class:?} target of {}; \
                 remove the `[[budgets.baseline]]` entry",
                limits.target
            ));
        }
    }

    for path in baseline.keys().chain(allow.keys()) {
        if !reached.contains(*path) {
            violations.push(format!(
                "{path}: recorded in architecture.toml but the walk never reached it; \
                 fix the path or remove the stale entry"
            ));
        }
    }

    violations
}

fn judge_hard_limit(
    relative: &str,
    lines: usize,
    class: FileClass,
    limits: Limits,
    exception: Option<&&SizeAllow>,
) -> Option<String> {
    match exception {
        Some(entry) if is_tracked(entry) => None,
        Some(_) => Some(format!(
            "{relative}: hard-limit exception must name a reason, an owner, and a tracking issue"
        )),
        None => Some(format!(
            "{relative}:{lines} exceeds the {class:?} hard limit of {}; \
             split the file or add a narrow tracked `[[budgets.allow]]` entry",
            limits.hard
        )),
    }
}

fn judge_baseline(
    relative: &str,
    lines: usize,
    class: FileClass,
    limits: Limits,
    recorded: Option<&&BudgetBaseline>,
) -> Option<String> {
    let (tier, threshold) = if lines > limits.soft {
        ("soft limit", limits.soft)
    } else {
        ("target", limits.target)
    };

    match recorded {
        Some(entry) if entry.reason.trim().is_empty() => Some(format!(
            "{relative}: `[[budgets.baseline]]` entry must explain why the file stays \
             above its {class:?} {tier} of {threshold}"
        )),
        Some(entry) if lines > entry.lines => Some(format!(
            "{relative}:{lines} grew past its recorded baseline of {}; \
             shrink the file, or raise the baseline deliberately with a reason",
            entry.lines
        )),
        Some(_) => None,
        None => Some(format!(
            "{relative}:{lines} exceeds the {class:?} {tier} of {threshold}; \
             split the file or record it in `[[budgets.baseline]]` with a reason"
        )),
    }
}

fn is_tracked(entry: &SizeAllow) -> bool {
    !entry.reason.trim().is_empty()
        && !entry.owner.trim().is_empty()
        && !entry.issue.trim().is_empty()
}

fn limits_for(class: FileClass, budgets: &Budgets) -> Limits {
    match class {
        FileClass::Facade => budgets.facade,
        FileClass::Implementation => budgets.implementation,
        FileClass::Generated => budgets.generated,
        FileClass::Test => budgets.test,
        FileClass::Auxiliary => budgets.auxiliary,
    }
}

#[test]
fn rust_files_respect_role_specific_size_ratchets() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = size_violations(
        &workspace,
        &rust_files(&workspace, &config),
        &config.budgets,
        &config.paths.generated_roots,
    );

    assert!(
        violations.is_empty(),
        "file-size test violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn the_hard_limit_rejects_oversize_and_untracked_exceptions_alike() {
    let (root, files) = fixture_files("oversized_file");
    let violations = size_violations(&root, &files, &uniform(10, 15, 20), &[]);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("oversized.rs") && violation.contains("hard limit")),
        "the file-size detector accepted a file past its hard limit: {violations:?}"
    );

    let mut budgets = uniform(10, 15, 20);
    budgets.allow = vec![SizeAllow {
        path: "src/oversized.rs".to_owned(),
        reason: "still being split".to_owned(),
        owner: String::new(),
        issue: String::new(),
    }];
    let untracked = size_violations(&root, &files, &budgets, &[]);
    assert!(
        untracked
            .iter()
            .any(|violation| violation.contains("a reason, an owner, and a tracking issue")),
        "the file-size detector accepted an untracked hard-limit exception: {untracked:?}"
    );
}

#[test]
fn an_unrecorded_above_target_file_is_rejected() {
    let (root, files) = fixture_files("oversized_file");
    let violations = size_violations(&root, &files, &uniform(10, 15, 1_000), &[]);

    assert!(
        violations.iter().any(|violation| {
            violation.contains("oversized.rs") && violation.contains("[[budgets.baseline]]")
        }),
        "the file-size detector accepted an unrecorded above-target file: {violations:?}"
    );
}

#[test]
fn a_recorded_file_may_hold_its_size_but_not_grow() {
    let (root, files) = fixture_files("oversized_file");
    let recorded = read(&root.join("src/oversized.rs")).lines().count();

    let mut budgets = uniform(10, 15, 1_000);
    budgets.baseline = vec![baseline_entry("src/oversized.rs", recorded)];
    assert!(
        size_violations(&root, &files, &budgets, &[]).is_empty(),
        "the file-size detector rejected a file sitting exactly on its baseline"
    );

    let mut budgets = uniform(10, 15, 1_000);
    budgets.baseline = vec![baseline_entry("src/oversized.rs", recorded - 1)];
    let violations = size_violations(&root, &files, &budgets, &[]);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("grew past its recorded baseline")),
        "the file-size detector accepted growth past a recorded baseline: {violations:?}"
    );
}

#[test]
fn a_recording_that_no_longer_describes_the_tree_is_rejected() {
    let (root, files) = fixture_files("oversized_file");

    let mut budgets = uniform(10_000, 10_000, 10_000);
    budgets.baseline = vec![baseline_entry("src/oversized.rs", 10_000)];
    let stale = size_violations(&root, &files, &budgets, &[]);
    assert!(
        stale
            .iter()
            .any(|violation| violation.contains("remove the `[[budgets.baseline]]` entry")),
        "the file-size detector kept a baseline entry for a file back within target: {stale:?}"
    );

    let mut budgets = uniform(10, 15, 1_000);
    budgets.baseline = vec![baseline_entry("src/departed.rs", 10)];
    let missing = size_violations(&root, &files, &budgets, &[]);
    assert!(
        missing
            .iter()
            .any(|violation| violation.contains("the walk never reached it")),
        "the file-size detector kept a baseline entry for a nonexistent path: {missing:?}"
    );
}

fn uniform(target: usize, soft: usize, hard: usize) -> Budgets {
    let limits = Limits { target, soft, hard };
    Budgets {
        facade: limits,
        implementation: limits,
        generated: limits,
        test: limits,
        auxiliary: limits,
        baseline: Vec::new(),
        allow: Vec::new(),
    }
}

fn baseline_entry(path: &str, lines: usize) -> BudgetBaseline {
    BudgetBaseline {
        path: path.to_owned(),
        lines,
        reason: "fixture recording".to_owned(),
    }
}
