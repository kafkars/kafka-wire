//! Typed `architecture.toml` loading shared by all architecture tests.
//!
//! This module owns the executable-policy schema and the rejection of any key
//! that schema does not define, so a misspelled rule fails loudly instead of
//! being ignored. It deliberately owns no traversal, no file classification,
//! and no individual ratchet's judgement.

use std::{fs, path::Path};

use serde::Deserialize;

/// Root executable architecture configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchitecturePolicy {
    pub(crate) schema: u32,
    pub(crate) paths: Paths,
    pub(crate) budgets: Budgets,
    #[serde(default)]
    pub(crate) dependency_rules: Vec<DependencyRule>,
    #[serde(default)]
    pub(crate) capability_rules: Vec<CapabilityRule>,
    #[serde(default)]
    pub(crate) capability_owner_rules: Vec<CapabilityOwnerRule>,
}

/// Repository roots and traversal exclusions.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Paths {
    pub(crate) rust_roots: Vec<String>,
    pub(crate) generated_roots: Vec<String>,
    pub(crate) generated_manifest: String,
    /// Subtrees that are test input rather than reviewed workspace source.
    #[serde(default)]
    pub(crate) excluded_roots: Vec<String>,
}

/// File-size thresholds, the above-target ratchet, and hard-limit exceptions.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Budgets {
    pub(crate) facade: Limits,
    pub(crate) implementation: Limits,
    pub(crate) generated: Limits,
    pub(crate) test: Limits,
    pub(crate) auxiliary: Limits,
    /// Files knowingly above target, frozen at the size recorded here.
    #[serde(default)]
    pub(crate) baseline: Vec<BudgetBaseline>,
    /// Files knowingly above the hard limit.
    #[serde(default)]
    pub(crate) allow: Vec<SizeAllow>,
}

/// Target, warning, and rejection thresholds.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Limits {
    pub(crate) target: usize,
    pub(crate) soft: usize,
    pub(crate) hard: usize,
}

/// One recorded above-target file size that may shrink but never grow.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BudgetBaseline {
    pub(crate) path: String,
    pub(crate) lines: usize,
    pub(crate) reason: String,
}

/// Narrow temporary hard-limit exception.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SizeAllow {
    pub(crate) path: String,
    pub(crate) reason: String,
    pub(crate) owner: String,
    pub(crate) issue: String,
}

/// Allowed dependencies for one package.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DependencyRule {
    pub(crate) package: String,
    pub(crate) allowed_internal: Vec<String>,
    /// When present, the package may depend only on these external crates, and
    /// any other third-party crate — a networking or process crate smuggled
    /// into a core crate, say — is rejected. Absent means external
    /// dependencies are not constrained for this package.
    #[serde(default)]
    pub(crate) allowed_external: Option<Vec<String>>,
}

/// Source tokens forbidden below one crate root.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapabilityRule {
    pub(crate) root: String,
    pub(crate) forbidden: Vec<String>,
}

/// Source capability token restricted to named owner files.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapabilityOwnerRule {
    pub(crate) root: String,
    pub(crate) token: String,
    pub(crate) allowed: Vec<String>,
}

/// Policy schema revision this architecture test suite understands.
const SUPPORTED_SCHEMA: u32 = 1;

pub(crate) fn load_policy(workspace: &Path) -> ArchitecturePolicy {
    let path = workspace.join("architecture.toml");
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let config =
        parse_policy(&source).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));

    assert_eq!(
        config.schema,
        SUPPORTED_SCHEMA,
        "{} declares policy schema {}; this test suite understands schema {SUPPORTED_SCHEMA}",
        path.display(),
        config.schema
    );
    config
}

/// Parse policy text without touching the filesystem so the schema itself is testable.
pub(crate) fn parse_policy(source: &str) -> Result<ArchitecturePolicy, toml::de::Error> {
    toml::from_str(source)
}
