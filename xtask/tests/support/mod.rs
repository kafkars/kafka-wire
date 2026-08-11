//! Shared repository-inspection mechanics for architecture tests.
//!
//! This module owns the vocabulary every ratchet reuses: policy loading,
//! bounded traversal, file classification, fixture location, and hashing. It
//! deliberately owns no ratchet, so a test's judgement always reads in the
//! file that names it.

#![allow(dead_code, unused_imports)]

mod config;
mod files;
mod fixtures;
mod hash;
mod macro_tokens;
mod module_tree;
mod paths;
mod walk;

pub(crate) use config::{
    ArchitecturePolicy, BudgetBaseline, Budgets, CapabilityOwnerRule, CapabilityRule,
    DependencyRule, Limits, SizeAllow, load_policy, parse_policy,
};
pub(crate) use files::{
    FileClass, WalkScope, classify, display_path, is_facade, read, rust_files, rust_files_under,
    workspace_root,
};
pub(crate) use fixtures::{fixture_files, fixture_root};
pub(crate) use hash::sha256;
pub(crate) use module_tree::compiled_files;
pub(crate) use paths::{PathReach, lies_under, path_reach};
pub(crate) use walk::tracked_files;
