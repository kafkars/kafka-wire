//! Stable workspace traversal and file-role classification.
//!
//! This module owns which Rust files a test sees, how each file is classed for
//! size policy, and the corroboration that makes an empty result impossible to
//! mistake for success. It deliberately owns no judgement about whether a file
//! is acceptable; every ratchet decides that for itself.

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::ArchitecturePolicy;

/// Size-policy class for one Rust file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileClass {
    Facade,
    Implementation,
    Generated,
    Test,
    Auxiliary,
}

/// Which tree a traversal walks, and therefore how plausible its result must be.
///
/// A test that silently inspects nothing reports success, so the live
/// workspace walk must clear a floor. Fixtures are deliberately tiny and are
/// exempt from that floor by name rather than by accident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalkScope {
    Workspace,
    Fixture,
}

/// Smallest believable live-workspace result.
///
/// The workspace has held well over one hundred Rust files since the first
/// milestone. A walk returning fewer than this many files is a misconfigured
/// root, not a clean tree.
const WORKSPACE_RUST_FILE_FLOOR: usize = 50;

pub(crate) fn workspace_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map_or_else(|| manifest_dir.to_path_buf(), Path::to_path_buf)
}

pub(crate) fn rust_files(workspace: &Path, config: &ArchitecturePolicy) -> Vec<PathBuf> {
    let excluded = config
        .paths
        .excluded_roots
        .iter()
        .map(|root| workspace.join(root))
        .collect::<Vec<_>>();

    collect_roots(
        workspace,
        &config.paths.rust_roots,
        &excluded,
        WalkScope::Workspace,
    )
}

/// Collect every Rust file below `base`, treating the whole subtree as one root.
pub(crate) fn rust_files_under(base: &Path, scope: WalkScope) -> Vec<PathBuf> {
    collect_roots(base, &[String::new()], &[], scope)
}

fn collect_roots(
    base: &Path,
    roots: &[String],
    excluded: &[PathBuf],
    scope: WalkScope,
) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        let path = base.join(root);
        assert!(
            path.is_dir(),
            "configured Rust root {} does not exist; \
             fix paths.rust_roots in architecture.toml rather than letting tests inspect nothing",
            path.display()
        );
        collect(&path, excluded, &mut files);
    }
    files.sort();
    files.dedup();

    if scope == WalkScope::Workspace {
        assert!(
            files.len() > WORKSPACE_RUST_FILE_FLOOR,
            "workspace walk found only {} Rust file(s) under {roots:?}, \
             which is below the plausibility floor of {WORKSPACE_RUST_FILE_FLOOR}; \
             the roots are misconfigured and every test would pass over an empty set",
            files.len()
        );
    }
    files
}

pub(crate) fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn display_path(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

pub(crate) fn is_facade(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("lib.rs" | "mod.rs")
    )
}

pub(crate) fn classify(workspace: &Path, path: &Path, generated_roots: &[String]) -> FileClass {
    let relative = display_path(workspace, path);
    if generated_roots
        .iter()
        .any(|root| relative.starts_with(root))
    {
        FileClass::Generated
    } else if relative.contains("/tests/")
        || relative.ends_with("/tests.rs")
        || relative.ends_with("_test.rs")
    {
        FileClass::Test
    } else if relative.ends_with("/src/main.rs") || relative.contains("/src/bin/") {
        FileClass::Auxiliary
    } else if is_facade(path) {
        FileClass::Facade
    } else {
        FileClass::Implementation
    }
}

fn collect(root: &Path, excluded: &[PathBuf], files: &mut Vec<PathBuf>) {
    if excluded.iter().any(|skipped| root == skipped) {
        return;
    }

    let entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("read directory {}: {error}", root.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", root.display()))
            .path();
        if path.is_dir() {
            collect(&path, excluded, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}
