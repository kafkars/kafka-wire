//! Bounded repository traversal that never descends into build output.
//!
//! This module owns which directories a repository-wide search may enter, so a
//! test cannot be slowed to a crawl by `target/` or confused by ignored
//! scratch trees. It deliberately owns no file classification and no policy.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use walkdir::{DirEntry, WalkDir};

/// Directories that are never reviewed source, whatever `.gitignore` says.
const ALWAYS_SKIPPED: [&str; 2] = ["target", ".git"];

/// Every tracked file below `root`, skipping build output and ignored directories.
pub(crate) fn tracked_files(root: &Path) -> Vec<PathBuf> {
    let skipped = skipped_directories(root);
    WalkDir::new(root)
        .into_iter()
        .filter_entry(|entry| !is_skipped_directory(entry, &skipped))
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.path().to_path_buf())
        .collect()
}

fn is_skipped_directory(entry: &DirEntry, skipped: &BTreeSet<String>) -> bool {
    if entry.depth() == 0 || !entry.file_type().is_dir() {
        return false;
    }
    entry
        .file_name()
        .to_str()
        .is_some_and(|name| skipped.contains(name))
}

/// Directory names excluded from traversal: build output plus `.gitignore` entries.
fn skipped_directories(root: &Path) -> BTreeSet<String> {
    let mut skipped = ALWAYS_SKIPPED
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();

    let Ok(source) = fs::read_to_string(root.join(".gitignore")) else {
        return skipped;
    };
    for line in source.lines() {
        let pattern = line.trim();
        if pattern.is_empty() || pattern.starts_with('#') || pattern.starts_with('!') {
            continue;
        }
        let name = pattern.trim_matches('/');
        // Only plain directory names are actionable here; globs and nested
        // patterns are left to Git, which owns the full ignore grammar.
        if !name.is_empty() && !name.contains('/') && !name.contains('*') {
            skipped.insert(name.to_owned());
        }
    }
    skipped
}
