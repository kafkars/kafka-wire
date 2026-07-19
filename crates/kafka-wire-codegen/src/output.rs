//! Generated-tree comparison, atomic file replacement, and stale cleanup.

use std::{collections::BTreeMap, fs, path::Path};

use walkdir::WalkDir;

use crate::{GenerationError, GenerationMode};

const MANIFEST_FILENAME: &str = "MANIFEST.json";

/// Summary of one generation or verification run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenerationReport {
    /// Files written with new contents.
    pub written: usize,
    /// Files already equal to expected output.
    pub unchanged: usize,
    /// Stale generated files removed.
    pub removed: usize,
}

pub(crate) fn apply_tree(
    root: &Path,
    expected: &BTreeMap<String, String>,
    mode: GenerationMode,
) -> Result<GenerationReport, GenerationError> {
    match mode {
        GenerationMode::Check => check_tree(root, expected),
        GenerationMode::Write => write_tree(root, expected),
    }
}

fn check_tree(
    root: &Path,
    expected: &BTreeMap<String, String>,
) -> Result<GenerationReport, GenerationError> {
    let actual = existing_files(root)?;
    let mut drift = Vec::new();
    let mut report = GenerationReport::default();
    for (path, source) in expected {
        match actual.get(path) {
            None => drift.push(format!("missing {path}")),
            Some(actual_source) if actual_source != source => {
                drift.push(format!("changed {path}"));
            }
            Some(_) => report.unchanged += 1,
        }
    }
    for path in actual.keys() {
        if !expected.contains_key(path) {
            drift.push(format!("unexpected {path}"));
        }
    }
    if drift.is_empty() {
        Ok(report)
    } else {
        Err(GenerationError::Stale {
            details: drift.join("\n"),
        })
    }
}

fn write_tree(
    root: &Path,
    expected: &BTreeMap<String, String>,
) -> Result<GenerationReport, GenerationError> {
    fs::create_dir_all(root).map_err(|error| GenerationError::io(root, error))?;
    let actual = existing_files(root)?;
    let mut report = GenerationReport::default();
    for (relative, source) in expected
        .iter()
        .filter(|(relative, _)| relative.as_str() != MANIFEST_FILENAME)
    {
        write_expected(root, &actual, relative, source, &mut report)?;
    }
    for relative in actual.keys() {
        if expected.contains_key(relative) {
            continue;
        }
        let path = root.join(relative);
        fs::remove_file(&path).map_err(|error| GenerationError::io(&path, error))?;
        report.removed += 1;
    }
    if let Some(source) = expected.get(MANIFEST_FILENAME) {
        write_expected(root, &actual, MANIFEST_FILENAME, source, &mut report)?;
    }
    Ok(report)
}

fn write_expected(
    root: &Path,
    actual: &BTreeMap<String, String>,
    relative: &str,
    source: &str,
    report: &mut GenerationReport,
) -> Result<(), GenerationError> {
    if actual
        .get(relative)
        .is_some_and(|current| current == source)
    {
        report.unchanged += 1;
        return Ok(());
    }
    write_atomic(&root.join(relative), source)?;
    report.written += 1;
    Ok(())
}

fn existing_files(root: &Path) -> Result<BTreeMap<String, String>, GenerationError> {
    if !root.exists() {
        return Ok(BTreeMap::new());
    }
    let mut files = BTreeMap::new();
    for entry in WalkDir::new(root).min_depth(1) {
        let entry = entry.map_err(|error| {
            let path = error
                .path()
                .map_or_else(|| root.to_path_buf(), Path::to_path_buf);
            GenerationError::io(path, std::io::Error::other(error))
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_or_else(|_| path.to_path_buf(), Path::to_path_buf)
            .to_string_lossy()
            .replace('\\', "/");
        let source = fs::read_to_string(path).map_err(|error| GenerationError::io(path, error))?;
        files.insert(relative, source);
    }
    Ok(files)
}

fn write_atomic(path: &Path, source: &str) -> Result<(), GenerationError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| GenerationError::io(parent, error))?;
    let temporary = path.with_extension("generated.tmp");
    fs::write(&temporary, source).map_err(|error| GenerationError::io(&temporary, error))?;
    fs::rename(&temporary, path).map_err(|error| GenerationError::io(path, error))
}
