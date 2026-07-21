//! Generated-tree comparison and recoverable staged directory replacement.
//!
//! It stages a complete sibling before moving the tree and owns no rendering.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

use crate::{GenerationError, GenerationMode, output_staging::StagingDirectory};

/// Summary of one generation or verification run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenerationReport {
    /// Files whose expected contents differ from the prior tree.
    pub written: usize,
    /// Files already equal to expected output.
    pub unchanged: usize,
    /// Stale generated files removed by replacing the prior tree.
    pub removed: usize,
    /// Best-effort cleanup that failed after the new tree was installed.
    ///
    /// These are warnings rather than generation failures: returning `Err`
    /// after commit would leave the caller unable to tell which tree is live.
    pub cleanup_warnings: Vec<String>,
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
    let (report, drift) = compare_trees(&actual, expected);
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
    let actual = existing_files(root)?;
    let (mut report, drift) = compare_trees(&actual, expected);
    if drift.is_empty() {
        return Ok(report);
    }

    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| GenerationError::io(parent, error))?;
    let mut staging = StagingDirectory::create(root)?;
    write_complete_tree(staging.path(), expected)?;

    let staged = existing_files(staging.path())?;
    let (_, staged_drift) = compare_trees(&staged, expected);
    if !staged_drift.is_empty() {
        return Err(GenerationError::StagedTreeMismatch {
            details: staged_drift.join("\n"),
        });
    }

    if let Some(warning) = replace_directory(root, staging.path())? {
        report.cleanup_warnings.push(warning);
    }
    staging.disarm();
    Ok(report)
}

fn write_complete_tree(
    root: &Path,
    expected: &BTreeMap<String, String>,
) -> Result<(), GenerationError> {
    for (relative, source) in expected {
        let path = root.join(relative);
        let parent = path.parent().unwrap_or(root);
        fs::create_dir_all(parent).map_err(|error| GenerationError::io(parent, error))?;
        fs::write(&path, source).map_err(|error| GenerationError::io(&path, error))?;
    }
    Ok(())
}

/// Swaps a complete sibling directory into place without rename-over-existing.
///
/// Moving the old tree aside first is portable across Windows and Unix; a
/// failed staged install attempts restoration and reports the rollback outcome.
pub(crate) fn replace_directory(
    root: &Path,
    staging: &Path,
) -> Result<Option<String>, GenerationError> {
    if !root.exists() {
        fs::rename(staging, root).map_err(|source| GenerationError::TreeSwap {
            root: root.to_path_buf(),
            source,
            rollback: "not needed; no prior tree was moved".to_owned(),
        })?;
        return Ok(None);
    }

    // Exclusive staging makes its derived backup name process-safe.
    let mut backup_name = staging.as_os_str().to_os_string();
    backup_name.push(".backup");
    let backup = PathBuf::from(backup_name);
    fs::rename(root, &backup).map_err(|source| GenerationError::TreeSwap {
        root: root.to_path_buf(),
        source,
        rollback: "not needed; the prior tree was not moved".to_owned(),
    })?;

    if let Err(source) = fs::rename(staging, root) {
        let rollback = match fs::rename(&backup, root) {
            Ok(()) => "succeeded".to_owned(),
            Err(error) => format!("failed: {error}"),
        };
        return Err(GenerationError::TreeSwap {
            root: root.to_path_buf(),
            source,
            rollback,
        });
    }

    Ok(cleanup_backup_after_install(&backup))
}

pub(crate) fn cleanup_backup_after_install(backup: &Path) -> Option<String> {
    fs::remove_dir_all(backup).err().map(|error| {
        format!(
            "generated tree was installed, but obsolete backup {} could not be removed: {error}",
            backup.display()
        )
    })
}

fn compare_trees(
    actual: &BTreeMap<String, String>,
    expected: &BTreeMap<String, String>,
) -> (GenerationReport, Vec<String>) {
    let mut report = GenerationReport::default();
    let mut drift = Vec::new();
    for (path, source) in expected {
        match actual.get(path) {
            None => {
                report.written += 1;
                drift.push(format!("missing {path}"));
            }
            Some(actual_source) if actual_source != source => {
                report.written += 1;
                drift.push(format!("changed {path}"));
            }
            Some(_) => report.unchanged += 1,
        }
    }
    for path in actual.keys() {
        if !expected.contains_key(path) {
            report.removed += 1;
            drift.push(format!("unexpected {path}"));
        }
    }
    (report, drift)
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
            GenerationError::io(path, io::Error::other(error))
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
