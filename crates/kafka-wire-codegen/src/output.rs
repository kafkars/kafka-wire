//! Generated-tree comparison and transactional directory replacement.
//!
//! It stages a complete sibling before moving the tree and owns no rendering.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use walkdir::WalkDir;

use crate::{GenerationError, GenerationMode};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Summary of one generation or verification run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenerationReport {
    /// Files whose expected contents differ from the prior tree.
    pub written: usize,
    /// Files already equal to expected output.
    pub unchanged: usize,
    /// Stale generated files removed by replacing the prior tree.
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
    let (report, drift) = compare_trees(&actual, expected);
    if drift.is_empty() {
        return Ok(report);
    }

    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| GenerationError::io(parent, error))?;
    let staging = create_unique_sibling(root, "staging")?;
    let mut staging_guard = DirectoryGuard::new(staging.clone());
    write_complete_tree(&staging, expected)?;

    let staged = existing_files(&staging)?;
    let (_, staged_drift) = compare_trees(&staged, expected);
    if !staged_drift.is_empty() {
        return Err(GenerationError::StagedTreeMismatch {
            details: staged_drift.join("\n"),
        });
    }

    replace_directory(root, &staging)?;
    staging_guard.disarm();
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
/// failed staged install restores it before returning.
pub(crate) fn replace_directory(root: &Path, staging: &Path) -> Result<(), GenerationError> {
    if !root.exists() {
        return fs::rename(staging, root).map_err(|source| GenerationError::TreeSwap {
            root: root.to_path_buf(),
            source,
            rollback: "not needed; no prior tree was moved".to_owned(),
        });
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

    fs::remove_dir_all(&backup).map_err(|error| GenerationError::io(&backup, error))
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

fn create_unique_sibling(root: &Path, role: &str) -> Result<PathBuf, GenerationError> {
    for _ in 0..1_000 {
        let candidate = unique_sibling_path(root, role);
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(GenerationError::io(&candidate, error)),
        }
    }
    let candidate = unique_sibling_path(root, role);
    Err(GenerationError::io(
        &candidate,
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique generated-tree sibling",
        ),
    ))
}

fn unique_sibling_path(root: &Path, role: &str) -> PathBuf {
    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("generated");
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    parent.join(format!(".{name}.{role}.{sequence}"))
}

struct DirectoryGuard {
    path: PathBuf,
    armed: bool,
}

impl DirectoryGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
