//! Recoverable installation of one complete vendored corpus and its lockfile.
//!
//! Fetching and lock construction happen elsewhere. This module stages and
//! verifies both filesystem targets, then coordinates portable swaps with
//! rollback so an ordinary failure cannot leave a mixed generation visible.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::vendor_verification::verify_staged;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A complete verified vendor update waiting beside its destinations.
pub(crate) struct StagedVendor {
    destination: PathBuf,
    lock_path: PathBuf,
    corpus_staging: PathBuf,
    lock_staging: PathBuf,
}

impl StagedVendor {
    /// Writes and verifies every fetched file and the matching rendered lock.
    pub(crate) fn new(
        destination: &Path,
        lock_path: &Path,
        corpus: &BTreeMap<String, Vec<u8>>,
        lock_document: &[u8],
    ) -> Result<Self, String> {
        let corpus_parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let lock_parent = lock_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(corpus_parent).map_err(|error| io_error(corpus_parent, error))?;
        fs::create_dir_all(lock_parent).map_err(|error| io_error(lock_parent, error))?;

        let corpus_staging = create_unique_directory(destination, "vendor-staging")?;
        let lock_staging = create_unique_file(lock_path, "vendor-staging", lock_document)?;
        let staged = Self {
            destination: destination.to_path_buf(),
            lock_path: lock_path.to_path_buf(),
            corpus_staging,
            lock_staging,
        };

        for (filename, expected) in corpus {
            let path = staged.corpus_staging.join(filename);
            fs::write(&path, expected).map_err(|error| io_error(&path, error))?;
        }
        verify_staged(
            &staged.corpus_staging,
            &staged.lock_staging,
            corpus,
            lock_document,
        )?;
        Ok(staged)
    }

    /// Installs both staged targets and restores both previous targets on error.
    pub(crate) fn commit(mut self) -> Result<(), String> {
        let corpus_backup = unique_sibling(&self.destination, "vendor-backup");
        let lock_backup = unique_sibling(&self.lock_path, "vendor-backup");
        let had_corpus = self.destination.exists();

        if had_corpus {
            fs::rename(&self.destination, &corpus_backup)
                .map_err(|error| io_error(&self.destination, error))?;
        }
        if let Err(error) = fs::rename(&self.lock_path, &lock_backup) {
            if had_corpus {
                let _ = fs::rename(&corpus_backup, &self.destination);
            }
            return Err(io_error(&self.lock_path, error));
        }

        if let Err(error) = fs::rename(&self.corpus_staging, &self.destination) {
            return Err(self.rollback(
                &corpus_backup,
                &lock_backup,
                had_corpus,
                io_error(&self.destination, error),
            ));
        }
        if let Err(error) = fs::rename(&self.lock_staging, &self.lock_path) {
            return Err(self.rollback(
                &corpus_backup,
                &lock_backup,
                had_corpus,
                io_error(&self.lock_path, error),
            ));
        }

        if had_corpus {
            fs::remove_dir_all(&corpus_backup).map_err(|error| io_error(&corpus_backup, error))?;
        }
        fs::remove_file(&lock_backup).map_err(|error| io_error(&lock_backup, error))?;
        self.corpus_staging.clear();
        self.lock_staging.clear();
        Ok(())
    }

    fn rollback(
        &self,
        corpus_backup: &Path,
        lock_backup: &Path,
        had_corpus: bool,
        cause: String,
    ) -> String {
        let mut failures = Vec::new();
        if self.destination.exists() {
            if let Err(error) = fs::remove_dir_all(&self.destination) {
                failures.push(io_error(&self.destination, error));
            }
        }
        if self.lock_path.exists() {
            if let Err(error) = fs::remove_file(&self.lock_path) {
                failures.push(io_error(&self.lock_path, error));
            }
        }
        if had_corpus {
            if let Err(error) = fs::rename(corpus_backup, &self.destination) {
                failures.push(io_error(corpus_backup, error));
            }
        }
        if let Err(error) = fs::rename(lock_backup, &self.lock_path) {
            failures.push(io_error(lock_backup, error));
        }
        let report = if failures.is_empty() {
            format!("{cause}; rollback succeeded")
        } else {
            format!("{cause}; rollback failures: {}", failures.join("; "))
        };
        drop(cause);
        report
    }

    #[cfg(test)]
    pub(crate) fn remove_staged_lock_for_test(&self) {
        fs::remove_file(&self.lock_staging)
            .unwrap_or_else(|error| panic!("remove staged lock: {error}"));
    }
}

impl Drop for StagedVendor {
    fn drop(&mut self) {
        if !self.corpus_staging.as_os_str().is_empty() {
            let _ = fs::remove_dir_all(&self.corpus_staging);
        }
        if !self.lock_staging.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.lock_staging);
        }
    }
}

fn create_unique_directory(target: &Path, role: &str) -> Result<PathBuf, String> {
    for _ in 0..1_000 {
        let path = unique_sibling(target, role);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error(&path, error)),
        }
    }
    Err("could not allocate a unique vendor staging directory".to_owned())
}

fn create_unique_file(target: &Path, role: &str, bytes: &[u8]) -> Result<PathBuf, String> {
    for _ in 0..1_000 {
        let path = unique_sibling(target, role);
        let opened = OpenOptions::new().write(true).create_new(true).open(&path);
        match opened {
            Ok(mut file) => {
                file.write_all(bytes)
                    .map_err(|error| io_error(&path, error))?;
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error(&path, error)),
        }
    }
    Err("could not allocate a unique vendor staging file".to_owned())
}

fn unique_sibling(target: &Path, role: &str) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("target");
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    parent.join(format!(".{name}.{role}.{}.{sequence}", std::process::id()))
}

fn io_error(path: &Path, error: io::Error) -> String {
    let report = format!(
        "filesystem operation failed for {}: {error}",
        path.display()
    );
    drop(error);
    report
}
