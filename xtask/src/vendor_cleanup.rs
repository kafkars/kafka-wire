//! Cleanup reporting for staged and already-committed vendor artifacts.
//!
//! It owns the distinction between a staging failure and a post-commit warning;
//! it does not decide when the corpus and lockfile are installed.

use std::{fs, path::Path};

/// Removes a staged directory while preserving the failure that prompted it.
pub(crate) fn directory_after_error(path: &Path, cause: String) -> String {
    match fs::remove_dir_all(path) {
        Ok(()) => cause,
        Err(error) => format!("{cause}; cleanup failed for {}: {error}", path.display()),
    }
}

/// Removes a partial staged file while preserving the write failure.
pub(crate) fn file_after_error(path: &Path, cause: String) -> String {
    match fs::remove_file(path) {
        Ok(()) => cause,
        Err(error) => format!("{cause}; cleanup failed for {}: {error}", path.display()),
    }
}

/// Best-effort removal after both new vendor targets are already live.
///
/// A cleanup error is a warning here. Calling it a failed commit would make the
/// caller's view of the installed state ambiguous.
pub(crate) fn installed_backups(
    corpus_backup: &Path,
    lock_backup: &Path,
    had_corpus: bool,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if had_corpus {
        if let Err(error) = fs::remove_dir_all(corpus_backup) {
            warnings.push(format!(
                "vendor pair was installed, but obsolete backup {} could not be removed: {error}",
                corpus_backup.display()
            ));
        }
    }
    if let Err(error) = fs::remove_file(lock_backup) {
        warnings.push(format!(
            "vendor pair was installed, but obsolete backup {} could not be removed: {error}",
            lock_backup.display()
        ));
    }
    warnings
}
