//! Allocation and lifetime cleanup for generated-tree staging directories.
//!
//! This module deliberately does not replace the live generated tree; the
//! output transaction owns that commit decision.

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::GenerationError;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// An exclusively created sibling removed unless its transaction commits it.
pub(crate) struct StagingDirectory {
    path: PathBuf,
    armed: bool,
}

impl StagingDirectory {
    pub(crate) fn create(root: &Path) -> Result<Self, GenerationError> {
        for _ in 0..1_000 {
            let path = unique_sibling_path(root);
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path, armed: true }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(GenerationError::io(&path, error)),
            }
        }
        let path = unique_sibling_path(root);
        Err(GenerationError::io(
            &path,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "could not allocate a unique generated-tree sibling",
            ),
        ))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn unique_sibling_path(root: &Path) -> PathBuf {
    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("generated");
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    parent.join(format!(".{name}.staging.{sequence}"))
}
