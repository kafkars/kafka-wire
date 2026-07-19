//! Owned source file used by front-end diagnostics.

use std::{fs, io, path::PathBuf};

/// UTF-8 schema source and its repository-relative or absolute path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceFile {
    path: PathBuf,
    contents: String,
}

impl SourceFile {
    /// Reads a UTF-8 source file from disk.
    pub fn read(path: impl Into<PathBuf>) -> Result<Self, io::Error> {
        let path = path.into();
        let contents = fs::read_to_string(&path)?;
        Ok(Self { path, contents })
    }

    /// Creates a source file from in-memory text, primarily for focused tests.
    pub fn new(path: impl Into<PathBuf>, contents: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            contents: contents.into(),
        }
    }

    /// Returns the source path.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Returns the source text.
    pub fn contents(&self) -> &str {
        &self.contents
    }
}
