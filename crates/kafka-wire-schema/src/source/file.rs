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
        let bytes = fs::read(&path)?;
        Self::from_bytes(path, bytes)
    }

    /// Creates a source file from exact UTF-8 bytes already obtained by a caller.
    ///
    /// A verifier can hash a buffer and hand that same allocation to the front
    /// end, proving that parsing cannot observe a second filesystem read.
    pub fn from_bytes(path: impl Into<PathBuf>, bytes: Vec<u8>) -> Result<Self, io::Error> {
        let path = path.into();
        let contents = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
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
