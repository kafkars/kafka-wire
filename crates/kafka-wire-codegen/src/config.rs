//! Explicit generator paths and write policy.

use std::path::{Path, PathBuf};

/// Whether generation mutates files or verifies the checked-in tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationMode {
    /// Replace changed generated files and remove stale outputs.
    Write,
    /// Compare expected output without modifying the workspace.
    Check,
}

/// Complete deterministic generator configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratorConfig {
    workspace_root: PathBuf,
    mode: GenerationMode,
}

impl GeneratorConfig {
    /// Creates a configuration rooted at a repository checkout.
    pub fn new(workspace_root: impl Into<PathBuf>, mode: GenerationMode) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            mode,
        }
    }

    /// Returns the repository root.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Returns write or check mode.
    pub const fn mode(&self) -> GenerationMode {
        self.mode
    }

    pub(crate) fn lockfile_path(&self) -> PathBuf {
        self.workspace_root.join("spec/protocol.lock")
    }
}
