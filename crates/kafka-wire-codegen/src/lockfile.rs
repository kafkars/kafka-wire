//! Deserialization and validation of the pinned protocol input contract.
//!
//! This module owns the reader half of `spec/protocol.lock`: the schema the
//! generator will accept, the paths it will trust, and the per-file declaration
//! of whether a pinned message is compiled or merely vendored. It deliberately
//! owns no fetching and no writing; `xtask` owns the writer half.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use serde::Deserialize;

use crate::GenerationError;

/// Parsed repository protocol lockfile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct ProtocolLock {
    pub(crate) schema: u32,
    pub(crate) kafka: KafkaLock,
    pub(crate) generator: GeneratorLock,
}

/// Pinned Apache Kafka source identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct KafkaLock {
    pub(crate) repository: String,
    pub(crate) commit: String,
    pub(crate) upstream_message_root: String,
    pub(crate) vendored_root: String,
    pub(crate) files: Vec<LockedFile>,
}

/// One vendored upstream file and its content digest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct LockedFile {
    pub(crate) path: String,
    pub(crate) sha256: String,
    pub(crate) status: SourceStatus,
}

/// Whether the backend compiles a pinned message or only pins its bytes.
///
/// Vendoring the upstream corpus and being able to generate from it are separate
/// capabilities. `status` is the seam between them: every pinned file is byte
/// verified, and only an `enabled` file is handed to the schema front end. There
/// is no default — a new entry must state which of the two it is, so a message
/// can never join or leave the compiled set by accident.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SourceStatus {
    /// Parsed, lowered, validated, and rendered into checked-in Rust.
    Enabled,
    /// Vendored and hashed, but not yet within the backend's capability.
    Pending,
}

/// Versioned compiler-model metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct GeneratorLock {
    pub(crate) ir_version: u32,
    pub(crate) output: String,
}

impl ProtocolLock {
    pub(crate) fn read(path: &Path) -> Result<Self, GenerationError> {
        let source = fs::read_to_string(path).map_err(|error| GenerationError::io(path, error))?;
        let lock: Self = toml::from_str(&source).map_err(|source| GenerationError::Lockfile {
            path: path.to_path_buf(),
            source,
        })?;
        if lock.schema != 1 {
            return Err(GenerationError::LockfileSchema { found: lock.schema });
        }
        validate_relative_path(
            "kafka.upstream_message_root",
            &lock.kafka.upstream_message_root,
        )?;
        validate_relative_path("kafka.vendored_root", &lock.kafka.vendored_root)?;
        validate_relative_path("generator.output", &lock.generator.output)?;
        lock.kafka.message_directory()?;
        for file in &lock.kafka.files {
            validate_plain_filename(&file.path)?;
        }
        Ok(lock)
    }
}

impl KafkaLock {
    /// Directory holding the vendored copy of the pinned message corpus.
    ///
    /// The vendored tree mirrors upstream's leaf directory under the pinned
    /// commit, so `upstream_message_root` names both where the bytes came from
    /// and what the local directory is called. Without this the configured
    /// upstream path would be recorded and then ignored.
    pub(crate) fn vendored_message_root(
        &self,
        workspace: &Path,
    ) -> Result<PathBuf, GenerationError> {
        Ok(workspace
            .join(&self.vendored_root)
            .join(&self.commit)
            .join(self.message_directory()?))
    }

    /// Leaf directory name shared by the upstream and vendored message trees.
    fn message_directory(&self) -> Result<&str, GenerationError> {
        Path::new(&self.upstream_message_root)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| GenerationError::UnsafeConfiguredPath {
                field: "kafka.upstream_message_root",
                path: self.upstream_message_root.clone(),
            })
    }
}

fn validate_plain_filename(path: &str) -> Result<(), GenerationError> {
    let candidate = Path::new(path);
    let plain = candidate.file_name().and_then(|name| name.to_str()) == Some(path);
    if plain && path != "." && path != ".." {
        Ok(())
    } else {
        Err(GenerationError::UnsafeSourcePath {
            path: path.to_owned(),
        })
    }
}

fn validate_relative_path(field: &'static str, path: &str) -> Result<(), GenerationError> {
    let candidate = Path::new(path);
    let safe = !path.trim().is_empty()
        && !candidate.is_absolute()
        && candidate
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir));
    if safe {
        Ok(())
    } else {
        Err(GenerationError::UnsafeConfiguredPath {
            field,
            path: path.to_owned(),
        })
    }
}
