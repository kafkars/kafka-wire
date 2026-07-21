//! Deserialization and validation of the pinned protocol input contract.
//!
//! This module owns the reader half of `spec/protocol.lock`: the schema the
//! generator will accept, the paths it will trust, and the per-file declaration
//! of whether a pinned message is compiled or merely vendored. It deliberately
//! owns no fetching and no writing; `xtask` owns the writer half.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::GenerationError;
use crate::{PortableFilename, RepoRelativePath};

/// The one semantic IR contract implemented by this compiler.
pub const SUPPORTED_IR_VERSION: u32 = 1;

/// The only repository directory this generator is authorized to replace.
pub(crate) const GENERATED_OUTPUT_PATH: &str = "crates/kafka-wire/src/generated";

/// Parsed repository protocol lockfile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProtocolLock {
    /// Lockfile schema version.
    pub schema: u32,
    /// Pinned Apache Kafka inputs.
    pub kafka: KafkaLock,
    /// Generator-model identity and output path.
    pub generator: GeneratorLock,
}

/// Pinned Apache Kafka source identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KafkaLock {
    /// Canonical upstream repository URL.
    pub repository: String,
    /// Full lowercase Git object ID.
    pub commit: String,
    /// Repository-relative upstream schema directory.
    pub upstream_message_root: RepoRelativePath,
    /// Repository-relative local vendor directory.
    pub vendored_root: RepoRelativePath,
    /// Complete pinned source inventory.
    pub files: Vec<LockedFile>,
}

/// One vendored upstream file and its content digest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LockedFile {
    /// Plain schema filename.
    pub path: PortableFilename,
    /// Lowercase SHA-256 of the exact vendored bytes.
    pub sha256: String,
    /// Whether generation compiles this source.
    pub status: SourceStatus,
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
pub enum SourceStatus {
    /// Parsed, lowered, validated, and rendered into checked-in Rust.
    Enabled,
    /// Vendored and hashed, but not yet within the backend's capability.
    Pending,
}

/// Versioned compiler-model metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GeneratorLock {
    /// Semantic IR contract version.
    pub ir_version: u32,
    /// Repository-relative generated-tree destination.
    pub output: RepoRelativePath,
}

impl ProtocolLock {
    /// Reads and fully validates a protocol lockfile.
    pub fn read(path: &Path) -> Result<Self, GenerationError> {
        Self::read_with_bytes(path).map(|(lock, _)| lock)
    }

    /// Reads once, validates the exact bytes, and returns them for provenance.
    pub(crate) fn read_with_bytes(path: &Path) -> Result<(Self, Vec<u8>), GenerationError> {
        let bytes = fs::read(path).map_err(|error| GenerationError::io(path, error))?;
        let source = std::str::from_utf8(&bytes).map_err(|error| {
            GenerationError::io(
                path,
                std::io::Error::new(std::io::ErrorKind::InvalidData, error),
            )
        })?;
        let lock = Self::parse(path, source)?;
        Ok((lock, bytes))
    }

    /// Decodes and validates lockfile text associated with `path`.
    pub fn parse(path: &Path, source: &str) -> Result<Self, GenerationError> {
        let lock: Self = toml::from_str(source).map_err(|source| GenerationError::Lockfile {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        if lock.schema != 1 {
            return Err(GenerationError::LockfileSchema { found: lock.schema });
        }
        if lock.generator.ir_version != SUPPORTED_IR_VERSION {
            return Err(GenerationError::IrVersion {
                found: lock.generator.ir_version,
                supported: SUPPORTED_IR_VERSION,
            });
        }
        if lock.generator.output.as_str() != GENERATED_OUTPUT_PATH {
            return invalid_value(
                "generator.output",
                lock.generator.output.as_str(),
                "expected the owned kafka-wire generated source directory",
            );
        }
        if lock.kafka.repository != "https://github.com/apache/kafka" {
            return invalid_value(
                "kafka.repository",
                &lock.kafka.repository,
                "expected the canonical Apache Kafka repository URL",
            );
        }
        validate_lower_hex("kafka.commit", &lock.kafka.commit, 40)?;
        let mut paths = BTreeSet::new();
        for file in &lock.kafka.files {
            validate_lower_hex(
                &format!("kafka.files[{}].sha256", file.path),
                &file.sha256,
                64,
            )?;
            if !paths.insert(file.path.as_str().to_ascii_lowercase()) {
                return invalid_value(
                    "kafka.files.path",
                    file.path.as_str(),
                    "source paths must be unique under ASCII case folding",
                );
            }
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
    pub fn vendored_message_root(&self, workspace: &Path) -> PathBuf {
        self.vendored_root
            .join_to(workspace)
            .join(&self.commit)
            .join(self.message_directory())
    }

    /// Leaf directory name shared by the upstream and vendored message trees.
    fn message_directory(&self) -> &str {
        self.upstream_message_root.file_name()
    }
}

fn validate_lower_hex(field: &str, value: &str, width: usize) -> Result<(), GenerationError> {
    if value.len() == width
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid_value(field, value, "expected fixed-width lowercase hexadecimal")
    }
}

fn invalid_value<T>(
    field: impl Into<String>,
    value: &str,
    reason: &'static str,
) -> Result<T, GenerationError> {
    Err(GenerationError::InvalidLockfileValue {
        field: field.into(),
        value: value.to_owned(),
        reason,
    })
}
