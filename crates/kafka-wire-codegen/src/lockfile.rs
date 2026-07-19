//! Deserialization and validation of the pinned protocol input contract.

use std::{
    fs,
    path::{Component, Path},
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
        for file in &lock.kafka.files {
            validate_plain_filename(&file.path)?;
        }
        Ok(lock)
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
