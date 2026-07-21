//! Strict reviewed protocol quirks loaded from `spec/overrides/`.
//!
//! This module owns common document I/O and schema versioning. Header policy
//! and accepted upstream defects validate in separate child domains.

mod header;
mod schema;

use std::path::{Path, PathBuf};

use crate::GenerationError;

pub(crate) use header::HeaderOverrides;
pub(crate) use schema::SchemaExceptionOverrides;

fn read_override(workspace_root: &Path, file: &str) -> Result<(PathBuf, Vec<u8>), GenerationError> {
    let path = workspace_root.join("spec").join("overrides").join(file);
    let source = std::fs::read(&path).map_err(|error| GenerationError::io(&path, error))?;
    Ok((path, source))
}

fn decode_override<T: serde::de::DeserializeOwned>(
    path: &Path,
    source: &[u8],
) -> Result<T, GenerationError> {
    let source = std::str::from_utf8(source).map_err(|error| {
        GenerationError::io(
            path,
            std::io::Error::new(std::io::ErrorKind::InvalidData, error),
        )
    })?;
    toml::from_str(source).map_err(|source| GenerationError::Override {
        path: path.to_path_buf(),
        source: Box::new(source),
    })
}

fn require_schema(path: &Path, schema: u32) -> Result<(), GenerationError> {
    if schema == 1 {
        Ok(())
    } else {
        invalid(path, format!("unsupported schema {schema}; expected 1"))
    }
}

fn invalid<T>(path: &Path, reason: impl Into<String>) -> Result<T, GenerationError> {
    Err(GenerationError::InvalidOverride {
        path: path.to_path_buf(),
        reason: reason.into(),
    })
}
