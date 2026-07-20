//! Reviewed protocol quirks, loaded from `spec/overrides/`.
//!
//! This file owns reading the exception data the renderer needs and nothing
//! about how it is emitted. A quirk lives here as data with an upstream
//! reference so that no renderer ever branches on a message name.

use std::path::Path;

use serde::Deserialize;

use crate::GenerationError;

/// Every reviewed header-version exception.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct HeaderOverrides {
    #[serde(default)]
    pub(crate) response_header_exceptions: Vec<ResponseHeaderException>,
}

/// One API whose response header version departs from the usual rule.
#[derive(Debug, Deserialize)]
pub(crate) struct ResponseHeaderException {
    pub(crate) api_key: i16,
    /// Inclusive first version the exception applies from.
    #[serde(deserialize_with = "first_version")]
    pub(crate) versions: i16,
    pub(crate) header_version: i16,
    /// Why the exception exists, carried into the generated doc comment.
    pub(crate) reason: String,
}

fn first_version<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<i16, D::Error> {
    let raw = String::deserialize(deserializer)?;
    let head = raw.trim_end_matches('+');
    let head = head.split('-').next().unwrap_or(head);
    head.parse().map_err(serde::de::Error::custom)
}

impl HeaderOverrides {
    /// Reads `spec/overrides/headers.toml` from the workspace.
    pub(crate) fn read(workspace_root: &Path) -> Result<Self, GenerationError> {
        let path = workspace_root
            .join("spec")
            .join("overrides")
            .join("headers.toml");
        let source = std::fs::read_to_string(&path).map_err(|error| GenerationError::Io {
            path: path.clone(),
            source: error,
        })?;
        toml::from_str(&source).map_err(|source| GenerationError::Lockfile { path, source })
    }
}
