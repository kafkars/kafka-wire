//! Portable path values admitted by the protocol lockfile trust boundary.
//!
//! This file owns the narrow, host-independent grammar used before any locked
//! path reaches `Path::join`. It deliberately does not own lockfile structure,
//! filesystem access, or generated-output naming.

use std::{
    fmt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Deserializer};
use thiserror::Error;

/// A validated repository-relative path written with `/` separators.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RepoRelativePath(String);

/// A validated portable filename containing no directory separator.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PortableFilename(String);

/// A value that cannot name the same file safely on every supported host.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid portable {kind} `{value}`: {reason}")]
pub struct PortablePathError {
    kind: &'static str,
    value: String,
    reason: &'static str,
}

impl RepoRelativePath {
    /// Validates one slash-separated repository-relative path.
    pub fn try_new(value: impl Into<String>) -> Result<Self, PortablePathError> {
        let value = value.into();
        if value.is_empty() {
            return Err(invalid("repository path", value, "the path is empty"));
        }
        if let Some(reason) = value.split('/').find_map(component_error) {
            return Err(invalid("repository path", value, reason));
        }
        Ok(Self(value))
    }

    /// Returns the canonical lockfile spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Resolves this already-validated relative path beneath `root`.
    pub fn join_to(&self, root: &Path) -> PathBuf {
        root.join(&self.0)
    }

    /// Returns the final validated component.
    pub fn file_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(self.0.as_str())
    }
}

impl PortableFilename {
    /// Validates one filename portable across Unix and Windows filesystems.
    pub fn try_new(value: impl Into<String>) -> Result<Self, PortablePathError> {
        let value = value.into();
        if let Some(reason) = component_error(&value) {
            return Err(invalid("filename", value, reason));
        }
        Ok(Self(value))
    }

    /// Returns the validated filename.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Resolves this already-validated filename beneath `root`.
    pub fn join_to(&self, root: &Path) -> PathBuf {
        root.join(&self.0)
    }

    /// Returns the owned validated spelling.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for RepoRelativePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for PortableFilename {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RepoRelativePath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for PortableFilename {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

fn component_error(component: &str) -> Option<&'static str> {
    if component.is_empty() {
        return Some("a path component is empty");
    }
    if component == "." || component == ".." {
        return Some("dot components are forbidden");
    }
    if !component
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Some("components may contain only ASCII letters, digits, `_`, `-`, and `.`");
    }
    if component.ends_with('.') {
        return Some("components may not end in a dot");
    }
    let stem = component.split('.').next().unwrap_or(component);
    if windows_device_name(stem) {
        return Some("Windows device names are forbidden, including with an extension");
    }
    None
}

fn windows_device_name(stem: &str) -> bool {
    if ["CON", "PRN", "AUX", "NUL"]
        .iter()
        .any(|device| stem.eq_ignore_ascii_case(device))
    {
        return true;
    }
    let upper = stem.to_ascii_uppercase();
    matches!(
        upper.as_bytes(),
        [b'C', b'O', b'M', b'1'..=b'9'] | [b'L', b'P', b'T', b'1'..=b'9']
    )
}

fn invalid(kind: &'static str, value: String, reason: &'static str) -> PortablePathError {
    PortablePathError {
        kind,
        value,
        reason,
    }
}
