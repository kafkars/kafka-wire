//! Phase-aware generator diagnostics.

use std::{io, path::PathBuf};

use thiserror::Error;

/// Deterministic generation failure.
#[derive(Debug, Error)]
pub enum GenerationError {
    /// A required file could not be read or written.
    #[error("filesystem operation failed for {path}: {source}")]
    Io {
        /// Affected path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: io::Error,
    },
    /// `protocol.lock` could not be decoded.
    #[error("invalid protocol lockfile {path}: {source}")]
    Lockfile {
        /// Lockfile path.
        path: PathBuf,
        /// TOML decoder error.
        #[source]
        source: toml::de::Error,
    },
    /// The lockfile schema is unsupported.
    #[error("unsupported protocol lockfile schema {found}; expected 1")]
    LockfileSchema {
        /// Encountered schema version.
        found: u32,
    },
    /// A lockfile source path was not one plain filename.
    #[error("unsafe source path in protocol.lock: {path}")]
    UnsafeSourcePath {
        /// Rejected path.
        path: String,
    },
    /// A configured repository-relative directory escaped its intended root.
    #[error("unsafe {field} path in protocol.lock: {path}")]
    UnsafeConfiguredPath {
        /// Lockfile field name.
        field: &'static str,
        /// Rejected path.
        path: String,
    },
    /// A pinned source hash did not match the vendored file.
    #[error("source hash mismatch for {path}: expected {expected}, found {actual}")]
    SourceHash {
        /// Source path.
        path: PathBuf,
        /// Lockfile digest.
        expected: String,
        /// Actual digest.
        actual: String,
    },
    /// The schema front end rejected a pinned source.
    #[error(transparent)]
    Schema(#[from] kafka_wire_schema::SchemaError),
    /// Two messages claimed the same direction for one API key.
    #[error("duplicate {direction} message for API key {api_key}: {left} and {right}")]
    DuplicateDirection {
        /// API key.
        api_key: i16,
        /// Request or response.
        direction: &'static str,
        /// First message.
        left: String,
        /// Second message.
        right: String,
    },
    /// One request and response sharing an API key had different API stems.
    #[error("API key {api_key} has mismatched pair names: {request} and {response}")]
    PairName {
        /// API key.
        api_key: i16,
        /// Request name.
        request: String,
        /// Response name.
        response: String,
    },
    /// The initial backend does not yet implement one normalized construct.
    #[error("cannot render {message}.{field}: {reason}")]
    UnsupportedSchema {
        /// Protocol message.
        message: String,
        /// Protocol field or `<message>`.
        field: String,
        /// Unsupported construct.
        reason: String,
    },
    /// The formatter that owns generated layout could not be launched.
    #[error(
        "could not run `{program}` to format generated Rust: {source}\n\
         rustfmt owns generated layout; install it for the toolchain pinned in \
         rust-toolchain.toml with `rustup component add rustfmt`, or set RUSTFMT to its path"
    )]
    FormatterUnavailable {
        /// Formatter program that could not be launched.
        program: String,
        /// Spawn or pipe failure.
        #[source]
        source: io::Error,
    },
    /// The formatter rejected one rendered file.
    #[error("rustfmt rejected generated {path}:\n{details}")]
    Formatter {
        /// Generated file name.
        path: String,
        /// Formatter exit status and diagnostics.
        details: String,
    },
    /// Generated manifest JSON serialization failed.
    #[error("could not serialize generated manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    /// Check mode found a generated-tree drift.
    #[error("generated protocol tree is stale:\n{details}")]
    Stale {
        /// Sorted changed, missing, or unexpected files.
        details: String,
    },
}

impl GenerationError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}
