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
        source: Box<toml::de::Error>,
    },
    /// A reviewed override document could not be decoded.
    #[error("invalid override file {path}: {source}")]
    Override {
        /// Override file path.
        path: PathBuf,
        /// TOML decoder error.
        #[source]
        source: Box<toml::de::Error>,
    },
    /// A decoded override violated its semantic contract.
    #[error("invalid override file {path}: {reason}")]
    InvalidOverride {
        /// Override file path.
        path: PathBuf,
        /// Rejected relationship or identity.
        reason: String,
    },
    /// The lockfile schema is unsupported.
    #[error("unsupported protocol lockfile schema {found}; expected 1")]
    LockfileSchema {
        /// Encountered schema version.
        found: u32,
    },
    /// The lock names a semantic IR contract this compiler does not implement.
    #[error("unsupported generator IR version {found}; expected {supported}")]
    IrVersion {
        /// Encountered semantic contract version.
        found: u32,
        /// The one semantic contract this compiler implements.
        supported: u32,
    },
    /// A decoded lockfile value violated its field contract.
    #[error("invalid {field} in protocol.lock: {reason}; found `{value}`")]
    InvalidLockfileValue {
        /// Fully qualified lockfile field.
        field: String,
        /// Rejected value.
        value: String,
        /// Required shape or identity.
        reason: &'static str,
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
    /// The configured output exists but does not prove generator ownership.
    #[error("refusing to replace unowned output tree {path}: {reason}")]
    UnownedOutputTree {
        /// Configured generated-tree destination.
        path: PathBuf,
        /// Missing or invalid ownership proof.
        reason: String,
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
    /// Whole-corpus semantics violated a cross-source invariant.
    #[error(transparent)]
    CorpusValidation(#[from] kafka_wire_schema::ValidationErrors),
    /// Request/response grouping rejected an incomplete or incompatible pair.
    #[error(transparent)]
    Pair(#[from] crate::PairError),
    /// Two producers claimed one emitted Rust symbol namespace.
    #[error(
        "generated symbol collision for `{symbol}` in {namespace}: first producer `{first}`, second producer `{second}`"
    )]
    GeneratedSymbolCollision {
        /// Rust namespace and scope where both claims land.
        namespace: String,
        /// Colliding emitted identifier.
        symbol: String,
        /// First message, struct, fixed output, or handwritten item.
        first: String,
        /// Later claimant.
        second: String,
    },
    /// Two compiler outputs claimed one generated-tree path.
    #[error(
        "generated path collision for {path}: first producer `{first}`, second producer `{second}`"
    )]
    GeneratedPathCollision {
        /// Colliding repository-relative output path.
        path: String,
        /// First phase or API that claimed the path.
        first: String,
        /// Later phase or API that attempted the same path.
        second: String,
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
    /// Rendering observed an IR state its validated phase contract forbids.
    #[error("internal generator invariant failed for {message}: {invariant}")]
    InternalInvariant {
        /// Protocol message whose renderable proof was incomplete.
        message: String,
        /// Missing or malformed proven fact.
        invariant: String,
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
    /// A fully staged generated tree did not reproduce the expected bytes.
    #[error("staged generated tree failed self-verification:\n{details}")]
    StagedTreeMismatch {
        /// Sorted staged-tree differences.
        details: String,
    },
    /// Replacing the generated directory failed, with rollback status retained.
    #[error("could not replace generated tree {root}: {source}; rollback: {rollback}")]
    TreeSwap {
        /// Generated tree destination.
        root: PathBuf,
        /// Failed rename operation.
        #[source]
        source: io::Error,
        /// Whether restoration succeeded, failed, or was unnecessary.
        rollback: String,
    },
}

impl GenerationError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Reports one normalized construct the backend has no emission rule for.
    ///
    /// Every renderer that reaches an unhandled construct must come here rather
    /// than emit a placeholder. A comment in place of a codec produces a file
    /// that can still compile while encoding nothing, which is a wrong-bytes
    /// bug wearing a green build.
    pub(crate) fn unsupported(
        message: &kafka_wire_schema::Message,
        field: &str,
        reason: impl Into<String>,
    ) -> Self {
        Self::UnsupportedSchema {
            message: message.name.protocol().to_owned(),
            field: field.to_owned(),
            reason: reason.into(),
        }
    }
}
