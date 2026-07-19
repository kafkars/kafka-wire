//! The writer half of `spec/protocol.lock`.
//!
//! This module owns rendering the pinned-input contract as deterministic TOML and
//! reading back the one fact vendoring must never invent: which files the backend
//! has been deliberately enabled for. Re-vendoring is a bytes-and-digests
//! operation; promoting a message from `pending` to `enabled` is a separate,
//! reviewed edit.
//!
//! `kafka-wire-codegen::lockfile` owns the reader half and remains the authority that
//! rejects a malformed document. This module deliberately owns no schema
//! validation, no path policy, and no generation decisions.

use std::{collections::BTreeMap, fs, path::Path};

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Pinned upstream identity plus every vendored file and its recorded digest.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ProtocolLock {
    pub(crate) schema: u32,
    pub(crate) kafka: PinnedSource,
    pub(crate) generator: GeneratorPin,
}

/// Upstream repository, commit, and message-tree coordinates.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PinnedSource {
    pub(crate) repository: String,
    pub(crate) commit: String,
    pub(crate) upstream_message_root: String,
    pub(crate) vendored_root: String,
    pub(crate) files: Vec<VendoredFile>,
}

/// Compiler-model metadata carried through a re-vendor unchanged.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GeneratorPin {
    pub(crate) ir_version: u32,
    pub(crate) output: String,
}

/// One vendored upstream file, its digest, and its generation status.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct VendoredFile {
    pub(crate) path: String,
    pub(crate) sha256: String,
    pub(crate) status: SourceStatus,
}

/// Whether the backend compiles a pinned message or only pins its bytes.
///
/// This mirrors `kafka-wire-codegen`'s reader-side status by spelling, not by shared
/// type: the generator's lockfile model is its private input contract. The
/// spelling cannot drift silently, because the reader rejects any token it does
/// not recognize on the very next `cargo xtask generated-check`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SourceStatus {
    /// Parsed, lowered, validated, and rendered into checked-in Rust.
    Enabled,
    /// Vendored and hashed, but not yet within the backend's capability.
    Pending,
}

impl SourceStatus {
    const fn token(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Pending => "pending",
        }
    }
}

impl ProtocolLock {
    /// Reads the current pinned-input contract.
    pub(crate) fn read(path: &Path) -> Result<Self, String> {
        let source = fs::read_to_string(path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        toml::from_str(&source)
            .map_err(|error| format!("could not parse {}: {error}", path.display()))
    }

    /// Statuses recorded for files already present in the lock.
    ///
    /// A file upstream added since the last vendoring has no entry here, and the
    /// caller records it as `pending`: new protocol surface is never assumed to
    /// be within the backend's reach.
    pub(crate) fn recorded_statuses(&self) -> BTreeMap<&str, SourceStatus> {
        self.kafka
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.status))
            .collect()
    }

    /// Renders the contract as deterministic, diff-stable TOML.
    ///
    /// One table per block, blocks separated by one blank line, files in the
    /// order the caller recorded them. Re-vendoring an unchanged commit must
    /// reproduce this document byte for byte.
    pub(crate) fn render(&self) -> String {
        let mut blocks = vec![
            format!("schema = {}", self.schema),
            [
                "[kafka]".to_owned(),
                format!("repository = \"{}\"", self.kafka.repository),
                format!("commit = \"{}\"", self.kafka.commit),
                format!(
                    "upstream_message_root = \"{}\"",
                    self.kafka.upstream_message_root
                ),
                format!("vendored_root = \"{}\"", self.kafka.vendored_root),
            ]
            .join("\n"),
        ];

        for file in &self.kafka.files {
            blocks.push(
                [
                    "[[kafka.files]]".to_owned(),
                    format!("path = \"{}\"", file.path),
                    format!("sha256 = \"{}\"", file.sha256),
                    format!("status = \"{}\"", file.status.token()),
                ]
                .join("\n"),
            );
        }

        blocks.push(
            [
                "[generator]".to_owned(),
                format!("ir_version = {}", self.generator.ir_version),
                format!("output = \"{}\"", self.generator.output),
            ]
            .join("\n"),
        );

        let mut document = blocks.join("\n\n");
        document.push('\n');
        document
    }

    /// Writes the rendered contract in place.
    pub(crate) fn write(&self, path: &Path) -> Result<(), String> {
        fs::write(path, self.render())
            .map_err(|error| format!("could not write {}: {error}", path.display()))
    }
}

/// Lowercase hexadecimal SHA-256 of exactly the bytes that were vendored.
pub(crate) fn digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let hash = Sha256::digest(bytes);
    let mut output = String::with_capacity(hash.len() * 2);
    for byte in hash {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
