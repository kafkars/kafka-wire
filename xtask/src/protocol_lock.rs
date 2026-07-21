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

use std::{collections::BTreeMap, path::Path};

use sha2::{Digest, Sha256};

pub(crate) use kafka_wire_codegen::{
    LockedFile as VendoredFile, PortableFilename, ProtocolLock, SourceStatus,
};

/// Reads the writer's input through the generator's validated lock model.
pub(crate) fn read(path: &Path) -> Result<ProtocolLock, String> {
    ProtocolLock::read(path).map_err(|error| error.to_string())
}

/// Statuses recorded for files already present in the lock.
///
/// A file upstream added since the last vendoring has no entry here, and the
/// caller records it as `pending`: new protocol surface is never assumed to be
/// within the backend's reach.
pub(crate) fn recorded_statuses(lock: &ProtocolLock) -> BTreeMap<&str, SourceStatus> {
    lock.kafka
        .files
        .iter()
        .map(|file| (file.path.as_str(), file.status))
        .collect()
}

/// Renders the validated contract as deterministic, diff-stable TOML.
///
/// One table per block, blocks separated by one blank line, files in the order
/// the caller recorded them. Re-vendoring an unchanged commit must reproduce
/// this document byte for byte.
pub(crate) fn render(lock: &ProtocolLock) -> String {
    let mut blocks = vec![
        format!("schema = {}", lock.schema),
        [
            "[kafka]".to_owned(),
            format!("repository = \"{}\"", lock.kafka.repository),
            format!("commit = \"{}\"", lock.kafka.commit),
            format!(
                "upstream_message_root = \"{}\"",
                lock.kafka.upstream_message_root
            ),
            format!("vendored_root = \"{}\"", lock.kafka.vendored_root),
        ]
        .join("\n"),
    ];

    for file in &lock.kafka.files {
        blocks.push(
            [
                "[[kafka.files]]".to_owned(),
                format!("path = \"{}\"", file.path),
                format!("sha256 = \"{}\"", file.sha256),
                format!("status = \"{}\"", status_token(file.status)),
            ]
            .join("\n"),
        );
    }

    blocks.push(
        [
            "[generator]".to_owned(),
            format!("ir_version = {}", lock.generator.ir_version),
            format!("output = \"{}\"", lock.generator.output),
        ]
        .join("\n"),
    );

    let mut document = blocks.join("\n\n");
    document.push('\n');
    document
}

const fn status_token(status: SourceStatus) -> &'static str {
    match status {
        SourceStatus::Enabled => "enabled",
        SourceStatus::Pending => "pending",
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
