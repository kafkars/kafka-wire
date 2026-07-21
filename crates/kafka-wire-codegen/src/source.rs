//! Locked source verification and schema-front-end invocation.
//!
//! Two obligations meet here and are deliberately kept apart. Every file pinned
//! by `spec/protocol.lock` is byte verified against its recorded digest, so
//! vendored drift fails generation regardless of what the backend can compile.
//! Only an `enabled` file is then handed to the schema front end, so the
//! vendored corpus may grow ahead of generation capability without breaking a
//! green tree.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_schema::{Message, SourceFile};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::{
    GenerationError,
    lockfile::{ProtocolLock, SourceStatus},
    overrides::SchemaExceptionOverrides,
};

/// Validated message paired with exact source provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MessageSource {
    pub(crate) message: Message,
    pub(crate) filename: String,
    pub(crate) sha256: String,
}

/// Every locked file, byte verified, with the front end's answer for each.
///
/// The all-or-nothing `load_sources` above is what generation uses: a corpus
/// with one unreadable schema is not a corpus to generate from. This one exists
/// for measurement, where a rejected file is a data point rather than a fault,
/// and `status` is deliberately ignored so the answer covers everything pinned.
#[derive(Clone, Debug, Default)]
pub(crate) struct LoadedCorpus {
    /// Files the front end accepted, in lockfile order.
    pub(crate) sources: Vec<MessageSource>,
    /// Files the front end rejected, by filename, with its diagnostic.
    pub(crate) rejected: BTreeMap<String, String>,
}

pub(crate) fn load_every_source(
    workspace: &Path,
    lock: &ProtocolLock,
) -> Result<LoadedCorpus, GenerationError> {
    let exceptions = SchemaExceptionOverrides::read(workspace, lock)?.exceptions();
    load_every_source_with(workspace, lock, &exceptions)
}

fn load_every_source_with(
    workspace: &Path,
    lock: &ProtocolLock,
    exceptions: &kafka_wire_schema::SchemaExceptions,
) -> Result<LoadedCorpus, GenerationError> {
    let source_root = lock.kafka.vendored_message_root(workspace)?;
    let mut corpus = LoadedCorpus::default();
    for locked in &lock.kafka.files {
        let path = source_root.join(&locked.path);
        let source = read_verified_source(&path, &locked.sha256)?;

        match kafka_wire_schema::load_source_with(source, exceptions) {
            Ok(message) => corpus.sources.push(MessageSource {
                message,
                filename: locked.path.clone(),
                sha256: locked.sha256.clone(),
            }),
            Err(error) => {
                corpus
                    .rejected
                    .insert(locked.path.clone(), error.to_string());
            }
        }
    }
    Ok(corpus)
}

#[cfg(test)]
pub(crate) fn load_sources(
    workspace: &Path,
    lock: &ProtocolLock,
) -> Result<Vec<MessageSource>, GenerationError> {
    let exceptions = SchemaExceptionOverrides::read(workspace, lock)?.exceptions();
    load_sources_with(workspace, lock, &exceptions)
}

pub(crate) fn load_sources_with(
    workspace: &Path,
    lock: &ProtocolLock,
    exceptions: &kafka_wire_schema::SchemaExceptions,
) -> Result<Vec<MessageSource>, GenerationError> {
    let source_root = lock.kafka.vendored_message_root(workspace)?;
    let enabled = lock
        .kafka
        .files
        .iter()
        .filter(|locked| locked.status == SourceStatus::Enabled)
        .count();
    let mut sources = Vec::with_capacity(enabled);
    for locked in &lock.kafka.files {
        let path = source_root.join(&locked.path);
        let source = read_verified_source(&path, &locked.sha256)?;

        if locked.status == SourceStatus::Pending {
            continue;
        }

        let message = kafka_wire_schema::load_source_with(source, exceptions)?;
        sources.push(MessageSource {
            message,
            filename: locked.path.clone(),
            sha256: locked.sha256.clone(),
        });
    }
    sources.sort_by(|left, right| {
        left.message
            .api_key
            .cmp(&right.message.api_key)
            .then_with(|| {
                left.message
                    .name
                    .protocol()
                    .cmp(right.message.name.protocol())
            })
    });
    Ok(sources)
}

/// Reads once, verifies that exact buffer, and turns the same bytes into source.
fn read_verified_source(path: &Path, expected: &str) -> Result<SourceFile, GenerationError> {
    let bytes = fs::read(path).map_err(|error| GenerationError::io(path, error))?;
    let actual = hex_digest(&bytes);
    if actual != expected {
        return Err(GenerationError::SourceHash {
            path: path.to_path_buf(),
            expected: expected.to_owned(),
            actual,
        });
    }
    SourceFile::from_bytes(path.to_path_buf(), bytes)
        .map_err(|error| GenerationError::io(path, error))
}

pub(crate) fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

/// Canonical compiler source bytes used by generated provenance.
pub(crate) fn compiler_source_bytes() -> Result<Vec<u8>, GenerationError> {
    let codegen = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository = codegen.parent().and_then(Path::parent).unwrap_or(codegen);
    let mut paths = Vec::new();
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "crates/kafka-wire-codegen/Cargo.toml",
        "crates/kafka-wire-codegen/src",
        "crates/kafka-wire-schema/Cargo.toml",
        "crates/kafka-wire-schema/src",
    ] {
        let path = repository.join(relative);
        if path.is_dir() {
            for entry in WalkDir::new(&path) {
                let entry = entry.map_err(|error| walk_error(&path, error))?;
                if entry.file_type().is_file() {
                    paths.push(entry.into_path());
                }
            }
        } else if path.is_file() {
            paths.push(path);
        }
    }
    paths.sort_by_key(|path| canonical_relative(repository, path));

    let mut canonical = Vec::new();
    for path in paths {
        let relative = canonical_relative(repository, &path);
        let bytes = fs::read(&path).map_err(|error| GenerationError::io(&path, error))?;
        append_component(&mut canonical, &relative, &bytes);
    }
    Ok(canonical)
}

/// Canonical formatter identity and workspace configuration bytes.
pub(crate) fn rustfmt_source_bytes(
    workspace_root: &Path,
    rustfmt_identity: &str,
) -> Result<Vec<u8>, GenerationError> {
    let mut canonical = Vec::new();
    append_component(&mut canonical, "identity", rustfmt_identity.as_bytes());
    for relative in ["rustfmt.toml", "rust-toolchain.toml"] {
        let path = workspace_root.join(relative);
        if path.is_file() {
            let bytes = fs::read(&path).map_err(|error| GenerationError::io(&path, error))?;
            append_component(&mut canonical, relative, &bytes);
        }
    }
    Ok(canonical)
}

pub(crate) fn append_component(output: &mut Vec<u8>, label: &str, bytes: &[u8]) {
    output.extend_from_slice(&(label.len() as u64).to_be_bytes());
    output.extend_from_slice(label.as_bytes());
    output.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    output.extend_from_slice(bytes);
}

fn canonical_relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn walk_error(path: &Path, error: walkdir::Error) -> GenerationError {
    let affected = error
        .path()
        .map_or_else(|| path.to_path_buf(), PathBuf::from);
    GenerationError::io(affected, std::io::Error::other(error))
}
