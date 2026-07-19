//! Locked source verification and schema-front-end invocation.
//!
//! Two obligations meet here and are deliberately kept apart. Every file pinned
//! by `spec/protocol.lock` is byte verified against its recorded digest, so
//! vendored drift fails generation regardless of what the backend can compile.
//! Only an `enabled` file is then handed to the schema front end, so the
//! vendored corpus may grow ahead of generation capability without breaking a
//! green tree.

use std::{collections::BTreeMap, fs, path::Path};

use kafka_wire_schema::Message;
use sha2::{Digest, Sha256};

use crate::{
    GenerationError,
    lockfile::{ProtocolLock, SourceStatus},
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
    let source_root = lock.kafka.vendored_message_root(workspace)?;
    let mut corpus = LoadedCorpus::default();
    for locked in &lock.kafka.files {
        let path = source_root.join(&locked.path);
        let bytes = fs::read(&path).map_err(|error| GenerationError::io(&path, error))?;
        let actual = hex_digest(&bytes);
        if actual != locked.sha256 {
            return Err(GenerationError::SourceHash {
                path,
                expected: locked.sha256.clone(),
                actual,
            });
        }

        match kafka_wire_schema::load_message(&path) {
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

pub(crate) fn load_sources(
    workspace: &Path,
    lock: &ProtocolLock,
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
        let bytes = fs::read(&path).map_err(|error| GenerationError::io(&path, error))?;
        let actual = hex_digest(&bytes);
        if actual != locked.sha256 {
            return Err(GenerationError::SourceHash {
                path,
                expected: locked.sha256.clone(),
                actual,
            });
        }

        if locked.status == SourceStatus::Pending {
            continue;
        }

        let message = kafka_wire_schema::load_message(&path)?;
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
