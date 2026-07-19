//! Locked source verification and schema-front-end invocation.

use std::{fs, path::Path};

use kafka_wire_schema::Message;
use sha2::{Digest, Sha256};

use crate::{GenerationError, lockfile::ProtocolLock};

/// Validated message paired with exact source provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MessageSource {
    pub(crate) message: Message,
    pub(crate) filename: String,
    pub(crate) sha256: String,
}

pub(crate) fn load_sources(
    workspace: &Path,
    lock: &ProtocolLock,
) -> Result<Vec<MessageSource>, GenerationError> {
    let source_root = workspace
        .join(&lock.kafka.vendored_root)
        .join(&lock.kafka.commit)
        .join("message");
    let mut sources = Vec::with_capacity(lock.kafka.files.len());
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
