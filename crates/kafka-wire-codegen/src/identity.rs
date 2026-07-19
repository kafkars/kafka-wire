//! Public read-only identity for the pinned compiler input set.

use std::path::Path;

use crate::{GenerationError, lockfile::ProtocolLock};

/// Stable summary of the protocol source selected by `spec/protocol.lock`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolIdentity {
    /// Upstream repository URL recorded by the lockfile.
    pub repository: String,
    /// Full upstream commit SHA.
    pub commit: String,
    /// Generator IR schema version.
    pub ir_version: u32,
    /// Number of explicitly locked message files.
    pub source_files: usize,
}

/// Reads the pinned protocol identity without parsing or generating messages.
pub fn protocol_identity(
    workspace_root: impl AsRef<Path>,
) -> Result<ProtocolIdentity, GenerationError> {
    let path = workspace_root.as_ref().join("spec/protocol.lock");
    let lock = ProtocolLock::read(&path)?;
    Ok(ProtocolIdentity {
        repository: lock.kafka.repository,
        commit: lock.kafka.commit,
        ir_version: lock.generator.ir_version,
        source_files: lock.kafka.files.len(),
    })
}
