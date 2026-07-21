//! Reviewed schema-defect exception validation against the pinned lock.
//!
//! This module owns exception identity and deliberately no front-end behavior.

use std::{collections::BTreeSet, path::Path};

use serde::Deserialize;

use crate::{GenerationError, lockfile::ProtocolLock};

use super::{decode_override, invalid, read_override, require_schema};

/// Every reviewed upstream schema defect the front end accepts by name.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchemaExceptionOverrides {
    schema: u32,
    #[serde(default)]
    accepted: Vec<AcceptedDefect>,
    #[serde(skip)]
    input_bytes: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedDefect {
    message: String,
    field: Option<String>,
    code: String,
    reason: String,
    upstream: String,
}

impl SchemaExceptionOverrides {
    /// Reads and validates `schema_exceptions.toml` against the pinned lock.
    pub(crate) fn read(
        workspace_root: &Path,
        lock: &ProtocolLock,
    ) -> Result<Self, GenerationError> {
        let (path, source) = read_override(workspace_root, "schema_exceptions.toml")?;
        let mut overrides: Self = decode_override(&path, &source)?;
        overrides.input_bytes = source;
        overrides.validate(&path, lock)?;
        Ok(overrides)
    }

    /// Exact validated document bytes used to construct these exceptions.
    pub(crate) fn input_bytes(&self) -> &[u8] {
        &self.input_bytes
    }

    fn validate(&self, path: &Path, lock: &ProtocolLock) -> Result<(), GenerationError> {
        require_schema(path, self.schema)?;
        let locked = lock
            .kafka
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::new();
        for entry in &self.accepted {
            validate_entry(path, lock, &locked, entry)?;
            let identity = (
                entry.message.as_str(),
                entry.field.as_deref(),
                entry.code.as_str(),
            );
            if !seen.insert(identity) {
                return invalid(
                    path,
                    format!(
                        "duplicate schema exception for {}.{:?} {}",
                        entry.message, entry.field, entry.code
                    ),
                );
            }
        }
        Ok(())
    }

    /// The reviewed set, in the shape the schema front end validates against.
    pub(crate) fn exceptions(&self) -> kafka_wire_schema::SchemaExceptions {
        kafka_wire_schema::SchemaExceptions::new(
            self.accepted
                .iter()
                .map(|entry| kafka_wire_schema::SchemaException {
                    message: entry.message.clone(),
                    field: entry.field.clone(),
                    code: entry.code.clone(),
                    reason: entry.reason.clone(),
                    upstream: entry.upstream.clone(),
                })
                .collect(),
        )
    }
}

fn validate_entry(
    path: &Path,
    lock: &ProtocolLock,
    locked: &BTreeSet<&str>,
    entry: &AcceptedDefect,
) -> Result<(), GenerationError> {
    if entry.message.trim().is_empty()
        || entry.code.trim().is_empty()
        || entry.reason.trim().is_empty()
        || entry.upstream.trim().is_empty()
        || entry
            .field
            .as_ref()
            .is_some_and(|field| field.trim().is_empty())
    {
        return invalid(
            path,
            "schema exceptions require nonempty identity and reason fields",
        );
    }
    let filename = format!("{}.json", entry.message);
    if !locked.contains(filename.as_str()) {
        return invalid(
            path,
            format!("schema exception message `{}` is not pinned", entry.message),
        );
    }
    let expected = format!("{}/{}", lock.kafka.upstream_message_root, filename);
    if entry.upstream != expected {
        return invalid(
            path,
            format!(
                "schema exception {} source must be `{expected}`, found `{}`",
                entry.message, entry.upstream
            ),
        );
    }
    Ok(())
}
