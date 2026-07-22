//! Response-header exception schema and API relationship validation.
//!
//! This module deliberately does not read arbitrary paths or render policy.

use std::{path::Path, str::FromStr};

use kafka_wire_schema::{MessageKind, VersionSet};
use serde::Deserialize;

use crate::{GenerationError, group::ApiGroup, lockfile::ProtocolLock};

use super::{decode_override, invalid, read_override, require_schema};

/// Every reviewed header-version exception.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HeaderOverrides {
    schema: u32,
    #[serde(default)]
    pub(crate) response_header_exceptions: Vec<ResponseHeaderException>,
    #[serde(skip)]
    input_bytes: Vec<u8>,
}

/// One API whose response header version departs from the usual rule.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResponseHeaderException {
    pub(crate) api_key: i16,
    #[serde(deserialize_with = "version_set")]
    versions: VersionSet,
    pub(crate) header_version: i16,
    pub(crate) reason: String,
    source: String,
}

impl ResponseHeaderException {
    /// Inclusive first version of the validated open-ended range.
    pub(crate) fn first_version(&self) -> i16 {
        self.versions.ranges()[0].start()
    }
}

impl HeaderOverrides {
    /// Reads and validates `headers.toml` against generated APIs.
    pub(crate) fn read(
        workspace_root: &Path,
        lock: &ProtocolLock,
        groups: &[ApiGroup],
        unkeyed: &[crate::source::MessageSource],
    ) -> Result<Self, GenerationError> {
        let (path, source) = read_override(workspace_root, "headers.toml")?;
        let mut overrides: Self = decode_override(&path, &source)?;
        overrides.input_bytes = source;
        overrides.validate(&path, lock, groups, unkeyed)?;
        Ok(overrides)
    }

    /// Exact validated document bytes used to construct these overrides.
    pub(crate) fn input_bytes(&self) -> &[u8] {
        &self.input_bytes
    }

    fn validate(
        &self,
        path: &Path,
        lock: &ProtocolLock,
        groups: &[ApiGroup],
        unkeyed: &[crate::source::MessageSource],
    ) -> Result<(), GenerationError> {
        require_schema(path, self.schema)?;
        let mut seen: Vec<&ResponseHeaderException> = Vec::new();
        for exception in &self.response_header_exceptions {
            let ranges = exception.versions.ranges();
            if !matches!(ranges, [range] if range.end().is_none()) {
                return invalid(
                    path,
                    format!(
                        "api key {} versions must be exactly one open-ended range, found `{}`",
                        exception.api_key, exception.versions
                    ),
                );
            }
            if exception.header_version < 0 {
                return invalid(
                    path,
                    format!(
                        "api key {} has negative header_version {}",
                        exception.api_key, exception.header_version
                    ),
                );
            }
            let response_header = response_header(unkeyed)?;
            if !response_header
                .message
                .valid_versions
                .contains(exception.header_version)
            {
                return invalid(
                    path,
                    format!(
                        "api key {} header_version {} is outside ResponseHeader versions `{}`",
                        exception.api_key,
                        exception.header_version,
                        response_header.message.valid_versions,
                    ),
                );
            }
            if exception.reason.trim().is_empty() {
                return invalid(path, format!("api key {} has no reason", exception.api_key));
            }
            let group = groups
                .iter()
                .find(|group| group.api_key == exception.api_key)
                .ok_or_else(|| GenerationError::InvalidOverride {
                    path: path.to_path_buf(),
                    reason: format!(
                        "api key {} does not exist in the pinned corpus",
                        exception.api_key
                    ),
                })?;
            let response = &group.response;
            if exception
                .versions
                .intersection(&response.message.valid_versions)
                .is_empty()
            {
                return invalid(
                    path,
                    format!(
                        "api key {} exception versions `{}` do not intersect response versions `{}`",
                        exception.api_key, exception.versions, response.message.valid_versions
                    ),
                );
            }
            validate_source(path, lock, response, exception)?;
            if seen.iter().any(|previous| {
                previous.api_key == exception.api_key
                    && !previous
                        .versions
                        .intersection(&exception.versions)
                        .is_empty()
            }) {
                return invalid(
                    path,
                    format!("api key {} has overlapping exceptions", exception.api_key),
                );
            }
            seen.push(exception);
        }
        Ok(())
    }
}

fn response_header(
    unkeyed: &[crate::source::MessageSource],
) -> Result<&crate::source::MessageSource, GenerationError> {
    unkeyed
        .iter()
        .find(|source| {
            source.message.kind == MessageKind::Header
                && source.message.name.protocol() == "ResponseHeader"
        })
        .ok_or_else(|| GenerationError::InternalInvariant {
            message: "ResponseHeader".to_owned(),
            invariant: "a header override requires the ResponseHeader schema".to_owned(),
        })
}

fn validate_source(
    path: &Path,
    lock: &ProtocolLock,
    response: &crate::source::MessageSource,
    exception: &ResponseHeaderException,
) -> Result<(), GenerationError> {
    let expected = format!("{}/{}", lock.kafka.upstream_message_root, response.filename);
    if exception.source == expected {
        Ok(())
    } else {
        invalid(
            path,
            format!(
                "api key {} source must be `{expected}`, found `{}`",
                exception.api_key, exception.source
            ),
        )
    }
}

fn version_set<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<VersionSet, D::Error> {
    let raw = String::deserialize(deserializer)?;
    VersionSet::from_str(&raw).map_err(serde::de::Error::custom)
}
