//! Raw message-level source fields.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::{RawCommonStruct, RawField};

/// Schema kind as spelled by Apache Kafka source files.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RawMessageKind {
    /// Client-to-server message.
    Request,
    /// Server-to-client message.
    Response,
    /// Request or response frame header.
    Header,
    /// Structure carried inside records rather than sent on its own.
    Data,
}

/// Deserialized Kafka message definition before semantic interpretation.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawMessage {
    /// Numeric Kafka API key, absent for header and data schemas.
    #[serde(default)]
    pub api_key: Option<i16>,
    /// Schema kind.
    #[serde(rename = "type")]
    pub kind: RawMessageKind,
    /// Listener scopes declared by upstream.
    #[serde(default)]
    pub listeners: Vec<String>,
    /// Protocol message name.
    pub name: String,
    /// Supported version expression.
    pub valid_versions: String,
    /// Flexible version expression.
    ///
    /// Absent in the eight pre-flexible schemas that Apache Kafka 4.0 retired
    /// (`ControlledShutdown`, `LeaderAndIsr`, `StopReplica`, `UpdateMetadata`),
    /// which predate the tagged-field encoding entirely and so are never
    /// flexible at any version.
    #[serde(default)]
    pub flexible_versions: Option<String>,
    /// Whether the highest valid version is still subject to change.
    #[serde(default)]
    pub latest_version_unstable: bool,
    /// Structs declared once and referred to by name within this message.
    #[serde(default)]
    pub common_structs: Vec<RawCommonStruct>,
    /// Ordered root fields.
    #[serde(default)]
    pub fields: Vec<RawField>,
    /// Properties not yet modeled by this adapter.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
