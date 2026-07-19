//! Raw message-level source fields.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::RawField;

/// Message direction as spelled by Apache Kafka source files.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RawMessageKind {
    /// Client-to-server message.
    Request,
    /// Server-to-client message.
    Response,
}

/// Deserialized Kafka message definition before semantic interpretation.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawMessage {
    /// Numeric Kafka API key.
    pub api_key: Option<i16>,
    /// Request or response direction.
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
    pub flexible_versions: String,
    /// Ordered root fields.
    #[serde(default)]
    pub fields: Vec<RawField>,
    /// Properties not yet modeled by this adapter.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
