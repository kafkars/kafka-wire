//! Raw field-level source fields.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

/// Deserialized Kafka field definition before semantic interpretation.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawField {
    /// Protocol field name.
    pub name: String,
    /// Kafka source type spelling.
    #[serde(rename = "type")]
    pub field_type: String,
    /// Versions where the field exists.
    pub versions: String,
    /// Versions where null is accepted.
    #[serde(default)]
    pub nullable_versions: Option<String>,
    /// Versions where this is encoded as a tagged field.
    #[serde(default)]
    pub tagged_versions: Option<String>,
    /// Flexible-version tag number.
    #[serde(default)]
    pub tag: Option<u32>,
    /// Protocol default as written by upstream.
    #[serde(default)]
    pub default: Option<Value>,
    /// Whether older versions may silently omit a non-default value.
    #[serde(default)]
    pub ignorable: bool,
    /// Whether this field acts as an in-memory map key.
    #[serde(default)]
    pub map_key: bool,
    /// Human-facing field documentation.
    #[serde(default)]
    pub about: String,
    /// Inline struct fields for array and struct definitions.
    #[serde(default)]
    pub fields: Vec<RawField>,
    /// Properties not yet modeled by this adapter.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
