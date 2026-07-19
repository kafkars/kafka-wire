//! Raw message-level struct declarations.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::RawField;

/// Deserialized `commonStructs` entry before semantic interpretation.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawCommonStruct {
    /// Struct name referred to by fields of this message.
    pub name: String,
    /// Version expression for the declaration.
    pub versions: String,
    /// Ordered struct fields.
    #[serde(default)]
    pub fields: Vec<RawField>,
    /// Properties not yet modeled by this adapter.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
