//! Construction of a generated message from its canonical Kafka JSON value.
//!
//! This module owns the field-by-field mapping from the JSON that Kafka's own
//! `<Message>DataJsonConverter` consumed onto the generated Rust struct. That
//! direction is why the corpus carries a JSON value at all: a suite that only
//! decoded and re-encoded bytes would never construct a message, so a wrong
//! default, a misnamed field, or a missing version gate would never be exercised.
//!
//! Unknown JSON keys are refused rather than ignored. A silently skipped key
//! leaves its field at a default, and the vector would still pass — the exact
//! self-consistent blindness the corpus exists to remove.
//!
//! It deliberately owns no dispatch and no wire access; `subject` chooses the
//! type and performs the encoding.

use std::collections::BTreeMap;

use bytes::Bytes;
use kafka_wire::{ApiVersionsRequest, SaslHandshakeRequest, SaslHandshakeResponse};
use kafka_wire_core::{StrBytes, TaggedField, TaggedFields};

use crate::corpus::{self, Vector};

pub(crate) fn api_versions_request(
    fields: &mut Fields<'_>,
    vector: &Vector,
) -> Result<ApiVersionsRequest, String> {
    let mut request = ApiVersionsRequest::default();

    if let Some(value) = fields.take("clientSoftwareName") {
        request.client_software_name = as_string(value, "clientSoftwareName")?;
    }
    if let Some(value) = fields.take("clientSoftwareVersion") {
        request.client_software_version = as_string(value, "clientSoftwareVersion")?;
    }
    if let Some(value) = fields.take("clusterId") {
        request.cluster_id = as_nullable_string(value, "clusterId")?;
    }
    if let Some(value) = fields.take("nodeId") {
        request.node_id = as_i32(value, "nodeId")?;
    }
    request.unknown_tagged_fields = tagged_fields(vector)?;

    Ok(request)
}

pub(crate) fn sasl_handshake_request(
    fields: &mut Fields<'_>,
) -> Result<SaslHandshakeRequest, String> {
    let mut request = SaslHandshakeRequest::default();
    if let Some(value) = fields.take("mechanism") {
        request.mechanism = as_string(value, "mechanism")?;
    }
    Ok(request)
}

pub(crate) fn sasl_handshake_response(
    fields: &mut Fields<'_>,
) -> Result<SaslHandshakeResponse, String> {
    let mut response = SaslHandshakeResponse::default();

    if let Some(value) = fields.take("errorCode") {
        response.error_code = as_i16(value, "errorCode")?;
    }
    if let Some(value) = fields.take("mechanisms") {
        let elements = value
            .as_array()
            .ok_or_else(|| "`mechanisms` must be an array".to_owned())?;
        response.mechanisms = elements
            .iter()
            .map(|element| as_string(element, "mechanisms element"))
            .collect::<Result<Vec<_>, _>>()?;
    }

    Ok(response)
}

/// Rebuild the unknown tagged fields a vector carries beside its JSON value.
///
/// No generated JSON converter can express these, so the corpus keeps them
/// alongside the value and both sides attach them after conversion.
fn tagged_fields(vector: &Vector) -> Result<TaggedFields, String> {
    let fields = vector
        .unknown_tagged_fields
        .iter()
        .map(|field| {
            corpus::from_hex(&field.data_hex)
                .map(|data| TaggedField::new(field.tag, Bytes::from(data)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    TaggedFields::from_sorted(fields).map_err(|error| error.to_string())
}

/// One vector's JSON object, tracking which keys a field has claimed.
pub(crate) struct Fields<'a> {
    at: &'a str,
    entries: BTreeMap<&'a str, &'a serde_json::Value>,
}

impl<'a> Fields<'a> {
    pub(crate) fn new(at: &'a str, value: &'a serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{at}: json_value must be an object"))?;
        Ok(Self {
            at,
            entries: object
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect(),
        })
    }

    fn take(&mut self, key: &str) -> Option<&'a serde_json::Value> {
        self.entries.remove(key)
    }

    /// Refuse any key no field claimed, rather than defaulting past it.
    pub(crate) fn finish(self) -> Result<(), String> {
        if self.entries.is_empty() {
            return Ok(());
        }
        Err(format!(
            "{}: json_value carries unmapped field(s) {:?}; a skipped key would leave \
             its field at a default and the vector would still pass",
            self.at,
            self.entries.keys().collect::<Vec<_>>()
        ))
    }
}

fn as_string(value: &serde_json::Value, field: &str) -> Result<StrBytes, String> {
    value
        .as_str()
        .map(StrBytes::from)
        .ok_or_else(|| format!("`{field}` must be a string"))
}

fn as_nullable_string(value: &serde_json::Value, field: &str) -> Result<Option<StrBytes>, String> {
    if value.is_null() {
        return Ok(None);
    }
    as_string(value, field).map(Some)
}

fn as_i32(value: &serde_json::Value, field: &str) -> Result<i32, String> {
    let number = value
        .as_i64()
        .ok_or_else(|| format!("`{field}` must be an integer"))?;
    i32::try_from(number).map_err(|_| format!("`{field}` value {number} does not fit an int32"))
}

fn as_i16(value: &serde_json::Value, field: &str) -> Result<i16, String> {
    let number = value
        .as_i64()
        .ok_or_else(|| format!("`{field}` must be an integer"))?;
    i16::try_from(number).map_err(|_| format!("`{field}` value {number} does not fit an int16"))
}
