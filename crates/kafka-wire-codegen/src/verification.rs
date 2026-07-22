//! Adversarial generated source used by the scratch-crate behavioral probe.
//!
//! This file owns synthetic schemas that reproduce emitter-only failure shapes
//! absent from the pinned Kafka corpus. It does not affect production output.

use std::path::PathBuf;

use kafka_wire_schema::SourceFile;

use crate::{GenerationError, group::group_sources, render::render_api, source::MessageSource};

/// Renders a pair whose sibling field names once aliased one decode local.
pub fn render_adversarial_decode_fixture() -> Result<String, GenerationError> {
    let request = source(
        "AdversarialDecodeRequest.json",
        r#"{
            "apiKey": 1000,
            "type": "request",
            "name": "AdversarialDecodeRequest",
            "validVersions": "0",
            "flexibleVersions": "none",
            "fields": [
                { "name": "Version", "type": "int32", "versions": "0" },
                { "name": "VersionValue", "type": "int32", "versions": "0" }
            ]
        }"#,
    )?;
    let response = source(
        "AdversarialDecodeResponse.json",
        r#"{
            "apiKey": 1000,
            "type": "response",
            "name": "AdversarialDecodeResponse",
            "validVersions": "0",
            "flexibleVersions": "none",
            "fields": [
                { "name": "Results", "type": "[]Result", "versions": "0",
                  "fields": [
                    { "name": "Value", "type": "int32", "versions": "0" }
                  ] },
                { "name": "Options", "type": "[]Option", "versions": "0",
                  "fields": [
                    { "name": "Value", "type": "int32", "versions": "0" }
                  ] },
                { "name": "Vectors", "type": "[]Vec", "versions": "0",
                  "fields": [
                    { "name": "Value", "type": "int32", "versions": "0" }
                  ] },
                { "name": "Defaults", "type": "[]Default", "versions": "0",
                  "fields": [
                    { "name": "Value", "type": "int32", "versions": "0" }
                  ] },
                { "name": "ProtocolEqualities", "type": "[]ProtocolEq", "versions": "0",
                  "fields": [
                    { "name": "Value", "type": "int32", "versions": "0" }
                  ] }
            ]
        }"#,
    )?;
    let grouped = group_sources(vec![request, response])?;
    let group = grouped
        .api
        .first()
        .ok_or_else(|| GenerationError::InternalInvariant {
            message: "AdversarialDecodeRequest".to_owned(),
            invariant: "verification pair did not produce an API group".to_owned(),
        })?;
    render_api(group, "verification-fixture")
}

/// Renders nested float defaults that exercise recursive protocol semantics.
pub fn render_adversarial_defaults_fixture() -> Result<String, GenerationError> {
    let request = source(
        "AdversarialDefaultsRequest.json",
        r#"{
            "apiKey": 1001,
            "type": "request",
            "name": "AdversarialDefaultsRequest",
            "validVersions": "0-1",
            "flexibleVersions": "1+",
            "fields": [
                { "name": "Version", "type": "int32", "versions": "0+" },
                { "name": "VersionValue", "type": "int32", "versions": "0+" },
                { "name": "GatedNan", "type": "GatedNan", "versions": "1+",
                  "fields": [
                    { "name": "Value", "type": "float64", "versions": "1+",
                      "default": "NaN" }
                  ] },
                { "name": "GatedNegativeZero", "type": "GatedNegativeZero", "versions": "1+",
                  "fields": [
                    { "name": "Value", "type": "float64", "versions": "1+",
                      "default": "-0.0" }
                  ] },
                { "name": "TaggedNan", "type": "TaggedNan", "versions": "1+",
                  "taggedVersions": "1+", "tag": 0,
                  "fields": [
                    { "name": "Value", "type": "float64", "versions": "1+",
                      "default": "NaN" }
                  ] },
                { "name": "TaggedNegativeZero", "type": "TaggedNegativeZero", "versions": "1+",
                  "taggedVersions": "1+", "tag": 1,
                  "fields": [
                    { "name": "Value", "type": "float64", "versions": "1+",
                      "default": "-0.0" }
                  ] },
                { "name": "Deep", "type": "DeepOuter", "versions": "1+" }
            ],
            "commonStructs": [
                { "name": "DeepOuter", "versions": "1+", "fields": [
                    { "name": "Inner", "type": "DeepInner", "versions": "1+",
                      "fields": [
                        { "name": "Value", "type": "float64", "versions": "1+",
                          "default": "NaN" }
                      ] }
                ] }
            ]
        }"#,
    )?;
    let response = source(
        "AdversarialDefaultsResponse.json",
        r#"{
            "apiKey": 1001,
            "type": "response",
            "name": "AdversarialDefaultsResponse",
            "validVersions": "0-1",
            "flexibleVersions": "1+",
            "fields": []
        }"#,
    )?;
    let grouped = group_sources(vec![request, response])?;
    let group = grouped
        .api
        .first()
        .ok_or_else(|| GenerationError::InternalInvariant {
            message: "AdversarialDefaultsRequest".to_owned(),
            invariant: "verification pair did not produce an API group".to_owned(),
        })?;
    render_api(group, "verification-fixture")
}

fn source(filename: &str, schema: &str) -> Result<MessageSource, GenerationError> {
    let message = kafka_wire_schema::load_source(SourceFile::new(PathBuf::from(filename), schema))?;
    Ok(MessageSource {
        message,
        filename: filename.to_owned(),
        sha256: "verification-fixture".to_owned(),
    })
}
