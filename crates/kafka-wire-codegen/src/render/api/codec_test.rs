//! Compiler-owned decode-local collision scenarios.
//!
//! These tests use distinct wire fields whose normalized names collide with
//! the old emitter suffix policy. Each field must retain a unique positional
//! binding through construction; merely parsing the generated Rust would miss
//! the legal-shadowing failure this scenario guards.

use std::path::PathBuf;

use kafka_wire_schema::{SourceFile, lower_message, parse_jsonc};

use crate::render::text::RustText;

use super::codec::render_decode;

#[test]
fn sibling_names_cannot_alias_compiler_owned_decode_locals() {
    let file = SourceFile::new(
        "LocalCollisionRequest.json",
        r#"{
          "apiKey": 1,
          "type": "request",
          "name": "LocalCollisionRequest",
          "validVersions": "0",
          "flexibleVersions": "none",
          "fields": [
            { "name": "Version", "type": "int32", "versions": "0" },
            { "name": "VersionValue", "type": "int32", "versions": "0" }
          ]
        }"#,
    );
    let raw = parse_jsonc(&file).unwrap_or_else(|error| panic!("parse fixture: {error}"));
    let message = lower_message(raw, PathBuf::from(file.path()))
        .unwrap_or_else(|error| panic!("lower fixture: {error}"));
    let mut rust = RustText::default();
    render_decode(&mut rust, &message).unwrap_or_else(|error| panic!("render fixture: {error}"));
    let rendered = rust.finish();

    assert!(rendered.contains("let __kw_field_0 = decoder.read_i32()?;"));
    assert!(rendered.contains("let __kw_field_1 = decoder.read_i32()?;"));
    assert!(rendered.contains("version: __kw_field_0,"));
    assert!(rendered.contains("version_value: __kw_field_1,"));
    assert!(!rendered.contains("let version_value ="));
}
