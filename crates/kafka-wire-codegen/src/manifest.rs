//! Content-addressed generated-tree manifest.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{
    GenerationError,
    provenance::{GENERATOR, SemanticInputDigests},
    source::hex_digest,
};

/// JSON manifest checked beside generated Rust files.
#[derive(Debug, Serialize)]
pub(crate) struct GeneratedManifest {
    schema: u32,
    generator: &'static str,
    ir_version: u32,
    upstream_repository: String,
    upstream_commit: String,
    semantic_inputs_sha256: String,
    protocol_lock_sha256: String,
    headers_override_sha256: String,
    schema_exceptions_sha256: String,
    compiler_source_sha256: String,
    rustfmt_sha256: String,
    rustfmt_identity: String,
    files: Vec<GeneratedFile>,
}

/// One generated file digest.
#[derive(Debug, Serialize)]
struct GeneratedFile {
    path: String,
    sha256: String,
}

pub(crate) fn render_manifest(
    files: &BTreeMap<String, String>,
    ir_version: u32,
    repository: &str,
    commit: &str,
    inputs: SemanticInputDigests,
) -> Result<String, GenerationError> {
    let manifest = GeneratedManifest {
        schema: 1,
        generator: GENERATOR,
        ir_version,
        upstream_repository: repository.to_owned(),
        upstream_commit: commit.to_owned(),
        semantic_inputs_sha256: inputs.aggregate_sha256,
        protocol_lock_sha256: inputs.protocol_lock_sha256,
        headers_override_sha256: inputs.headers_override_sha256,
        schema_exceptions_sha256: inputs.schema_exceptions_sha256,
        compiler_source_sha256: inputs.compiler_source_sha256,
        rustfmt_sha256: inputs.rustfmt_sha256,
        rustfmt_identity: inputs.rustfmt_identity,
        files: files
            .iter()
            .map(|(path, source)| GeneratedFile {
                path: path.clone(),
                sha256: hex_digest(source.as_bytes()),
            })
            .collect(),
    };
    let mut json = serde_json::to_string_pretty(&manifest)?;
    json.push('\n');
    Ok(json)
}
