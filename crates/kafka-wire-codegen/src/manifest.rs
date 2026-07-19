//! Content-addressed generated-tree manifest.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{GenerationError, provenance::GENERATOR, source::hex_digest};

/// JSON manifest checked beside generated Rust files.
#[derive(Debug, Serialize)]
pub(crate) struct GeneratedManifest {
    schema: u32,
    generator: &'static str,
    ir_version: u32,
    upstream_repository: String,
    upstream_commit: String,
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
) -> Result<String, GenerationError> {
    let manifest = GeneratedManifest {
        schema: 1,
        generator: GENERATOR,
        ir_version,
        upstream_repository: repository.to_owned(),
        upstream_commit: commit.to_owned(),
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
