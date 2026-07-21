//! Ownership proof for an existing generated-tree destination.
//!
//! This module deliberately decides only whether replacement is authorized;
//! staging, comparison, and filesystem replacement remain in `output.rs`.

use std::{fs, path::Path};

use crate::GenerationError;

#[derive(serde::Deserialize)]
struct OwnershipManifest {
    schema: u32,
    generator: String,
}

pub(crate) fn verify_output_ownership(root: &Path) -> Result<(), GenerationError> {
    if !root.exists() {
        return Ok(());
    }
    if !root.is_dir() {
        return Err(unowned(root, "destination is not a directory"));
    }

    let source = fs::read_to_string(root.join("MANIFEST.json"))
        .map_err(|error| unowned(root, format!("cannot read MANIFEST.json: {error}")))?;
    let manifest: OwnershipManifest = serde_json::from_str(&source)
        .map_err(|error| unowned(root, format!("invalid MANIFEST.json: {error}")))?;
    if manifest.schema != 1 {
        return Err(unowned(
            root,
            format!("unsupported MANIFEST.json schema {}", manifest.schema),
        ));
    }
    let Some(version) = manifest.generator.strip_prefix("kafka-wire-codegen ") else {
        return Err(unowned(root, "MANIFEST.json names another generator"));
    };
    if version.is_empty() {
        return Err(unowned(root, "MANIFEST.json omits the generator version"));
    }
    Ok(())
}

fn unowned(path: &Path, reason: impl Into<String>) -> GenerationError {
    GenerationError::UnownedOutputTree {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}
