//! Pre-install verification for a staged vendored corpus and lockfile.
//!
//! This module proves the staged bytes reproduce the fetched inputs and that
//! the validated lock names and hashes exactly that corpus. It deliberately
//! does not own fetching, filesystem replacement, or rollback.

use std::{collections::BTreeMap, fs, path::Path};

use crate::protocol_lock::{ProtocolLock, digest};

pub(crate) fn verify_staged(
    corpus_staging: &Path,
    lock_staging: &Path,
    corpus: &BTreeMap<String, Vec<u8>>,
    lock_document: &[u8],
) -> Result<(), String> {
    let found = read_corpus(corpus_staging)?;
    if &found != corpus {
        return Err("staged vendor corpus does not reproduce every fetched byte".to_owned());
    }

    let staged_lock = fs::read(lock_staging).map_err(|error| io_error(lock_staging, error))?;
    if staged_lock != lock_document {
        return Err("staged protocol.lock does not reproduce the rendered bytes".to_owned());
    }
    let lock = ProtocolLock::read(lock_staging)
        .map_err(|error| format!("staged protocol.lock failed validation: {error}"))?;
    if lock.kafka.files.len() != found.len() {
        return Err("staged protocol.lock does not name the complete corpus".to_owned());
    }
    for file in &lock.kafka.files {
        let bytes = found
            .get(file.path.as_str())
            .ok_or_else(|| format!("staged protocol.lock names a missing file: {}", file.path))?;
        if digest(bytes) != file.sha256 {
            return Err(format!(
                "staged protocol.lock digest does not match {}",
                file.path
            ));
        }
    }
    Ok(())
}

fn read_corpus(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let entries = fs::read_dir(root).map_err(|error| io_error(root, error))?;
    let mut found = BTreeMap::new();
    for entry in entries {
        let path = entry.map_err(|error| io_error(root, error))?.path();
        if !path.is_file() {
            return Err(format!(
                "staged vendor entry is not a file: {}",
                path.display()
            ));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("staged vendor filename is not UTF-8: {}", path.display()))?;
        let bytes = fs::read(&path).map_err(|error| io_error(&path, error))?;
        found.insert(name.to_owned(), bytes);
    }
    Ok(found)
}

fn io_error(path: &Path, error: std::io::Error) -> String {
    let report = format!(
        "filesystem operation failed for {}: {error}",
        path.display()
    );
    drop(error);
    report
}
