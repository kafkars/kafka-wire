//! Explicit human-invoked intake of the pinned upstream message corpus.
//!
//! This module owns the vendoring pipeline: list the pinned commit's message
//! tree, fetch each file's exact upstream bytes, mirror them under the commit
//! directory, and re-record every digest in `spec/protocol.lock`. Upstream bytes
//! are stored verbatim, license headers included, so a vendored file is a copy
//! and never an interpretation.
//!
//! Vendoring deliberately owns no generation policy. A newly vendored file is
//! recorded as `pending`, and a file already marked `enabled` keeps that status;
//! deciding that the backend can compile a message is a separate reviewed edit.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde::Deserialize;

use crate::{
    fetch::{self, Accept},
    protocol_lock::{
        SourceStatus, VendoredFile, digest, read as read_lock, recorded_statuses,
        render as render_lock,
    },
    upstream_name::{is_schema_file, plain_filename, repository_slug},
    vendor_transaction::StagedVendor,
};

/// What one vendoring run changed on disk.
#[derive(Clone, Debug, Default)]
pub(crate) struct VendorReport {
    pub(crate) vendored: usize,
    pub(crate) enabled: usize,
    pub(crate) pending: usize,
    pub(crate) removed: Vec<String>,
    pub(crate) cleanup_warnings: Vec<String>,
}

/// One entry of a GitHub git-tree listing.
#[derive(Debug, Deserialize)]
struct TreeEntry {
    path: String,
    #[serde(rename = "type")]
    kind: String,
}

/// A GitHub git-tree listing for one directory of the pinned commit.
#[derive(Debug, Deserialize)]
struct TreeListing {
    /// GitHub sets this when the response omitted entries.
    #[serde(default)]
    truncated: bool,
    tree: Vec<TreeEntry>,
}

/// Refreshes the vendored corpus and lockfile from the pinned upstream commit.
pub(crate) fn vendor(workspace: &Path) -> Result<VendorReport, String> {
    let lock_path = workspace.join("spec").join("protocol.lock");
    let mut lock = read_lock(&lock_path)?;
    let slug = repository_slug(&lock.kafka.repository)?;

    let filenames = discover(
        &slug,
        &lock.kafka.commit,
        lock.kafka.upstream_message_root.as_str(),
    )?;
    let recorded = recorded_statuses(&lock);
    let destination = lock.kafka.vendored_message_root(workspace);

    let mut files = Vec::with_capacity(filenames.len());
    let mut corpus = BTreeMap::new();
    for filename in &filenames {
        let url = format!(
            "https://raw.githubusercontent.com/{slug}/{}/{}/{filename}",
            lock.kafka.commit, lock.kafka.upstream_message_root
        );
        let bytes = fetch::get(&url, Accept::RawBytes)?;
        files.push(relock(filename, &bytes, &recorded)?);
        if corpus.insert(filename.clone(), bytes).is_some() {
            return Err(format!("upstream listed duplicate schema file {filename}"));
        }
    }

    let removed = removed_files(&destination, &filenames)?;
    let enabled = files
        .iter()
        .filter(|file| file.status == SourceStatus::Enabled)
        .count();

    lock.kafka.files = files;
    let lock_document = render_lock(&lock);
    let cleanup_warnings =
        StagedVendor::new(&destination, &lock_path, &corpus, lock_document.as_bytes())?.commit()?;

    Ok(VendorReport {
        vendored: filenames.len(),
        enabled,
        pending: filenames.len() - enabled,
        removed,
        cleanup_warnings,
    })
}

/// Sorted message filenames present in the pinned commit's message tree.
fn discover(slug: &str, commit: &str, message_root: &str) -> Result<Vec<String>, String> {
    let url = format!(
        "https://api.github.com/repos/{slug}/git/trees/{commit}:{}",
        message_root.replace('/', "%2F")
    );
    let body = fetch::get(&url, Accept::GithubJson)?;
    schema_filenames(&body, commit)
}

/// Sorted schema filenames named by one git-tree listing.
///
/// Split from the request above so every judgement about a listing is decidable
/// without a network round trip, and therefore testable.
pub(crate) fn schema_filenames(listing: &[u8], commit: &str) -> Result<Vec<String>, String> {
    let listing: TreeListing = serde_json::from_slice(listing)
        .map_err(|error| format!("could not parse the message tree listing: {error}"))?;

    // A truncated listing would vendor a partial corpus and record it as if it
    // were complete, which is the one failure this command must never produce.
    if listing.truncated {
        return Err(format!(
            "GitHub truncated the message tree listing for {commit}; \
             the corpus cannot be vendored completely from this response"
        ));
    }

    let mut filenames = Vec::new();
    let mut folded_names = BTreeSet::new();
    for entry in listing.tree {
        if entry.kind != "blob" || !is_schema_file(&entry.path) {
            continue;
        }
        let filename = plain_filename(&entry.path)?;
        if !folded_names.insert(filename.to_ascii_lowercase()) {
            return Err(format!(
                "the message tree for {commit} listed colliding schema filename `{filename}`"
            ));
        }
        filenames.push(filename);
    }
    filenames.sort();

    if filenames.is_empty() {
        return Err(format!(
            "the message tree for {commit} listed no schema files; \
             refusing to record an empty corpus"
        ));
    }
    Ok(filenames)
}

/// Records one vendored file, carrying forward any status already reviewed.
///
/// A file upstream added since the last run has no recorded status and becomes
/// `pending`: vendoring never promotes a message into the compiled set.
pub(crate) fn relock(
    filename: &str,
    bytes: &[u8],
    recorded: &BTreeMap<&str, SourceStatus>,
) -> Result<VendoredFile, String> {
    Ok(VendoredFile {
        sha256: digest(bytes),
        status: recorded
            .get(filename)
            .copied()
            .unwrap_or(SourceStatus::Pending),
        path: crate::protocol_lock::PortableFilename::try_new(filename.to_owned())
            .map_err(|error| error.to_string())?,
    })
}

/// Reports schema files the complete staged replacement will remove.
fn removed_files(destination: &Path, expected: &[String]) -> Result<Vec<String>, String> {
    if !destination.exists() {
        return Ok(Vec::new());
    }
    let expected = expected.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let entries = fs::read_dir(destination)
        .map_err(|error| format!("could not read {}: {error}", destination.display()))?;
    let mut removed = Vec::new();

    for entry in entries {
        let entry =
            entry.map_err(|error| format!("could not read {}: {error}", destination.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !path.is_file() || !is_schema_file(name) || expected.contains(name) {
            continue;
        }
        removed.push(name.to_owned());
    }

    removed.sort();
    Ok(removed)
}
