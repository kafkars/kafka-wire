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
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::{
    fetch::{self, Accept},
    protocol_lock::{ProtocolLock, SourceStatus, VendoredFile, digest},
};

/// What one vendoring run changed on disk.
#[derive(Clone, Debug, Default)]
pub(crate) struct VendorReport {
    pub(crate) vendored: usize,
    pub(crate) enabled: usize,
    pub(crate) pending: usize,
    pub(crate) removed: Vec<String>,
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
    let mut lock = ProtocolLock::read(&lock_path)?;
    let slug = repository_slug(&lock.kafka.repository)?;

    let filenames = discover(&slug, &lock.kafka.commit, &lock.kafka.upstream_message_root)?;
    let recorded = lock.recorded_statuses();
    let destination = vendored_message_root(workspace, &lock)?;
    fs::create_dir_all(&destination)
        .map_err(|error| format!("could not create {}: {error}", destination.display()))?;

    let mut files = Vec::with_capacity(filenames.len());
    for filename in &filenames {
        let url = format!(
            "https://raw.githubusercontent.com/{slug}/{}/{}/{filename}",
            lock.kafka.commit, lock.kafka.upstream_message_root
        );
        let bytes = fetch::get(&url, Accept::RawBytes)?;
        let path = destination.join(filename);
        fs::write(&path, &bytes)
            .map_err(|error| format!("could not write {}: {error}", path.display()))?;
        files.push(VendoredFile {
            sha256: digest(&bytes),
            status: recorded
                .get(filename.as_str())
                .copied()
                .unwrap_or(SourceStatus::Pending),
            path: filename.clone(),
        });
    }

    let removed = prune(&destination, &filenames)?;
    let enabled = files
        .iter()
        .filter(|file| file.status == SourceStatus::Enabled)
        .count();

    lock.kafka.files = files;
    lock.write(&lock_path)?;

    Ok(VendorReport {
        vendored: filenames.len(),
        enabled,
        pending: filenames.len() - enabled,
        removed,
    })
}

/// Sorted message filenames present in the pinned commit's message tree.
fn discover(slug: &str, commit: &str, message_root: &str) -> Result<Vec<String>, String> {
    let url = format!(
        "https://api.github.com/repos/{slug}/git/trees/{commit}:{}",
        message_root.replace('/', "%2F")
    );
    let body = fetch::get(&url, Accept::GithubJson)?;
    let listing: TreeListing = serde_json::from_slice(&body)
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
    for entry in listing.tree {
        if entry.kind != "blob" || !is_schema_file(&entry.path) {
            continue;
        }
        filenames.push(plain_filename(&entry.path)?);
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

/// Removes vendored schema files the pinned commit no longer contains.
fn prune(destination: &Path, expected: &[String]) -> Result<Vec<String>, String> {
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
        let name = name.to_owned();
        fs::remove_file(&path)
            .map_err(|error| format!("could not remove {}: {error}", path.display()))?;
        removed.push(name);
    }

    removed.sort();
    Ok(removed)
}

/// Whether a listing or directory entry names a JSON schema definition.
fn is_schema_file(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
}

/// Directory mirroring the pinned commit's message tree, named as upstream names it.
fn vendored_message_root(workspace: &Path, lock: &ProtocolLock) -> Result<PathBuf, String> {
    let directory = Path::new(&lock.kafka.upstream_message_root)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "kafka.upstream_message_root has no directory name: {}",
                lock.kafka.upstream_message_root
            )
        })?;
    Ok(workspace
        .join(&lock.kafka.vendored_root)
        .join(&lock.kafka.commit)
        .join(directory))
}

/// `owner/repo` for the pinned upstream repository URL.
fn repository_slug(repository: &str) -> Result<String, String> {
    let slug = repository
        .trim_end_matches('/')
        .strip_prefix("https://github.com/")
        .ok_or_else(|| format!("kafka.repository is not a GitHub URL: {repository}"))?;
    if slug.split('/').filter(|part| !part.is_empty()).count() == 2 {
        Ok(slug.to_owned())
    } else {
        Err(format!("kafka.repository is not owner/repo: {repository}"))
    }
}

/// Accepts a listing entry only if it is one ordinary, quotable filename.
///
/// The name becomes both a path component and a TOML string, so a separator, a
/// traversal segment, or a quote is rejected here rather than escaped later.
fn plain_filename(candidate: &str) -> Result<String, String> {
    let ordinary = !candidate.is_empty()
        && candidate != "."
        && candidate != ".."
        && Path::new(candidate)
            .file_name()
            .and_then(|name| name.to_str())
            == Some(candidate)
        && candidate
            .chars()
            .all(|character| character.is_ascii_graphic() && character != '"' && character != '\\');
    if ordinary {
        Ok(candidate.to_owned())
    } else {
        Err(format!(
            "upstream listed an unusable schema filename: {candidate}"
        ))
    }
}
