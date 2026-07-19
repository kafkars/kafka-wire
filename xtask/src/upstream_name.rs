//! Which upstream names this repository will accept as vendorable.
//!
//! This module owns one judgement, made before anything is fetched or written:
//! is a repository URL, or a name upstream listed, usable here at all. Each
//! accepted name goes on to become a path component under the vendored commit
//! directory and a quoted string in `spec/protocol.lock`, so a separator, a
//! traversal segment, or a quote is refused at intake rather than escaped at
//! every later use.
//!
//! It deliberately owns no I/O and no lockfile policy. Nothing here knows what
//! the corpus is for.

use std::path::Path;

/// Whether a listing or directory entry names a JSON schema definition.
pub(crate) fn is_schema_file(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
}

/// `owner/repo` for the pinned upstream repository URL.
pub(crate) fn repository_slug(repository: &str) -> Result<String, String> {
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
pub(crate) fn plain_filename(candidate: &str) -> Result<String, String> {
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
