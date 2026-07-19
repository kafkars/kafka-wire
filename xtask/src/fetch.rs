//! The repository's only network capability, spelled as one explicit tool call.
//!
//! This module owns fetching bytes over HTTPS by delegating to `curl`, and it is
//! reachable from exactly one place: the human-invoked `cargo xtask vendor`
//! command. Nothing on the build, test, or generation path may call it, which is
//! what keeps `cargo build` and `cargo test` offline.
//!
//! It deliberately owns no knowledge of Kafka, of GitHub response shapes, or of
//! where fetched bytes are written. It returns exactly what the server sent.

use std::process::Command as Process;

/// Response shape requested from the remote host.
///
/// The listing and the file bytes are different kinds of request, so the caller
/// names which one it is making rather than passing an opaque header string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Accept {
    /// A GitHub REST API document.
    GithubJson,
    /// Verbatim file contents, preserved byte for byte.
    RawBytes,
}

impl Accept {
    const fn header(self) -> &'static str {
        match self {
            Self::GithubJson => "Accept: application/vnd.github+json",
            Self::RawBytes => "Accept: application/vnd.github.raw",
        }
    }
}

/// Retrieves one URL, returning the response body or a diagnostic naming the URL.
///
/// `--fail` is what makes an HTTP error an error here: without it curl reports
/// success and prints the error page, which would be vendored as if it were a
/// protocol schema.
pub(crate) fn get(url: &str, accept: Accept) -> Result<Vec<u8>, String> {
    let output = Process::new("curl")
        .args([
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            "--retry",
            "3",
            "--retry-delay",
            "1",
            "--max-time",
            "60",
            "--header",
            accept.header(),
            url,
        ])
        .output()
        .map_err(|error| format!("could not launch curl for {url}: {error}"))?;

    if output.status.success() {
        return Ok(output.stdout);
    }

    let detail = String::from_utf8_lossy(&output.stderr);
    let detail = detail.trim();
    if detail.is_empty() {
        Err(format!("fetching {url} failed with {}", output.status))
    } else {
        Err(format!("fetching {url} failed: {detail}"))
    }
}
