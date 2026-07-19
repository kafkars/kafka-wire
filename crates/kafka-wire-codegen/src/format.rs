//! Layout authority handed to the pinned toolchain's `rustfmt`.
//!
//! This module owns the single point where rendered Rust text becomes formatted
//! Rust text: it resolves the formatter binary, feeds one rendered file over
//! stdin, and returns what `rustfmt` emits. Running here — before the manifest
//! is hashed — is what makes `cargo fmt --all --check` and the generated tree
//! agree by construction instead of by coincidence.
//!
//! It deliberately does not decide what Rust to emit, does not touch the
//! filesystem, and does not know which protocol files are being generated.

// The guarded capability token is spelled `std::process` on purpose. Folding
// this import into the group below would hide the spawn from the capability
// guard rather than declare it, which is how a boundary rots.
use std::process::{Command, Output, Stdio};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    io::{self, Write},
    path::{Path, PathBuf},
    thread,
};

use crate::GenerationError;

/// Environment variable naming an explicit formatter binary, as `cargo fmt` reads it.
const PROGRAM_OVERRIDE: &str = "RUSTFMT";

/// Formatter resolved through the toolchain pinned by `rust-toolchain.toml`.
const DEFAULT_PROGRAM: &str = "rustfmt";

/// Edition the generated tree is parsed and laid out under.
const EDITION: &str = "2024";

/// Formats every rendered Rust file, preserving the caller's path ordering.
///
/// The caller must pass rendered Rust only. Layout is not a generator decision,
/// so the manifest is rendered from the values returned here, never from the
/// emitter's own line breaks.
pub(crate) fn format_rendered_rust(
    files: BTreeMap<String, String>,
    workspace_root: &Path,
) -> Result<BTreeMap<String, String>, GenerationError> {
    let formatter = RustFormatter::pinned(workspace_root);

    files
        .into_iter()
        .map(|(path, rendered)| {
            let formatted = formatter.format(&path, &rendered)?;
            Ok((path, formatted))
        })
        .collect()
}

/// One configured `rustfmt` invocation target.
#[derive(Debug)]
struct RustFormatter {
    program: OsString,
    workspace_root: PathBuf,
}

impl RustFormatter {
    /// Resolves the formatter of the toolchain pinned at the workspace root.
    ///
    /// Running from the workspace root is what pins the toolchain: the rustup
    /// shim reads `rust-toolchain.toml` from the working directory, and
    /// `rustfmt` reads `rustfmt.toml` from the same place. An explicit
    /// `RUSTFMT` binary overrides both for environments without rustup.
    fn pinned(workspace_root: &Path) -> Self {
        let program = env::var_os(PROGRAM_OVERRIDE)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from(DEFAULT_PROGRAM));

        Self {
            program,
            workspace_root: workspace_root.to_path_buf(),
        }
    }

    /// Returns the formatted form of one rendered generated file.
    fn format(&self, path: &str, rendered: &str) -> Result<String, GenerationError> {
        let output = self
            .run(rendered)
            .map_err(|source| self.unavailable(source))?;

        if !output.status.success() {
            let diagnostics = String::from_utf8_lossy(&output.stderr);
            return Err(GenerationError::Formatter {
                path: path.to_owned(),
                details: format!("{}\n{}", output.status, diagnostics.trim_end()),
            });
        }

        String::from_utf8(output.stdout).map_err(|_| GenerationError::Formatter {
            path: path.to_owned(),
            details: "rustfmt returned bytes that are not valid UTF-8".to_owned(),
        })
    }

    /// Reports a formatter that could not be launched at all.
    fn unavailable(&self, source: io::Error) -> GenerationError {
        GenerationError::FormatterUnavailable {
            program: self.program.to_string_lossy().into_owned(),
            source,
        }
    }

    /// Exchanges one file with `rustfmt` over pipes.
    ///
    /// Stdin is written from a scoped thread while the parent drains stdout, so
    /// a generated file larger than one pipe buffer cannot deadlock the
    /// exchange the way a write-then-read sequence eventually would.
    fn run(&self, rendered: &str) -> io::Result<Output> {
        let mut child = Command::new(&self.program)
            .args(["--edition", EDITION, "--emit", "stdout", "--quiet"])
            .current_dir(&self.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take();
        thread::scope(|scope| {
            let feeder = scope.spawn(move || match stdin {
                Some(mut stdin) => stdin.write_all(rendered.as_bytes()),
                None => Ok(()),
            });
            let output = child.wait_with_output();

            match feeder.join() {
                Ok(Ok(())) => output,
                // A closed input pipe means rustfmt stopped reading early; its
                // exit status and diagnostics explain that better than the write.
                Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => output,
                Ok(Err(error)) => Err(error),
                Err(_) => Err(io::Error::other("rustfmt input thread panicked")),
            }
        })
    }
}
