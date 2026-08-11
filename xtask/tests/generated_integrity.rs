//! Generated Rust files are content-addressed and visibly compiler-owned.
//!
//! Scenario: read a generated manifest, then confirm every listed file exists,
//! carries its `@generated` provenance line, and hashes to the recorded digest,
//! with no unlisted Rust file beside them. A fixture whose output was edited
//! after generation must be rejected by hash.
//!
//! Two kinds of generated file exist and are distinguished by extension. A `.rs`
//! file is a module and opens with `//! @generated`. A `.rsi` file is a fragment
//! another file includes, and opens with `// @generated` because Rust refuses an
//! inner doc comment where an include expands. Both are hashed here; a fragment
//! left out of the manifest would be the crate's public surface going unhashed.

#![allow(clippy::unwrap_used)]

mod support;

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

use serde::Deserialize;
use support::{fixture_root, load_policy, sha256, workspace_root};

/// Compiler identity every generated manifest must carry.
///
/// The workspace shares one version, so reading this crate's own package
/// version tracks `kafka-wire-codegen`'s without hardcoding a number that a release
/// would leave stale. `xtask` cannot depend on the generator to ask
/// it directly; the dependency test forbids that edge.
const EXPECTED_GENERATOR: &str = concat!("kafka-wire-codegen ", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Deserialize)]
struct Manifest {
    schema: u32,
    generator: String,
    ir_version: u32,
    upstream_repository: String,
    upstream_commit: String,
    files: Vec<GeneratedFile>,
}

#[derive(Debug, Deserialize)]
struct GeneratedFile {
    path: String,
    sha256: String,
}

/// Provenance, hash, and completeness findings for one generated tree.
fn generated_violations(manifest_path: &Path) -> Vec<String> {
    let source = fs::read_to_string(manifest_path).unwrap_or_else(|error| {
        panic!(
            "read generated manifest {}: {error}",
            manifest_path.display()
        )
    });
    let manifest: Manifest = serde_json::from_str(&source).unwrap_or_else(|error| {
        panic!(
            "parse generated manifest {}: {error}",
            manifest_path.display()
        )
    });
    let generated_root = manifest_path.parent().unwrap_or(Path::new("."));
    let mut violations = Vec::new();

    validate_manifest_metadata(&manifest, &mut violations);
    let expected = manifest
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<BTreeSet<_>>();
    if expected.len() != manifest.files.len() {
        violations.push("generated manifest contains duplicate paths".to_owned());
    }

    for file in &manifest.files {
        let provenance = provenance_line(&file.path).filter(|_| is_plain_filename(&file.path));
        let Some(provenance) = provenance else {
            violations.push(format!(
                "generated manifest path must be one plain Rust filename: {}",
                file.path
            ));
            continue;
        };

        let path = generated_root.join(&file.path);
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("read generated file {}: {error}", path.display()));
        let text = String::from_utf8_lossy(&bytes);
        if !text.trim_start().starts_with(provenance) {
            violations.push(format!("{} must begin with `{provenance}`", path.display()));
        }
        let actual = sha256(&bytes);
        if actual != file.sha256 {
            violations.push(format!(
                "{} hash mismatch: manifest {}, actual {actual}",
                path.display(),
                file.sha256
            ));
        }
    }

    let actual = fs::read_dir(generated_root)
        .unwrap_or_else(|error| {
            panic!(
                "read generated directory {}: {error}",
                generated_root.display()
            )
        })
        .filter_map(Result::ok)
        .filter(|entry| provenance_line(&entry.file_name().to_string_lossy()).is_some())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<BTreeSet<_>>();
    let expected_owned = expected
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    for unexpected in actual.difference(&expected_owned) {
        violations.push(format!("unexpected generated Rust file: {unexpected}"));
    }
    for missing in expected_owned.difference(&actual) {
        violations.push(format!("missing generated Rust file: {missing}"));
    }

    violations
}

#[test]
fn generated_files_match_the_checked_in_manifest() {
    let workspace = workspace_root();
    let config = load_policy(&workspace);
    let violations = generated_violations(&workspace.join(&config.paths.generated_manifest));

    assert!(
        violations.is_empty(),
        "generated output violations:\n{}\nRun `cargo xtask generate`; do not patch output directly.",
        violations.join("\n")
    );
}

#[test]
fn a_generated_file_edited_after_generation_is_rejected() {
    let root = fixture_root("tampered_generated_output");
    let violations = generated_violations(&root.join("generated/MANIFEST.json"));

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("edited.rs") && violation.contains("hash mismatch")),
        "the generated detector accepted a hand-edited generated file: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("faithful.rs")),
        "the generated detector rejected an untouched generated file: {violations:?}"
    );
}

#[test]
fn an_unlisted_or_unprovenanced_generated_file_is_rejected() {
    let root = fixture_root("tampered_generated_output");
    let violations = generated_violations(&root.join("generated/MANIFEST.json"));

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("unexpected generated Rust file: smuggled.rs")),
        "the generated detector accepted a Rust file absent from the manifest: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("must begin with `//! @generated`")),
        "the generated detector accepted a generated file with no provenance line: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation
                .contains("unexpected generated Rust file: smuggled_fragment.rsi")),
        "the generated detector accepted an include fragment absent from the manifest, \
         so the crate's public surface could grow unhashed: {violations:?}"
    );
}

/// The `@generated` line a path of this kind must open with, if it is one.
///
/// This is also what decides which files beside the manifest must be listed in
/// it: an unlisted file of either kind is smuggled output, and one of neither
/// kind is not generated Rust at all.
fn provenance_line(path: &str) -> Option<&'static str> {
    match Path::new(path).extension()?.to_str()? {
        "rs" => Some("//! @generated"),
        // A fragment is textually spliced into the file that includes it, and
        // an inner doc comment cannot appear at an expansion site.
        "rsi" => Some("// @generated"),
        _ => None,
    }
}

fn validate_manifest_metadata(manifest: &Manifest, violations: &mut Vec<String>) {
    if manifest.schema != 1 {
        violations.push(format!(
            "generated manifest schema must be 1, found {}",
            manifest.schema
        ));
    }
    if manifest.generator != EXPECTED_GENERATOR {
        violations.push(format!(
            "unexpected generated manifest compiler identity: {}",
            manifest.generator
        ));
    }
    if manifest.ir_version != 1 {
        violations.push(format!(
            "generated manifest IR version must be 1, found {}",
            manifest.ir_version
        ));
    }
    if manifest.upstream_repository.trim().is_empty() {
        violations.push("generated manifest upstream repository is empty".to_owned());
    }
    if manifest.upstream_commit.len() != 40
        || !manifest
            .upstream_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        violations.push("generated manifest upstream commit is not a full SHA".to_owned());
    }
}

fn is_plain_filename(source: &str) -> bool {
    let path = Path::new(source);
    path.components()
        .all(|component| matches!(component, Component::Normal(_)))
        && path.file_name().and_then(|name| name.to_str()) == Some(source)
}
