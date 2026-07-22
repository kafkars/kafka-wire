//! Generation is deterministic and touches the output tree exactly once.
//!
//! Scenario: drive the real `generate` entry point against a synthetic pinned
//! workspace and observe the output directory. Every claim here was a milestone
//! claim resting on manual probes that were reverted: that the same input
//! produces the same bytes twice, that check mode never writes, and that a
//! failure anywhere before the write phase leaves the tree exactly as it was.
//!
//! What stamp the output carries is a separate question, proved in
//! `provenance.rs`.

mod support;

use std::fs;

use kafka_wire_codegen::{GenerationError, GenerationMode, PairError, render_corpus};
use support::{COMMIT, REFUSED, SUPPORTED, Workspace, hex_digest, read, repository_root, write};

#[test]
fn the_same_pinned_input_generates_the_same_bytes_twice() {
    let workspace = Workspace::pinning("determinism", &SUPPORTED);

    let first = workspace
        .generate(GenerationMode::Write)
        .unwrap_or_else(|error| panic!("first generation failed: {error}"));
    let after_first = workspace.tree();

    let second = workspace
        .generate(GenerationMode::Write)
        .unwrap_or_else(|error| panic!("second generation failed: {error}"));
    let after_second = workspace.tree();

    assert_eq!(
        after_first, after_second,
        "regenerating the same pinned input produced different bytes"
    );
    assert!(
        first.written > 0,
        "the first generation wrote nothing, so determinism was proved over an empty tree"
    );
    assert_eq!(
        second.written, 0,
        "the second generation rewrote files that had not changed"
    );
    assert_eq!(
        second.unchanged,
        after_first.len(),
        "the second generation did not recognize every file as current"
    );
}

#[test]
fn check_mode_reports_drift_without_creating_anything() {
    let workspace = Workspace::pinning("check-mode", &SUPPORTED);

    let error = workspace
        .generate(GenerationMode::Check)
        .err()
        .unwrap_or_else(|| panic!("check mode accepted a tree that does not exist"));

    assert!(
        matches!(error, GenerationError::Stale { .. }),
        "a missing tree must be reported as staleness: {error:?}"
    );
    assert!(
        workspace.tree().is_empty(),
        "check mode created files in the output tree"
    );
    assert!(
        !workspace.output_root().exists(),
        "check mode created the output directory"
    );
}

#[test]
fn a_refused_schema_leaves_no_partial_tree_behind() {
    // Rendering, formatting, and hashing all complete in memory before the
    // output directory is touched, so a failure at any of those phases must be
    // invisible on disk. Proved twice: against an empty tree, where a partial
    // write would appear as files, and against a populated one, where it would
    // appear as a mixture of old and new bytes.
    let workspace = Workspace::pinning("partial-tree", &[SUPPORTED[0], SUPPORTED[1], REFUSED]);

    let error = workspace
        .generate(GenerationMode::Write)
        .err()
        .unwrap_or_else(|| panic!("the backend rendered a schema outside its slice"));
    assert!(
        matches!(
            error,
            GenerationError::UnsupportedSchema { .. }
                | GenerationError::Pair(PairError::MissingDirection { .. })
        ),
        "an incomplete or unsupported API must be refused by name: {error:?}"
    );
    assert!(
        workspace.tree().is_empty(),
        "a refused generation wrote part of a tree"
    );

    let complete = Workspace::pinning("partial-tree-populated", &SUPPORTED);
    complete
        .generate(GenerationMode::Write)
        .unwrap_or_else(|error| panic!("generation failed: {error}"));
    let good = complete.tree();

    // Re-pin the same workspace with the refused schema added and regenerate.
    let lock = complete.root.join("spec/protocol.lock");
    let bytes = fs::read(
        repository_root()
            .join("spec/upstream/apache-kafka")
            .join(COMMIT)
            .join("message")
            .join(REFUSED),
    )
    .unwrap_or_else(|error| panic!("read vendored {REFUSED}: {error}"));
    write(
        &complete
            .root
            .join("spec/upstream/apache-kafka")
            .join(COMMIT)
            .join("message")
            .join(REFUSED),
        &String::from_utf8_lossy(&bytes),
    );
    let extended = read(&lock).replace(
        "\n[generator]",
        &format!(
            "\n[[kafka.files]]\npath = \"{REFUSED}\"\nsha256 = \"{}\"\nstatus = \"enabled\"\n\n[generator]",
            hex_digest(&bytes)
        ),
    );
    write(&lock, &extended);

    assert!(
        complete.generate(GenerationMode::Write).is_err(),
        "the extended lockfile was accepted"
    );
    assert_eq!(
        complete.tree(),
        good,
        "a refused generation modified an already-generated tree"
    );
}

#[test]
fn an_output_redirect_cannot_replace_a_source_directory() {
    let workspace = Workspace::pinning("output-authorization", &SUPPORTED);
    let source = workspace.root.join("crates/keep.rs");
    write(
        &source,
        "//! Handwritten source that generation must preserve.\n",
    );
    let lock = workspace.root.join("spec/protocol.lock");
    write(
        &lock,
        &read(&lock).replace("crates/kafka-wire/src/generated", "crates"),
    );

    let error = workspace
        .generate(GenerationMode::Write)
        .err()
        .unwrap_or_else(|| panic!("an unauthorized output path was accepted"));

    assert!(matches!(
        error,
        GenerationError::InvalidLockfileValue { ref field, .. }
            if field == "generator.output"
    ));
    assert_eq!(
        read(&source),
        "//! Handwritten source that generation must preserve.\n"
    );
}

#[test]
fn an_existing_destination_requires_a_generator_manifest() {
    let workspace = Workspace::pinning("output-ownership", &SUPPORTED);
    let handwritten = workspace.output_root().join("handwritten.rs");
    write(&handwritten, "//! This directory is not generator-owned.\n");

    let error = workspace
        .generate(GenerationMode::Write)
        .err()
        .unwrap_or_else(|| panic!("an unowned output directory was replaced"));

    assert!(matches!(error, GenerationError::UnownedOutputTree { .. }));
    assert_eq!(
        read(&handwritten),
        "//! This directory is not generator-owned.\n"
    );
}

#[test]
fn a_manifest_must_name_the_expected_schema_and_generator() {
    for (name, manifest) in [
        (
            "output-schema",
            r#"{ "schema": 2, "generator": "kafka-wire-codegen 0.1.0" }"#,
        ),
        (
            "output-generator",
            r#"{ "schema": 1, "generator": "another-generator 0.1.0" }"#,
        ),
    ] {
        let workspace = Workspace::pinning(name, &SUPPORTED);
        let handwritten = workspace.output_root().join("handwritten.rs");
        write(&handwritten, "//! Preserve me.\n");
        write(&workspace.output_root().join("MANIFEST.json"), manifest);

        let error = workspace
            .generate(GenerationMode::Write)
            .err()
            .unwrap_or_else(|| panic!("invalid ownership manifest was accepted"));

        assert!(matches!(error, GenerationError::UnownedOutputTree { .. }));
        assert_eq!(read(&handwritten), "//! Preserve me.\n");
    }
}

#[test]
fn generation_and_corpus_probing_run_cross_schema_validation() {
    let workspace = Workspace::pinning("corpus-validation", &SUPPORTED);
    replace_locked_schema(
        &workspace,
        SUPPORTED[0],
        r#"{
          "apiKey": 17,
          "type": "request",
          "name": "SaslHandshakeRequest",
          "validVersions": "0-1",
          "flexibleVersions": "none",
          "fields": [
            {
              "name": "Entries",
              "type": "[]SaslHandshakeRequest",
              "versions": "0+",
              "fields": [
                { "name": "Value", "type": "string", "versions": "0+" }
              ]
            }
          ]
        }"#,
    );

    for error in [
        workspace
            .generate(GenerationMode::Write)
            .err()
            .unwrap_or_else(|| panic!("generation accepted a cross-schema collision")),
        render_corpus(&workspace.root)
            .err()
            .unwrap_or_else(|| panic!("corpus probing accepted a cross-schema collision")),
    ] {
        let GenerationError::CorpusValidation(errors) = error else {
            panic!("cross-schema collision produced the wrong error: {error:?}");
        };
        assert!(errors.0.iter().any(|error| {
            error.code == "KAFKA_SCHEMA_QUALIFIED_STRUCT_COLLISION"
                && error.message.contains("message `SaslHandshakeRequest`")
                && error.message.contains("struct `SaslHandshakeRequest`")
        }));
    }
    assert!(workspace.tree().is_empty());
}

fn replace_locked_schema(workspace: &Workspace, filename: &str, source: &str) {
    let path = workspace
        .root
        .join("spec/upstream/apache-kafka")
        .join(COMMIT)
        .join("message")
        .join(filename);
    let old = fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let old_digest = hex_digest(&old);
    write(&path, source);
    let new_digest = hex_digest(source.as_bytes());
    let lock = workspace.root.join("spec/protocol.lock");
    let updated = read(&lock).replace(&old_digest, &new_digest);
    assert_ne!(
        updated,
        read(&lock),
        "fixture digest was not present in lock"
    );
    write(&lock, &updated);
}
