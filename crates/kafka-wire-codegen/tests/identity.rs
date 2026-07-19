//! The pinned corpus and the compiled subset are reported as two numbers.
//!
//! Scenario: read the repository's pinned identity and compare it with the
//! lockfile it summarizes. `cargo xtask doctor` prints these numbers and they
//! are the only place anyone sees how much of the vendored protocol is actually
//! compiled, so reporting the corpus size as the generated size would overstate
//! the work done by two orders of magnitude.

use std::{
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_codegen::protocol_identity;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn lockfile() -> String {
    fs::read_to_string(repository_root().join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read the repository lockfile: {error}"))
}

#[test]
fn the_pinned_identity_repeats_what_the_lockfile_says() {
    let identity = protocol_identity(repository_root())
        .unwrap_or_else(|error| panic!("read the pinned protocol identity: {error}"));
    let lock = lockfile();

    assert!(
        lock.contains(&format!("repository = \"{}\"", identity.repository)),
        "the reported repository is not the pinned one: {}",
        identity.repository
    );
    assert!(
        lock.contains(&format!("commit = \"{}\"", identity.commit)),
        "the reported commit is not the pinned one: {}",
        identity.commit
    );
    assert_eq!(
        identity.source_files,
        lock.matches("[[kafka.files]]").count(),
        "the reported corpus size does not match the pinned file count"
    );
}

#[test]
fn the_compiled_subset_is_counted_separately_from_the_corpus() {
    let identity = protocol_identity(repository_root())
        .unwrap_or_else(|error| panic!("read the pinned protocol identity: {error}"));
    let enabled = lockfile().matches("status = \"enabled\"").count();

    assert_eq!(
        identity.enabled_files, enabled,
        "the reported compiled count does not match the enabled entries"
    );
    assert!(
        identity.enabled_files < identity.source_files,
        "every pinned file is enabled, so the two numbers no longer say \
         anything different and the distinction has stopped being exercised"
    );
}

#[test]
fn a_checkout_without_a_lockfile_is_an_error_rather_than_an_empty_identity() {
    // Returning zeroes would make `doctor` report a repository pinned to
    // nothing, which reads as a clean state rather than a missing file.
    let missing = repository_root().join("target/no-such-checkout");

    assert!(
        protocol_identity(&missing).is_err(),
        "an identity was reported for a checkout with no lockfile"
    );
}
