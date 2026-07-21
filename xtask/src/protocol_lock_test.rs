//! The pinned-input contract survives a round trip byte for byte.
//!
//! Scenario: read the repository's real `spec/protocol.lock`, render it back,
//! and compare. Vendoring an unchanged commit rewrites this document from
//! scratch, so anything the writer cannot reproduce exactly would show up as a
//! spurious diff on every `cargo xtask vendor` — and a real change would then
//! be invisible inside the noise.
//!
//! The real lockfile is used rather than a fixture on purpose: a writer proved
//! only against its own output would agree with itself while disagreeing with
//! the document under review.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::protocol_lock::{ProtocolLock, SourceStatus, digest, read, recorded_statuses, render};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn lock_path() -> PathBuf {
    repository_root().join("spec").join("protocol.lock")
}

fn checked_in_lock() -> ProtocolLock {
    read(&lock_path()).unwrap_or_else(|error| panic!("read the repository lockfile: {error}"))
}

#[test]
fn re_rendering_the_checked_in_lockfile_reproduces_it_exactly() {
    // This is vendor idempotence with the network removed: given the same
    // upstream bytes, the digests and statuses are unchanged, so the only thing
    // that could differ is the rendering.
    let on_disk = fs::read_to_string(lock_path())
        .unwrap_or_else(|error| panic!("read the repository lockfile: {error}"));

    assert_eq!(
        render(&checked_in_lock()),
        on_disk,
        "re-rendering spec/protocol.lock does not reproduce the checked-in bytes; \
         `cargo xtask vendor` would report a diff on an unchanged commit"
    );
}

#[test]
fn parsing_a_rendered_lockfile_is_a_fixed_point() {
    let once = render(&checked_in_lock());
    let reparsed = ProtocolLock::parse(&lock_path(), &once)
        .unwrap_or_else(|error| panic!("re-parse the rendered lockfile: {error}"));

    assert_eq!(
        render(&reparsed),
        once,
        "parse -> render -> parse -> render is not stable"
    );
}

#[test]
fn a_round_trip_preserves_every_pinned_fact() {
    let original = checked_in_lock();
    let reparsed = ProtocolLock::parse(&lock_path(), &render(&original))
        .unwrap_or_else(|error| panic!("re-parse the rendered lockfile: {error}"));

    assert_eq!(original.schema, reparsed.schema);
    assert_eq!(original.kafka.repository, reparsed.kafka.repository);
    assert_eq!(original.kafka.commit, reparsed.kafka.commit);
    assert_eq!(
        original.kafka.upstream_message_root,
        reparsed.kafka.upstream_message_root
    );
    assert_eq!(original.kafka.vendored_root, reparsed.kafka.vendored_root);
    assert_eq!(original.generator.ir_version, reparsed.generator.ir_version);
    assert_eq!(original.generator.output, reparsed.generator.output);

    assert_eq!(
        original.kafka.files.len(),
        reparsed.kafka.files.len(),
        "the round trip lost or invented pinned files"
    );
    for (before, after) in original.kafka.files.iter().zip(&reparsed.kafka.files) {
        assert_eq!(before.path, after.path, "pinned files changed order");
        assert_eq!(
            before.sha256, after.sha256,
            "{} lost its digest",
            before.path
        );
        assert_eq!(
            before.status, after.status,
            "{} changed status",
            before.path
        );
    }
}

#[test]
fn the_checked_in_digests_match_the_vendored_bytes() {
    // The reader half in kafka-wire-codegen enforces this at generation time for
    // every file it opens. Asserting it here as well means a vendored file
    // edited by hand fails the fast test suite, not the slow generation.
    let lock = checked_in_lock();
    let message_root = repository_root()
        .join(&lock.kafka.vendored_root)
        .join(&lock.kafka.commit)
        .join(
            Path::new(&lock.kafka.upstream_message_root)
                .file_name()
                .unwrap_or_else(|| panic!("upstream_message_root names no directory")),
        );

    let mut mismatched = Vec::new();
    for file in &lock.kafka.files {
        let path = message_root.join(&file.path);
        let bytes =
            fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        if digest(&bytes) != file.sha256 {
            mismatched.push(file.path.clone());
        }
    }

    assert!(
        mismatched.is_empty(),
        "vendored bytes no longer match their pinned digests: {mismatched:?}"
    );
    assert!(
        lock.kafka.files.len() > 100,
        "the pinned corpus shrank to {} files, so this proof covers almost nothing",
        lock.kafka.files.len()
    );
}

#[test]
fn recorded_statuses_report_what_the_document_says() {
    let lock = checked_in_lock();
    let statuses = recorded_statuses(&lock);

    assert_eq!(
        statuses.len(),
        lock.kafka.files.len(),
        "two pinned entries share one filename, so re-vendoring would lose a status"
    );
    let enabled = statuses
        .values()
        .filter(|status| **status == SourceStatus::Enabled)
        .count();
    assert!(
        enabled > 0,
        "no pinned file is enabled, so nothing would be generated"
    );
    assert!(
        enabled < lock.kafka.files.len(),
        "every pinned file is enabled, so the pending seam is no longer exercised"
    );
}

#[test]
fn a_digest_is_lowercase_hexadecimal_of_exactly_the_bytes_given() {
    // The lockfile is compared textually, so an uppercase or truncated digest
    // would look like upstream drift rather than a formatting slip.
    assert_eq!(
        digest(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        digest(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_ne!(
        digest(b"abc"),
        digest(b"abc\n"),
        "a trailing newline must change the digest"
    );
}
