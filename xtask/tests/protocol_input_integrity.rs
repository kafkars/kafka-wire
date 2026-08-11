//! The protocol lockfile content-addresses every vendored upstream message input.
//!
//! Scenario: read a lockfile, check its upstream identity, then hash every
//! vendored message it pins. A fixture whose vendored bytes drifted from the
//! recorded digest — an upstream file edited after vendoring — must be
//! rejected by name.

#![allow(clippy::unwrap_used)]

mod support;

use std::{collections::BTreeSet, fs, path::Path};

use serde::Deserialize;
use support::{fixture_root, sha256, workspace_root};

#[derive(Debug, Deserialize)]
struct ProtocolLock {
    schema: u32,
    kafka: KafkaLock,
    generator: GeneratorLock,
}

#[derive(Debug, Deserialize)]
struct KafkaLock {
    repository: String,
    commit: String,
    upstream_message_root: String,
    vendored_root: String,
    files: Vec<LockedFile>,
}

#[derive(Debug, Deserialize)]
struct LockedFile {
    path: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct GeneratorLock {
    ir_version: u32,
    output: String,
}

/// Lockfile identity findings and vendored-byte drift for one pinned tree.
fn protocol_input_violations(base: &Path, lock_path: &Path) -> Vec<String> {
    let source = fs::read_to_string(lock_path)
        .unwrap_or_else(|error| panic!("read protocol lock {}: {error}", lock_path.display()));
    let lock: ProtocolLock = toml::from_str(&source)
        .unwrap_or_else(|error| panic!("parse protocol lock {}: {error}", lock_path.display()));
    let mut violations = Vec::new();

    if lock.schema != 1 {
        violations.push(format!(
            "protocol lock schema must be 1, found {}",
            lock.schema
        ));
    }
    if lock.kafka.repository.trim().is_empty() {
        violations.push("protocol lock upstream identity is incomplete".to_owned());
    }
    if !is_full_sha(&lock.kafka.commit) {
        violations.push("protocol lock commit must be one full hexadecimal SHA".to_owned());
    }
    if lock.generator.ir_version != 1 {
        violations.push(format!(
            "generator IR version must be 1, found {}",
            lock.generator.ir_version
        ));
    }
    let mut configured_paths_safe = true;
    for configured in [
        &lock.kafka.upstream_message_root,
        &lock.kafka.vendored_root,
        &lock.generator.output,
    ] {
        if !is_safe_relative_path(configured) {
            configured_paths_safe = false;
            violations.push(format!(
                "configured path is not a safe relative path: {configured}"
            ));
        }
    }

    assert!(
        !lock.kafka.files.is_empty(),
        "{} pins no upstream message files; \
         a lock test with nothing to hash would pass over an empty set",
        lock_path.display()
    );

    if !configured_paths_safe {
        return violations;
    }

    let source_root = base
        .join(&lock.kafka.vendored_root)
        .join(&lock.kafka.commit)
        .join(
            lock.kafka
                .upstream_message_root
                .rsplit('/')
                .next()
                .unwrap_or("message"),
        );
    let mut folded_paths = BTreeSet::new();
    for file in &lock.kafka.files {
        if !is_plain_filename(&file.path) {
            violations.push(format!(
                "locked source path must be one plain filename: {}",
                file.path
            ));
            continue;
        }
        if !folded_paths.insert(file.path.to_ascii_lowercase()) {
            violations.push(format!(
                "locked source paths collide under ASCII case folding: {}",
                file.path
            ));
            continue;
        }
        let path = source_root.join(&file.path);
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("read locked source {}: {error}", path.display()));
        let actual = sha256(&bytes);
        if actual != file.sha256 {
            violations.push(format!(
                "{} hash mismatch: lock {}, actual {actual}",
                path.display(),
                file.sha256
            ));
        }
    }

    violations
}

#[test]
fn vendored_message_bytes_match_protocol_lock() {
    let workspace = workspace_root();
    let violations =
        protocol_input_violations(&workspace, &workspace.join("spec").join("protocol.lock"));

    assert!(
        violations.is_empty(),
        "pinned protocol input violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_vendored_message_edited_after_pinning_is_rejected() {
    let root = fixture_root("tampered_protocol_input");
    let violations = protocol_input_violations(&root, &root.join("spec").join("protocol.lock"));

    assert!(
        violations.iter().any(|violation| {
            violation.contains("EditedRequest.json") && violation.contains("hash mismatch")
        }),
        "the protocol-input detector accepted drifted vendored bytes: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("FaithfulRequest.json")),
        "the protocol-input detector rejected an untouched vendored file: {violations:?}"
    );
}

fn is_full_sha(source: &str) -> bool {
    source.len() == 40 && source.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_plain_filename(source: &str) -> bool {
    portable_component(source)
}

fn is_safe_relative_path(source: &str) -> bool {
    !source.is_empty() && source.split('/').all(portable_component)
}

fn portable_component(component: &str) -> bool {
    !component.is_empty()
        && component != "."
        && component != ".."
        && !component.ends_with('.')
        && component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        && !windows_device_name(component.split('.').next().unwrap_or(component))
}

fn windows_device_name(stem: &str) -> bool {
    if ["CON", "PRN", "AUX", "NUL"]
        .iter()
        .any(|device| stem.eq_ignore_ascii_case(device))
    {
        return true;
    }
    let upper = stem.to_ascii_uppercase();
    matches!(
        upper.as_bytes(),
        [b'C', b'O', b'M', b'1'..=b'9'] | [b'L', b'P', b'T', b'1'..=b'9']
    )
}
