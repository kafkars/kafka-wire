//! The protocol lock is a strict, platform-independent trust boundary.
//!
//! Scenario: parse focused lock documents that vary one identity, digest,
//! repository path, or duplicate entry and require rejection before any native
//! path is constructed or source file is opened.

use std::path::Path;

use kafka_wire_codegen::{GenerationError, ProtocolLock};

const COMMIT: &str = "678c0e07e4733c5a592e52046dc2c4e1625587f1";
const DIGEST: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

fn document(
    repository: &str,
    commit: &str,
    upstream: &str,
    vendored: &str,
    output: &str,
    files: &str,
) -> String {
    format!(
        "schema = 1\n\n\
         [kafka]\n\
         repository = \"{repository}\"\n\
         commit = \"{commit}\"\n\
         upstream_message_root = \"{upstream}\"\n\
         vendored_root = \"{vendored}\"\n\n\
         {files}\n\n\
         [generator]\n\
         ir_version = 1\n\
         output = \"{output}\"\n"
    )
}

fn one_file(path: &str, digest: &str, status: &str) -> String {
    format!("[[kafka.files]]\npath = \"{path}\"\nsha256 = \"{digest}\"\nstatus = \"{status}\"")
}

fn valid() -> String {
    document(
        "https://github.com/apache/kafka",
        COMMIT,
        "clients/src/main/resources/common/message",
        "spec/upstream/apache-kafka",
        "crates/kafka-wire/src/generated",
        &one_file("ApiVersionsRequest.json", DIGEST, "enabled"),
    )
}

macro_rules! parse {
    ($source:expr) => {
        ProtocolLock::parse(Path::new("fixture/protocol.lock"), $source)
    };
}

#[test]
fn a_complete_canonical_lock_is_accepted() {
    parse!(&valid()).unwrap_or_else(|error| panic!("valid lock rejected: {error}"));
}

#[test]
fn unknown_fields_and_wrong_repository_identity_are_rejected() {
    let unknown = valid().replace("[kafka]\n", "[kafka]\nmystery = true\n");
    assert!(
        matches!(parse!(&unknown), Err(GenerationError::Lockfile { .. })),
        "an unknown lockfile key was ignored"
    );

    let wrong = valid().replace(
        "https://github.com/apache/kafka",
        "https://example.invalid/apache/kafka",
    );
    assert!(
        matches!(
            parse!(&wrong),
            Err(GenerationError::InvalidLockfileValue { ref field, .. })
                if field == "kafka.repository"
        ),
        "an arbitrary upstream repository was accepted"
    );
}

#[test]
fn object_ids_and_content_digests_require_fixed_lowercase_hex() {
    for commit in ["abc", "678C0E07E4733C5A592E52046DC2C4E1625587F1"] {
        let source = valid().replace(COMMIT, commit);
        assert!(
            matches!(
                parse!(&source),
                Err(GenerationError::InvalidLockfileValue { ref field, .. })
                    if field == "kafka.commit"
            ),
            "invalid commit `{commit}` was accepted"
        );
    }

    for digest in [
        "abc",
        "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD",
    ] {
        let source = valid().replace(DIGEST, digest);
        assert!(
            matches!(
                parse!(&source),
                Err(GenerationError::InvalidLockfileValue { ref field, .. })
                    if field.ends_with(".sha256")
            ),
            "invalid digest `{digest}` was accepted"
        );
    }
}

#[test]
fn repository_paths_have_one_host_independent_grammar() {
    for unsafe_path in [
        "../message",
        "clients//message",
        "clients/./message",
        "clients\\message",
        "/clients/message",
        "clients/message/",
    ] {
        let source = document(
            "https://github.com/apache/kafka",
            COMMIT,
            unsafe_path,
            "spec/upstream/apache-kafka",
            "crates/kafka-wire/src/generated",
            &one_file("ApiVersionsRequest.json", DIGEST, "enabled"),
        );
        assert!(
            parse!(&source).is_err(),
            "unsafe repository path `{unsafe_path}` was accepted"
        );
    }

    let source = valid().replace("ApiVersionsRequest.json", "..\\ApiVersionsRequest.json");
    assert!(
        parse!(&source).is_err(),
        "a Windows traversal spelling was accepted"
    );
}

#[test]
fn duplicate_file_entries_are_rejected_even_when_statuses_conflict() {
    let files = [
        one_file("ApiVersionsRequest.json", DIGEST, "enabled"),
        one_file("ApiVersionsRequest.json", DIGEST, "pending"),
    ]
    .join("\n\n");
    let source = document(
        "https://github.com/apache/kafka",
        COMMIT,
        "clients/src/main/resources/common/message",
        "spec/upstream/apache-kafka",
        "crates/kafka-wire/src/generated",
        &files,
    );

    assert!(
        matches!(
            parse!(&source),
            Err(GenerationError::InvalidLockfileValue { ref field, .. })
                if field == "kafka.files.path"
        ),
        "duplicate source entries were accepted"
    );
}
