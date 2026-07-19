//! Location of the vendored corpus and the exceptions a strict run accepts.

use std::{fs, path::PathBuf};

use kafka_wire_schema::{SchemaException, SchemaExceptions};

/// The single vendored commit tree, discovered rather than spelled out.
///
/// Hardcoding the pinned SHA here would let every corpus test keep surveying an
/// abandoned tree after `cargo xtask vendor` moved the pin.
pub(crate) fn corpus_root() -> PathBuf {
    let vendored = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("spec/upstream/apache-kafka");
    let mut commits = fs::read_dir(&vendored)
        .unwrap_or_else(|error| panic!("read {}: {error}", vendored.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    commits.sort();

    assert_eq!(
        commits.len(),
        1,
        "expected exactly one vendored commit tree under {}, found {:?}",
        vendored.display(),
        commits,
    );
    commits.remove(0).join("message")
}

/// Every vendored schema file, in a stable order.
pub(crate) fn schema_files() -> Vec<PathBuf> {
    let corpus = corpus_root();
    let mut files = fs::read_dir(&corpus)
        .unwrap_or_else(|error| panic!("read {}: {error}", corpus.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

/// Reads the reviewed upstream defects from `spec/overrides`.
///
/// Two upstream files violate an invariant this front end is right to enforce.
/// They are accepted by name rather than by weakening a rule, and
/// `vendored_corpus.rs` fails if either entry stops being load-bearing.
pub(crate) fn exceptions() -> SchemaExceptions {
    #[derive(serde::Deserialize)]
    struct Overrides {
        accepted: Vec<Accepted>,
    }

    #[derive(serde::Deserialize)]
    struct Accepted {
        message: String,
        field: Option<String>,
        code: String,
        reason: String,
        upstream: String,
    }

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("spec/overrides/schema_exceptions.toml");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let overrides: Overrides =
        toml::from_str(&text).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));

    SchemaExceptions::new(
        overrides
            .accepted
            .into_iter()
            .map(|entry| SchemaException {
                message: entry.message,
                field: entry.field,
                code: entry.code,
                reason: entry.reason,
                upstream: entry.upstream,
            })
            .collect(),
    )
}
