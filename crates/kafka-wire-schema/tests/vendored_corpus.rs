//! How much of the pinned Kafka protocol can this front end actually read?
//!
//! Scenario: run the complete front end — read, parse, lower, validate — over
//! every vendored upstream message, both the files the backend compiles today
//! and the ones it does not yet, and report exactly which constructs it cannot
//! represent. Vendoring the corpus and being able to compile it are separate
//! capabilities; this file is the instrument that measures the gap between them.
//!
//! Two tests share that walk. `the_front_end_reads_the_whole_vendored_corpus`
//! is the capability tripwire and is `#[ignore]`d because it is known-failing:
//! `commonStructs`, non-message `"type"` values, and unmodeled field properties
//! are still fatal. Un-ignore it when the census reaches zero.
//! `front_end_coverage_does_not_regress` is the ratchet that runs in CI: it
//! pins how many messages load today so coverage can only grow.
//!
//! Read the census as *first blocking construct per file*, not as a usage count.
//! The front end stops at the first phase that fails, so a file blocked on an
//! unmodeled field property never reaches lowering and cannot report what it
//! would have hit there. Counts therefore shrink as earlier blockers are fixed
//! and later ones surface; the census is a work queue, not an inventory.
//!
//! Run the census with:
//! `cargo test -p kafka-wire-schema --test vendored_corpus -- --ignored --nocapture`

#![allow(clippy::unwrap_used)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_schema::{LowerError, SchemaError, SourceError, load_message};

/// Messages that load cleanly today.
///
/// This is a floor, not a target. Raise it whenever the front end learns a new
/// construct; never lower it to make a change pass.
const COVERAGE_FLOOR: usize = 51;

/// One reason a vendored message did not survive the front end.
struct Finding {
    code: &'static str,
    detail: String,
}

/// Aggregated failures: code, then distinct reason, then count and one example.
type Census = BTreeMap<&'static str, BTreeMap<String, (usize, String)>>;

/// Result of running the front end across the whole pinned corpus.
struct Coverage {
    corpus: PathBuf,
    total: usize,
    loaded: Vec<String>,
    failed: usize,
    census: Census,
}

#[test]
#[ignore = "known-failing capability tripwire: the front end cannot yet read the \
            whole vendored corpus. Run with --ignored for the current census, and \
            delete this attribute when it reaches zero failures."]
fn the_front_end_reads_the_whole_vendored_corpus() {
    let coverage = survey_corpus();

    assert!(
        coverage.failed == 0,
        "{}",
        coverage.report("the front end cannot yet read the whole pinned corpus")
    );
}

#[test]
fn front_end_coverage_does_not_regress() {
    let coverage = survey_corpus();

    assert!(
        coverage.loaded.len() >= COVERAGE_FLOOR,
        "front-end coverage regressed: {} of {} vendored messages load, \
         but {COVERAGE_FLOOR} loaded when this floor was recorded.\n{}",
        coverage.loaded.len(),
        coverage.total,
        coverage.report("newly unreadable messages"),
    );
}

#[test]
fn the_vendored_corpus_is_the_whole_upstream_message_tree() {
    let coverage = survey_corpus();

    // A walk that reaches almost nothing would let both tests above pass over
    // an empty set, which is the one way this instrument could lie.
    assert!(
        coverage.total > 150,
        "the vendored corpus at {} holds only {} message files; \
         the pinned upstream tree has roughly 201",
        coverage.corpus.display(),
        coverage.total,
    );
}

/// Runs read, parse, lower, and validate over every vendored message.
fn survey_corpus() -> Coverage {
    let corpus = corpus_root();
    let mut loaded = Vec::new();
    let mut failed = 0;
    let mut census = Census::new();
    let mut total = 0;

    for path in schema_files(&corpus) {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        total += 1;

        match load_message(&path) {
            Ok(_) => loaded.push(filename),
            Err(error) => {
                failed += 1;
                for finding in findings(&error) {
                    let reasons = census.entry(finding.code).or_default();
                    let entry = reasons
                        .entry(finding.detail)
                        .or_insert_with(|| (0, filename.clone()));
                    entry.0 += 1;
                }
            }
        }
    }

    Coverage {
        corpus,
        total,
        loaded,
        failed,
        census,
    }
}

impl Coverage {
    /// Renders the census as the breakdown a capability milestone is planned from.
    fn report(&self, headline: &str) -> String {
        let mut lines = vec![
            format!("{headline}."),
            String::new(),
            format!("corpus: {} files at {}", self.total, self.corpus.display()),
            format!("loaded: {}", self.loaded.len()),
            format!("failed: {}", self.failed),
            String::new(),
            "first blocking construct per file, by code then distinct reason:".to_owned(),
        ];

        for (code, reasons) in &self.census {
            let total: usize = reasons.values().map(|(count, _)| count).sum();
            lines.push(format!("  {total:>4}  {code}"));
            for (reason, (count, example)) in reasons {
                lines.push(format!(
                    "        {count:>4}  {}  (e.g. {example})",
                    truncate(reason)
                ));
            }
        }

        lines.join("\n")
    }
}

/// Splits one front-end failure into its independent census findings.
///
/// Lowering has no stable diagnostic codes yet, so the buckets below mirror
/// `LowerError`'s variants by name. Validation already carries stable codes and
/// contributes them directly. When lowering grows codes of its own, this match
/// should defer to them rather than keep a second vocabulary alive.
fn findings(error: &SchemaError) -> Vec<Finding> {
    match error {
        SchemaError::Source(source) => vec![source_finding(source)],
        SchemaError::Lower(lower) => vec![lower_finding(lower)],
        SchemaError::Validation(errors) => errors
            .0
            .iter()
            .map(|error| Finding {
                code: error.code,
                detail: error.message.clone(),
            })
            .collect(),
    }
}

fn source_finding(error: &SourceError) -> Finding {
    match error {
        SourceError::Read { source, .. } => Finding {
            code: "SOURCE_READ",
            detail: source.to_string(),
        },
        SourceError::UnterminatedBlockComment { .. } => Finding {
            code: "SOURCE_BLOCK_COMMENT",
            detail: "unterminated block comment".to_owned(),
        },
        SourceError::Json { source, .. } => Finding {
            code: "SOURCE_JSON",
            detail: without_position(&source.to_string()),
        },
    }
}

fn lower_finding(error: &LowerError) -> Finding {
    match error {
        LowerError::MessageProperties { properties, .. } => Finding {
            code: "LOWER_MESSAGE_PROPERTIES",
            detail: properties.clone(),
        },
        LowerError::FieldProperties { properties, .. } => Finding {
            code: "LOWER_FIELD_PROPERTIES",
            detail: properties.clone(),
        },
        LowerError::MissingApiKey { .. } => Finding {
            code: "LOWER_MISSING_API_KEY",
            detail: "message declares no apiKey".to_owned(),
        },
        LowerError::Versions { role, reason, .. } => Finding {
            code: "LOWER_VERSIONS",
            detail: format!("{role} versions: {reason}"),
        },
        LowerError::Default { reason, .. } => Finding {
            code: "LOWER_DEFAULT",
            detail: reason.clone(),
        },
    }
}

/// The single vendored commit tree, discovered rather than spelled out.
///
/// Hardcoding the pinned SHA here would let this instrument keep surveying an
/// abandoned corpus after `cargo xtask vendor` moved the pin.
fn corpus_root() -> PathBuf {
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
fn schema_files(corpus: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(corpus)
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

/// Drops the trailing position from a serde diagnostic so like reasons group.
fn without_position(message: &str) -> String {
    match message.find(" at line ") {
        Some(offset) => message[..offset].to_owned(),
        None => message.to_owned(),
    }
}

fn truncate(reason: &str) -> String {
    const LIMIT: usize = 72;

    if reason.chars().count() <= LIMIT {
        return reason.to_owned();
    }
    let head = reason.chars().take(LIMIT - 3).collect::<String>();
    format!("{head}...")
}
