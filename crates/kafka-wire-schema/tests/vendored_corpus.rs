//! How much of the pinned Kafka protocol can this front end actually read?
//!
//! Scenario: run the complete front end — read, parse, lower, validate — over
//! every vendored upstream message, both the files the backend compiles today
//! and the ones it does not yet, and report exactly which constructs it cannot
//! represent. Vendoring the corpus and being able to compile it are separate
//! capabilities; this file is the instrument that measures the gap between them.
//!
//! Three tests share that walk. `the_front_end_reads_the_whole_vendored_corpus`
//! is the capability tripwire: it now passes, so it runs unconditionally and any
//! new upstream construct the front end cannot represent fails CI on arrival.
//! `front_end_coverage_does_not_regress` is the ratchet that pins how many
//! messages load today so coverage can only grow.
//! `every_documented_exception_is_still_needed` keeps the override file honest.
//!
//! The walk runs with the reviewed exceptions from
//! `spec/overrides/schema_exceptions.toml`, loaded by `support::corpus`. Two
//! upstream files violate an invariant this front end is right to enforce, so
//! they are accepted by name rather than by weakening the rule; the third test
//! below fails if either entry stops being necessary.
//!
//! Read the census as *first blocking construct per file*, not as a usage count.
//! The front end stops at the first phase that fails, so a file blocked on an
//! unmodeled field property never reaches lowering and cannot report what it
//! would have hit there. Counts therefore shrink as earlier blockers are fixed
//! and later ones surface; the census is a work queue, not an inventory.
//!
//! Read the census with:
//! `cargo test -p kafka-wire-schema --test vendored_corpus -- --nocapture`

#![allow(clippy::unwrap_used)]

mod support;

use std::{collections::BTreeMap, path::PathBuf};

use kafka_wire_schema::{LowerError, SchemaError, SourceError, load_message, load_message_with};

use support::{corpus_root, exceptions, schema_files};

/// Messages that load cleanly today.
///
/// This is a floor, not a target. Raise it whenever the front end learns a new
/// construct; never lower it to make a change pass.
const COVERAGE_FLOOR: usize = 201;

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

/// Every declared exception must still be load-bearing.
///
/// An override that no longer changes any outcome is worse than no override: it
/// documents a defect that may already be fixed upstream and quietly widens what
/// the next reader believes is tolerated. Each entry is therefore replayed
/// against its own file with exceptions off, and must reproduce exactly the
/// finding it claims to accept.
#[test]
fn every_documented_exception_is_still_needed() {
    let corpus = corpus_root();

    for exception in exceptions().entries() {
        let path = corpus.join(format!("{}.json", exception.message));
        let Err(SchemaError::Validation(errors)) = load_message(&path) else {
            panic!(
                "{} no longer fails validation, so the `{}` exception for field {:?} is stale",
                path.display(),
                exception.code,
                exception.field,
            );
        };

        assert!(
            errors.0.iter().any(|error| error.code == exception.code
                && error.field.as_deref() == exception.field.as_deref()),
            "{} no longer reports {} for field {:?}; the exception is stale.\nstill reports: {:?}",
            path.display(),
            exception.code,
            exception.field,
            errors.0.iter().map(|error| error.code).collect::<Vec<_>>(),
        );
    }
}

/// Runs read, parse, lower, and validate over every vendored message.
fn survey_corpus() -> Coverage {
    let corpus = corpus_root();
    let exceptions = exceptions();
    let mut loaded = Vec::new();
    let mut failed = 0;
    let mut census = Census::new();
    let mut total = 0;

    for path in schema_files() {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        total += 1;

        match load_message_with(&path, &exceptions) {
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
        other => vec![Finding {
            code: "SCHEMA_UNCLASSIFIED",
            detail: other.to_string(),
        }],
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
        LowerError::CommonStructProperties { properties, .. } => Finding {
            code: "LOWER_COMMON_STRUCT_PROPERTIES",
            detail: properties.clone(),
        },
        LowerError::FieldType { reason, .. } => Finding {
            code: "LOWER_FIELD_TYPE",
            detail: reason.clone(),
        },
        LowerError::EntityType { reason, .. } => Finding {
            code: "LOWER_ENTITY_TYPE",
            detail: reason.clone(),
        },
        LowerError::NestingDepth { limit, .. } => Finding {
            code: "LOWER_NESTING_DEPTH",
            detail: format!("inline fields nest deeper than {limit} levels"),
        },
        LowerError::Versions { role, reason, .. } => Finding {
            code: "LOWER_VERSIONS",
            detail: format!("{role} versions: {reason}"),
        },
        LowerError::Default { reason, .. } => Finding {
            code: "LOWER_DEFAULT",
            detail: reason.clone(),
        },
        other => Finding {
            code: "LOWER_UNCLASSIFIED",
            detail: other.to_string(),
        },
    }
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
