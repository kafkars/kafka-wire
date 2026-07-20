//! The record-batch corpus, and the two directions of owning it.
//!
//! A `records` field is opaque to Kafka's generated JSON converters — they take
//! it as base64 and hand it back untouched — so the message oracle structurally
//! cannot author a batch. This corpus has one of its own, driving Kafka's
//! `MemoryRecordsBuilder`, and this module owns the `cargo xtask records`
//! surface around it.
//!
//! The split mirrors `vectors`: `--refresh` needs a Java toolchain and the
//! pinned jar and is run by a human, while `--check` reads the checked-in files
//! in pure Rust. Whether this repository actually agrees with the bytes is
//! neither one's job — `kafka-wire-conformance` decides that.

use std::{collections::BTreeSet, path::Path};

use serde::{Deserialize, Serialize};

mod oracle;

use crate::cli::RecordsMode;

/// Format revision written here and required by the conformance crate.
const SCHEMA: u32 = 1;

/// One authored batch situation.
#[derive(Debug, Deserialize, Serialize)]
struct Plan {
    name: String,
    why: String,
    #[serde(flatten)]
    rest: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Plans {
    schema: u32,
    about: String,
    batches: Vec<Plan>,
}

/// The record oracle's reply: one answer per batch, in the order asked.
#[derive(Debug, Deserialize)]
struct Answers {
    results: Vec<Answer>,
}

/// One batch's bytes, as Kafka laid them out.
#[derive(Debug, Deserialize)]
struct Answer {
    name: String,
    hex: String,
}

/// One batch, and the bytes Apache Kafka laid out for it.
#[derive(Debug, Deserialize, Serialize)]
struct Vector {
    name: String,
    why: String,
    hex: String,
}

/// The whole `vectors.json` file. Named for the file rather than its one
/// collection, so the serialized key stays `vectors` where a reader expects it.
#[derive(Debug, Deserialize, Serialize)]
struct Corpus {
    schema: u32,
    about: String,
    vectors: Vec<Vector>,
}

pub(crate) fn run(mode: RecordsMode) -> Result<(), String> {
    let workspace = crate::workspace::root();
    match mode {
        RecordsMode::Refresh => refresh(&workspace),
        RecordsMode::Check => check(&workspace),
    }
}

fn plans_path(workspace: &Path) -> std::path::PathBuf {
    workspace.join("spec").join("records").join("plans.json")
}

fn vectors_path(workspace: &Path) -> std::path::PathBuf {
    workspace.join("spec").join("records").join("vectors.json")
}

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_json::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display()))
}

/// Verify the checked-in corpus without Java, a jar, or a network.
///
/// This proves the corpus is internally coherent — every authored batch has a
/// vector, no vector is orphaned, and every hex is well formed. It deliberately
/// does not check the bytes against an encoder: that comparison belongs to
/// `kafka-wire-conformance`, which holds `kafka-wire-records` to them.
fn check(workspace: &Path) -> Result<(), String> {
    let plans: Plans = read(&plans_path(workspace))?;
    let vectors: Corpus = read(&vectors_path(workspace))?;

    for (label, schema) in [("plans", plans.schema), ("vectors", vectors.schema)] {
        if schema != SCHEMA {
            return Err(format!(
                "spec/records/{label}.json declares schema {schema}, not the supported {SCHEMA}"
            ));
        }
    }

    let authored: Vec<&str> = plans
        .batches
        .iter()
        .map(|plan| plan.name.as_str())
        .collect();
    let recorded: Vec<&str> = vectors
        .vectors
        .iter()
        .map(|vector| vector.name.as_str())
        .collect();
    if authored != recorded {
        let authored_set: BTreeSet<_> = authored.iter().collect();
        let recorded_set: BTreeSet<_> = recorded.iter().collect();
        return Err(format!(
            "spec/records/vectors.json has drifted from plans.json; \
             refresh it with `cargo xtask records --refresh`\n  \
             only in plans:   {:?}\n  only in vectors: {:?}",
            authored_set.difference(&recorded_set).collect::<Vec<_>>(),
            recorded_set.difference(&authored_set).collect::<Vec<_>>(),
        ));
    }

    for vector in &vectors.vectors {
        if vector.hex.len() % 2 != 0 || !vector.hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("{}: hex is not a byte string", vector.name));
        }
        if vector.hex.is_empty() {
            return Err(format!("{}: a batch cannot be empty", vector.name));
        }
    }

    println!(
        "record corpus is current: {} batch(es), authored by Apache Kafka",
        vectors.vectors.len()
    );
    Ok(())
}

/// Re-author the corpus from the pinned jar.
fn refresh(workspace: &Path) -> Result<(), String> {
    let plans: Plans = read(&plans_path(workspace))?;

    println!("proving every codec is reachable and every batch reproducible:");
    for line in oracle::self_test(workspace)?.lines() {
        println!("  {line}");
    }

    let request = std::fs::read_to_string(plans_path(workspace))
        .map_err(|error| format!("read plans: {error}"))?;
    let answered = oracle::encode(workspace, &request)?;

    let answers: Answers = serde_json::from_str(&answered)
        .map_err(|error| format!("parse the record oracle's answer: {error}"))?;
    if answers.results.len() != plans.batches.len() {
        return Err(format!(
            "asked for {} batch(es) and the oracle answered {}",
            plans.batches.len(),
            answers.results.len()
        ));
    }

    let mut vectors = Vec::with_capacity(plans.batches.len());
    for (plan, answer) in plans.batches.iter().zip(&answers.results) {
        if plan.name != answer.name {
            return Err(format!(
                "the oracle answered `{}` where `{}` was asked; batch order is not reliable",
                answer.name, plan.name
            ));
        }
        vectors.push(Vector {
            name: plan.name.clone(),
            why: plan.why.clone(),
            hex: answer.hex.clone(),
        });
    }

    let written = Corpus {
        schema: SCHEMA,
        about: "Byte vectors authored by Apache Kafka's own MemoryRecordsBuilder, the class its \
                producer uses to lay out a batch. Regenerate with `cargo xtask records --refresh`; \
                never edit a hex by hand."
            .to_owned(),
        vectors,
    };
    let text = serde_json::to_string_pretty(&written)
        .map_err(|error| format!("serialize vectors: {error}"))?;
    std::fs::write(vectors_path(workspace), text + "\n")
        .map_err(|error| format!("write vectors: {error}"))?;

    println!(
        "refreshed {} broker-authored record batch(es)",
        written.vectors.len()
    );
    Ok(())
}
