//! An independent reader for `spec/records/vectors.json`.
//!
//! Written against the file format rather than against the xtask that writes it,
//! for the same reason `src/corpus.rs` is: a reader and a writer that agree
//! should do so because the format is stable, not because one imported the
//! other's struct.

use std::{fs, path::PathBuf};

use serde::Deserialize;

/// One batch, and the bytes Apache Kafka's own producer laid out for it.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Batch {
    /// Plan case that produced this batch.
    pub(crate) name: String,
    /// Why this case earns its place in the corpus.
    pub(crate) why: String,
    /// Lowercase hex of the batch Kafka wrote.
    pub(crate) hex: String,
}

#[derive(Deserialize)]
struct Corpus {
    vectors: Vec<Batch>,
}

/// Load every checked-in batch, in the order the corpus declares them.
///
/// A missing or unreadable corpus panics rather than yielding an empty list: a
/// conformance run that silently inspects nothing reports success.
pub(crate) fn batches() -> Vec<Batch> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("spec/records/vectors.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let corpus: Corpus = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    corpus.vectors
}
