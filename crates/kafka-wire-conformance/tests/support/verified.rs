//! An independent reader for `spec/records/verified.json`.
//!
//! The transcript records what Apache Kafka's own `MemoryRecords` reader
//! recovered from batches this repository compressed. Its `hex` is deliberately
//! not Kafka's — it is the output under judgement — so a test must compare
//! against it rather than trust it, and this module hands over the fields to do
//! that with and no opinion about them.

use std::{fs, path::PathBuf};

use serde::Deserialize;

/// One re-encoded batch, and Kafka's reading of it.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Verified {
    /// Batch this repository re-encoded, named as `spec/records/vectors.json` does.
    pub(crate) name: String,
    /// Why the batch earns its place in the corpus.
    pub(crate) why: String,
    /// Hex of the bytes Kafka was shown. Written by this repository, not by Kafka.
    pub(crate) hex: String,
    /// What Kafka read back out of them.
    pub(crate) kafka: Reading,
}

/// Everything Kafka's reader found in one blob.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Reading {
    pub(crate) batches: Vec<ReadBatch>,
}

/// One batch, as Kafka's own accessors reported it.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReadBatch {
    pub(crate) compression: String,
    pub(crate) magic: i8,
    pub(crate) base_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) partition_leader_epoch: i32,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) base_sequence: i32,
    pub(crate) max_timestamp: i64,
    pub(crate) timestamp_type: String,
    pub(crate) transactional: bool,
    pub(crate) control_batch: bool,
    pub(crate) records: Vec<ReadRecord>,
}

/// One record, with the absolute offset and timestamp Kafka resolved.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ReadRecord {
    pub(crate) offset: i64,
    pub(crate) timestamp: i64,
    /// Lowercase hex, or absent where the record carries no key.
    pub(crate) key: Option<String>,
    /// Lowercase hex, or absent for a tombstone.
    pub(crate) value: Option<String>,
    pub(crate) headers: Vec<ReadHeader>,
}

/// One header. The key is never absent on the wire; the value may be.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ReadHeader {
    pub(crate) key: String,
    pub(crate) value: Option<String>,
}

#[derive(Deserialize)]
struct Transcript {
    verified: Vec<Verified>,
}

/// Load the transcript, in the order it was authored.
pub(crate) fn verified() -> Vec<Verified> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("spec/records/verified.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let transcript: Transcript = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    transcript.verified
}
