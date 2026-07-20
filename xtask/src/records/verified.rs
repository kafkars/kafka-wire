//! What Apache Kafka reads back from bytes this repository wrote.
//!
//! This module owns `spec/records/verified.json`, the transcript that carries
//! compression encode across the Java boundary. A compressed payload cannot be
//! held to Kafka's bytes — no two implementations of DEFLATE, zstd, LZ4, or
//! snappy make the same internal choices — so the claim under test is not "the
//! same bytes" but "bytes Kafka accepts and reads back unchanged", and only
//! Kafka can answer that.
//!
//! The transcript is therefore the one file in this repository whose `hex` is
//! deliberately NOT Kafka's. It is this repository's own output, pinned so that
//! a later comparison is against bytes Kafka actually saw: when the compressor's
//! output moves, the entry stops matching and a human re-runs `--refresh` rather
//! than inheriting a verdict about bytes that no longer exist.
//!
//! It deliberately owns no judgement. Whether Kafka's answer is the right answer
//! is decided by `kafka-wire-conformance`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use kafka_wire_records::{Compression, RecordBatch, RecordDecodeLimits};
use serde::{Deserialize, Serialize};

use super::{Corpus, SCHEMA, read};

/// One batch, the bytes this repository re-encoded it to, and Kafka's reading.
#[derive(Debug, Deserialize, Serialize)]
struct Verified {
    name: String,
    why: String,
    /// Hex of the batch THIS repository wrote. Not Kafka's — see the module note.
    hex: String,
    /// The oracle's answer, carried verbatim. This module interprets none of it.
    kafka: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct Transcript {
    schema: u32,
    about: String,
    verified: Vec<Verified>,
}

/// The oracle's reply: one reading per batch, in the order asked.
#[derive(Debug, Deserialize)]
struct Readings {
    results: Vec<Reading>,
}

#[derive(Debug, Deserialize)]
struct Reading {
    name: String,
    read: serde_json::Value,
}

/// One batch put in front of Kafka's reader.
#[derive(Clone, Debug, Serialize)]
struct Question {
    name: String,
    hex: String,
}

#[derive(Debug, Serialize)]
struct Questions {
    batches: Vec<Question>,
}

pub(super) fn path(workspace: &Path) -> PathBuf {
    workspace.join("spec").join("records").join("verified.json")
}

/// Verify the checked-in transcript's shape without Java, a jar, or a network.
///
/// The comparison that matters — re-encoding a batch and finding the bytes Kafka
/// was shown — belongs to `kafka-wire-conformance`, which owns holding this
/// repository to an outside authority. What is left here is that the file is
/// coherent and names batches the corpus actually carries.
pub(super) fn check(workspace: &Path, corpus: &Corpus) -> Result<usize, String> {
    let transcript: Transcript = read(&path(workspace))?;
    if transcript.schema != SCHEMA {
        return Err(format!(
            "spec/records/verified.json declares schema {}, not the supported {SCHEMA}",
            transcript.schema
        ));
    }

    for entry in &transcript.verified {
        if !corpus
            .vectors
            .iter()
            .any(|vector| vector.name == entry.name)
        {
            return Err(format!(
                "spec/records/verified.json carries `{}`, which vectors.json does not; \
                 refresh both with `cargo xtask records --refresh`",
                entry.name
            ));
        }
        if entry.hex.is_empty()
            || entry.hex.len() % 2 != 0
            || !entry.hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format!("{}: verified hex is not a byte string", entry.name));
        }
        if entry
            .kafka
            .get("batches")
            .and_then(serde_json::Value::as_array)
            .is_none_or(Vec::is_empty)
        {
            return Err(format!(
                "{}: the transcript records no batch Kafka read",
                entry.name
            ));
        }
    }

    Ok(transcript.verified.len())
}

/// Re-author the transcript by putting this repository's bytes to Kafka.
///
/// Every compressed batch is asked about, and only those. An uncompressed batch
/// is already held to Kafka's own bytes by byte identity, so asking Kafka to
/// read back what it wrote itself would prove nothing. A compressed one has no
/// such guarantee — three of the four codecs happen to reproduce Java's bytes
/// today and one does not, and that split is a property of four third-party
/// compressors rather than of this protocol, so it is not a thing to assert.
pub(super) fn refresh(workspace: &Path, corpus: &Corpus) -> Result<usize, String> {
    let mut asked: Vec<(Question, String)> = Vec::new();
    for vector in &corpus.vectors {
        let mut authored = from_hex(&vector.hex, &vector.name)?;
        let batch = RecordBatch::decode(&mut authored, RecordDecodeLimits::default())
            .map_err(|error| format!("{}: decode: {error}", vector.name))?;
        if !authored.is_empty() {
            return Err(format!(
                "{}: record oracle vector carries {} trailing byte(s) after its batch",
                vector.name,
                authored.len()
            ));
        }
        if batch.compression == Compression::None {
            continue;
        }
        let rewritten = batch
            .encode_to_bytes()
            .map_err(|error| format!("{}: re-encode: {error}", vector.name))?;
        let question = Question {
            name: vector.name.clone(),
            hex: to_hex(&rewritten),
        };
        asked.push((question, vector.why.clone()));
    }

    if asked.is_empty() {
        return Err(
            "the corpus carries no compressed batch, so there is nothing whose \
             compression Kafka can be asked to read"
                .to_owned(),
        );
    }

    let request = Questions {
        batches: asked.iter().map(|(question, _)| question.clone()).collect(),
    };
    let request = serde_json::to_string(&request)
        .map_err(|error| format!("serialize the verification request: {error}"))?;
    let answered = super::oracle::verify(workspace, &request)?;
    let readings: Readings = serde_json::from_str(&answered)
        .map_err(|error| format!("parse the record oracle's reading: {error}"))?;

    if readings.results.len() != asked.len() {
        return Err(format!(
            "asked Kafka to read {} batch(es) and it answered about {}",
            asked.len(),
            readings.results.len()
        ));
    }

    let mut verified = Vec::with_capacity(asked.len());
    for ((question, why), reading) in asked.into_iter().zip(&readings.results) {
        if question.name != reading.name {
            return Err(format!(
                "the oracle read `{}` where `{}` was asked; batch order is not reliable",
                reading.name, question.name
            ));
        }
        verified.push(Verified {
            name: question.name,
            why,
            hex: question.hex,
            kafka: reading.read.clone(),
        });
    }

    let written = Transcript {
        schema: SCHEMA,
        about: "What Apache Kafka's own MemoryRecords reader recovered from batches THIS \
                repository compressed. Unlike vectors.json the `hex` here is not Kafka's: it is \
                the output under judgement, pinned so a later comparison is against bytes Kafka \
                actually saw. Regenerate with `cargo xtask records --refresh`."
            .to_owned(),
        verified,
    };
    let text = serde_json::to_string_pretty(&written)
        .map_err(|error| format!("serialize the transcript: {error}"))?;
    std::fs::write(path(workspace), text + "\n")
        .map_err(|error| format!("write the transcript: {error}"))?;

    Ok(written.verified.len())
}

fn from_hex(text: &str, name: &str) -> Result<Bytes, String> {
    if text.len() % 2 != 0 {
        return Err(format!("{name}: hex has an odd number of digits"));
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let digits = std::str::from_utf8(pair).map_err(|error| format!("{name}: hex: {error}"))?;
        bytes
            .push(u8::from_str_radix(digits, 16).map_err(|error| format!("{name}: hex: {error}"))?);
    }
    Ok(Bytes::from(bytes))
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}
