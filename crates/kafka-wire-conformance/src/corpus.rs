//! An independent reader for the checked-in byte-vector corpus.
//!
//! This module owns loading `spec/vectors/**/v*.json` into typed vectors, and
//! owns turning a vector's hex body back into bytes. It is written against the
//! file format rather than against the xtask that writes it: the two share no
//! code on purpose, so a reader and a writer that agree do so because the format
//! is stable, not because one imported the other's struct.
//!
//! It deliberately owns no judgement. It does not encode, decode, or compare
//! anything, and it holds no opinion about whether a vector is correct — every
//! `hex` here was authored by Apache Kafka's own writer and is treated as given.

use std::{fs, path::PathBuf};

use serde::Deserialize;

/// Format revision this reader understands.
const SCHEMA: u32 = 1;

/// Direction of one Kafka message, as its upstream schema declares it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Neither: the schema frames a message rather than being one.
    Framing,
    /// Client to server.
    Request,
    /// Server to client.
    Response,
}

/// One broker-authored byte vector.
#[derive(Clone, Debug, Deserialize)]
pub struct Vector {
    /// Plan case that produced this vector.
    pub name: String,
    /// Why this case earns its place in the corpus.
    pub why: String,
    /// Upstream protocol message name.
    pub message: String,
    /// Numeric Kafka API key, as reported by Kafka itself.
    #[serde(default)]
    pub api_key: Option<i16>,
    /// Request or response direction.
    pub direction: Direction,
    /// Version at which these bytes were written.
    pub version: i16,
    /// Whether this version uses flexible encoding, transcribed from the schema.
    pub flexible: bool,
    /// Canonical JSON value Kafka's own JSON converter consumed.
    pub json_value: serde_json::Value,
    /// Unknown tagged fields attached after conversion.
    #[serde(default)]
    pub unknown_tagged_fields: Vec<TaggedField>,
    /// Lowercase hex of the message body Kafka wrote.
    pub hex: String,
}

/// One unknown tagged field carried alongside a vector.
#[derive(Clone, Debug, Deserialize)]
pub struct TaggedField {
    /// Numeric tag.
    pub tag: u32,
    /// Raw payload, hex encoded.
    pub data_hex: String,
}

/// One version's vector file.
#[derive(Clone, Debug, Deserialize)]
struct VectorFile {
    schema: u32,
    vectors: Vec<Vector>,
}

/// Repository root, resolved from this crate's manifest location.
pub fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(std::path::Path::parent)
        .map_or_else(|| manifest.clone(), std::path::Path::to_path_buf)
}

/// Load every checked-in vector, in a stable message-then-version order.
///
/// A missing or unreadable corpus is an error rather than an empty result: a
/// conformance run that silently inspects nothing reports success, which is the
/// one failure mode this whole corpus exists to prevent.
pub fn load() -> Result<Vec<Vector>, String> {
    let root = workspace_root().join("spec").join("vectors");
    let mut paths = Vec::new();

    let messages = fs::read_dir(&root)
        .map_err(|error| format!("read vector corpus in {}: {error}", root.display()))?;
    for message in messages {
        let directory = message
            .map_err(|error| format!("read entry in {}: {error}", root.display()))?
            .path();
        if !directory.is_dir() {
            continue;
        }
        let versions = fs::read_dir(&directory)
            .map_err(|error| format!("read {}: {error}", directory.display()))?;
        for version in versions {
            let path = version
                .map_err(|error| format!("read entry in {}: {error}", directory.display()))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                paths.push(path);
            }
        }
    }
    paths.sort();

    let mut vectors = Vec::new();
    for path in paths {
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        let file: VectorFile = serde_json::from_str(&source)
            .map_err(|error| format!("parse {}: {error}", path.display()))?;
        if file.schema != SCHEMA {
            return Err(format!(
                "{}: vector schema {} is not the supported schema {SCHEMA}",
                path.display(),
                file.schema
            ));
        }
        vectors.extend(file.vectors);
    }

    Ok(vectors)
}

/// Decode a lowercase hex body into the bytes it stands for.
pub fn from_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err(format!("hex `{hex}` has an odd number of digits"));
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let digits = hex.as_bytes();
    for pair in digits.chunks_exact(2) {
        let high = digit(pair[0])?;
        let low = digit(pair[1])?;
        bytes.push(high << 4 | low);
    }
    Ok(bytes)
}

/// Render bytes as lowercase hex, so a failure prints in the corpus's spelling.
pub fn to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push(nibble(byte >> 4));
        hex.push(nibble(byte & 0x0f));
    }
    hex
}

fn digit(character: u8) -> Result<u8, String> {
    match character {
        b'0'..=b'9' => Ok(character - b'0'),
        b'a'..=b'f' => Ok(character - b'a' + 10),
        _ => Err(format!(
            "`{}` is not a lowercase hexadecimal digit",
            char::from(character)
        )),
    }
}

fn nibble(value: u8) -> char {
    char::from(if value < 10 {
        b'0' + value
    } else {
        b'a' + value - 10
    })
}
