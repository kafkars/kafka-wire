//! Acquiring the two corpora the defaults comparison reads, each independently.
//!
//! This file owns reading the two things `broker_authored_defaults` holds against
//! each other: Apache Kafka's transcript at `spec/defaults.json`, read straight
//! from the file format without sharing code with the xtask that writes it, and
//! this repository's lowered IR, read by driving `kafka-wire-schema` over the vendored
//! corpus. It also records the corpus's measured shape, so the proof can refuse
//! to run against a truncated file.
//!
//! It deliberately owns no judgement. Whether the two readings agree is the
//! proof's job; this file only puts both in front of it.

use std::{collections::BTreeMap, fs, path::PathBuf};

use kafka_wire_conformance::workspace_root;
use kafka_wire_schema::{Message, SchemaException, SchemaExceptions, load_message_with};
use serde::Deserialize;

/// The measured shape of the transcript, asserted by the proof so it cannot pass
/// on a truncated or empty file — the one failure mode a comparison like this
/// hides. Independently equal to the module-scoped naming rule's separately measured 501 types.
pub(crate) const MESSAGES: usize = 193;
pub(crate) const STRUCTS: usize = 501;
pub(crate) const FIELDS: usize = 1670;

/// Format revision this reader understands.
const SCHEMA: u32 = 1;

/// The whole `spec/defaults.json` file.
#[derive(Debug, Deserialize)]
pub(crate) struct Transcript {
    schema: u32,
    pub(crate) messages: Vec<MessageDefaults>,
}

/// One message and every struct it declares, keyed as this repository names them.
#[derive(Debug, Deserialize)]
pub(crate) struct MessageDefaults {
    pub(crate) message: String,
    pub(crate) structs: Vec<StructDefaults>,
}

/// One struct's fields, keyed top-level by message name and nested by upstream's
/// own struct spelling, as the module-scoped naming rule scopes them.
#[derive(Debug, Deserialize)]
pub(crate) struct StructDefaults {
    #[serde(rename = "struct")]
    pub(crate) struct_name: String,
    pub(crate) fields: Vec<FieldDefault>,
}

/// One field and the value Kafka's generated class initializes it to.
#[derive(Debug, Deserialize)]
pub(crate) struct FieldDefault {
    pub(crate) field: String,
    pub(crate) default: DefaultKind,
}

/// A default tagged by kind, matching what `Oracle.java --defaults` emits.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(crate) enum DefaultKind {
    Null,
    Bool { value: bool },
    Int { value: i64 },
    Float { value: f64 },
    String { value: String },
    Uuid { value: String },
    Empty,
    Struct,
}

/// Read Kafka's checked-in transcript, refusing an unrecognised schema.
pub(crate) fn load_transcript() -> Transcript {
    let path = workspace_root().join("spec").join("defaults.json");
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let transcript: Transcript = serde_json::from_str(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    assert_eq!(
        transcript.schema, SCHEMA,
        "spec/defaults.json declares schema {}, not the supported {SCHEMA}",
        transcript.schema
    );
    transcript
}

/// Lower every vendored schema and key it by message name for lookup.
///
/// The reviewed upstream defects are accepted here for the same reason the
/// front-end corpus tests accept them: two messages fail an invariant this
/// repository is right to enforce, and are exempted by name rather than by
/// weakening a rule. Without them those two messages would not lower and the
/// transcript's totals could not be met.
pub(crate) fn lower_every_message() -> BTreeMap<String, Message> {
    let exceptions = exceptions();
    let mut lowered = BTreeMap::new();
    for path in schema_files() {
        if let Ok(message) = load_message_with(&path, &exceptions) {
            lowered.insert(message.name.protocol().to_owned(), message);
        }
    }
    assert!(
        lowered.len() >= MESSAGES,
        "only {} schema(s) lowered; the walk is not reaching the corpus",
        lowered.len()
    );
    lowered
}

/// The one vendored commit tree, discovered rather than pinned by SHA here.
fn corpus_root() -> PathBuf {
    let vendored = workspace_root()
        .join("spec")
        .join("upstream")
        .join("apache-kafka");
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
        "expected one vendored commit tree under {}, found {commits:?}",
        vendored.display()
    );
    commits.remove(0).join("message")
}

fn schema_files() -> Vec<PathBuf> {
    let mut files = fs::read_dir(corpus_root())
        .expect("read the vendored corpus")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

/// Read the reviewed upstream defects from `spec/overrides`, as the schema tests do.
fn exceptions() -> SchemaExceptions {
    #[derive(Deserialize)]
    struct Overrides {
        accepted: Vec<Accepted>,
    }
    #[derive(Deserialize)]
    struct Accepted {
        message: String,
        field: Option<String>,
        code: String,
        reason: String,
        upstream: String,
    }

    let path = workspace_root()
        .join("spec")
        .join("overrides")
        .join("schema_exceptions.toml");
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
