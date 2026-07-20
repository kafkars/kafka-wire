//! The default-value transcript, in the one direction it has.
//!
//! Kafka's generated `<Message>Data` initializes every field to the default its
//! schema declares, so a freshly constructed instance is upstream's own default
//! table. This module owns the `cargo xtask defaults` command that asks Kafka's
//! classes for that table and checks it in at `spec/defaults.json`, so the
//! conformance crate can hold this repository's lowered defaults to Kafka's two
//! independent readings of one schema rather than to itself.
//!
//! Unlike `vectors` and `records` there is no `--check` half here. Verifying the
//! transcript is a pure-Rust comparison against the lowered IR, which belongs to
//! `kafka-wire-conformance` and runs under `cargo test`; this module only authors,
//! needs a Java toolchain and the pinned jar, and is run by a human on purpose.
//!
//! It deliberately owns no judgement and computes no default. Every `kind` it
//! writes arrives from the oracle unmodified: a transcript this repository
//! derived from its own lowering would prove only that the lowering agrees with
//! itself.

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

mod oracle;

/// Format revision written here and required by the conformance crate.
const SCHEMA: u32 = 1;

/// The message list handed to the oracle, and the order the transcript keeps.
#[derive(Debug, Serialize)]
struct Request<'a> {
    messages: &'a [String],
}

/// The oracle's reply: one entry per message, in the order asked.
#[derive(Debug, Deserialize)]
struct Report {
    messages: Vec<MessageDefaults>,
}

/// The checked-in file. Named for what it is rather than its one collection, so
/// the serialized keys read `schema`, `about`, `messages` in that order.
#[derive(Debug, Serialize)]
struct Transcript {
    schema: u32,
    about: String,
    messages: Vec<MessageDefaults>,
}

/// One message and every struct it declares, keyed as this repository names them.
#[derive(Debug, Deserialize, Serialize)]
struct MessageDefaults {
    message: String,
    structs: Vec<StructDefaults>,
}

/// One struct's fields, keyed top-level by message name and nested by upstream's
/// own struct spelling, as the module-scoped naming rule scopes them.
#[derive(Debug, Deserialize, Serialize)]
struct StructDefaults {
    #[serde(rename = "struct")]
    struct_name: String,
    fields: Vec<FieldDefault>,
}

/// One field and the value Kafka's generated class initializes it to.
#[derive(Debug, Deserialize, Serialize)]
struct FieldDefault {
    field: String,
    java_type: String,
    default: DefaultKind,
}

/// A default tagged by kind, so the distinctions bare JSON erases survive.
///
/// An absent bytes field and an empty one both matter here, and `null` for a
/// string is a different claim than `""`. Kept as the oracle emits it and
/// re-serialized unchanged; the conformance crate owns comparing it to the IR.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum DefaultKind {
    Null,
    Bool { value: bool },
    Int { value: i64 },
    Float { value: f64 },
    String { value: String },
    Uuid { value: String },
    Empty,
    Struct,
}

pub(crate) fn run() -> Result<(), String> {
    refresh(&crate::workspace::root())
}

/// Re-author the transcript from the pinned jar.
fn refresh(workspace: &Path) -> Result<(), String> {
    let messages = message_names(workspace)?;
    let request = serde_json::to_string(&Request {
        messages: &messages,
    })
    .map_err(|error| format!("render the message list: {error}"))?;

    let answered = oracle::report(workspace, &request)?;
    let report: Report = serde_json::from_str(&answered)
        .map_err(|error| format!("parse the defaults oracle's answer: {error}\n{answered}"))?;

    if report.messages.len() != messages.len() {
        return Err(format!(
            "asked for {} message(s) and the oracle answered {}",
            messages.len(),
            report.messages.len()
        ));
    }
    for (asked, answered) in messages.iter().zip(&report.messages) {
        if asked != &answered.message {
            return Err(format!(
                "the oracle answered `{}` where `{asked}` was asked; message order is not reliable",
                answered.message
            ));
        }
    }

    let structs: usize = report.messages.iter().map(|m| m.structs.len()).sum();
    let fields: usize = report
        .messages
        .iter()
        .flat_map(|m| &m.structs)
        .map(|s| s.fields.len())
        .sum();

    let transcript = Transcript {
        schema: SCHEMA,
        about: "The value Apache Kafka's own generated `<Message>Data` classes initialize every \
                field to, which is upstream's schema default read from the class rather than the \
                schema. Regenerate with `cargo xtask defaults`; never edit a `kind` by hand. \
                `kafka-wire-conformance` compares it field by field against this repository's lowered \
                defaults."
            .to_owned(),
        messages: report.messages,
    };

    let mut rendered = serde_json::to_string_pretty(&transcript)
        .map_err(|error| format!("serialize the transcript: {error}"))?;
    rendered.push('\n');
    let path = workspace.join("spec").join("defaults.json");
    fs::write(&path, rendered).map_err(|error| format!("write {}: {error}", path.display()))?;

    println!(
        "refreshed spec/defaults.json: {} message(s), {structs} struct(s), {fields} field(s), \
         authored by Apache Kafka's own generated classes",
        messages.len()
    );
    Ok(())
}

/// The message names to ask about: every directory under `spec/vectors/`.
///
/// The vector corpus already names every message this repository authors bytes
/// for, one directory each, so it is the list of messages a defaults transcript
/// should cover too. Reading it here rather than re-listing the messages keeps
/// the two corpora from drifting into different message sets.
fn message_names(workspace: &Path) -> Result<Vec<String>, String> {
    let root = workspace.join("spec").join("vectors");
    let entries =
        fs::read_dir(&root).map_err(|error| format!("read {}: {error}", root.display()))?;

    let mut names = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("read an entry in {}: {error}", root.display()))?
            .path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| format!("{}: message directory has no name", path.display()))?;
            names.push(name.to_owned());
        }
    }
    names.sort();

    if names.is_empty() {
        return Err(format!(
            "no message directories under {}; a transcript with nothing to author \
             would leave the defaults corpus silently empty",
            root.display()
        ));
    }
    Ok(names)
}
