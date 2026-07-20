//! Lowering preserves the order in which upstream declares its fields.
//!
//! Scenario: for every pinned schema, walk the raw JSONC and the lowered IR
//! side by side and assert the field names appear in the same sequence, at every
//! nesting depth.
//!
//! This is the one property the byte corpus structurally cannot check, and the
//! reason is worth stating. A vector proves that decoding Kafka's bytes and
//! re-encoding them reproduces those bytes — but a decoder and an encoder that
//! share a field order agree with each other whatever that order is. Swap two
//! adjacent `int32` fields in the IR and the decode reads them into the wrong
//! slots, the encode writes them back out of the same wrong slots, and the bytes
//! match perfectly while every value is attributed to the wrong field.
//!
//! The bytes cannot see it, so the schema has to. Upstream's declaration order
//! IS the wire order, so comparing the IR against the source array is not a
//! self-consistency check but a check against the authority.
//!
//! Two other halves of the same question are already answered elsewhere and are
//! named here so this file is not mistaken for covering them. Field NAMES are
//! checked by the byte oracle: every synthesized vector's JSON is keyed by this
//! repository's lowered names, and Kafka's own converter refuses a key it does
//! not recognise, so a misnamed field fails at mint time rather than here. A
//! field this repository invented that upstream does not declare would add bytes
//! Kafka never wrote, which the round trip catches.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use serde_json::Value;

use support::{corpus_root, exceptions};

/// Field names in the order the raw schema declares them, depth first.
fn raw_order(fields: &Value, into: &mut Vec<String>) {
    let Some(fields) = fields.as_array() else {
        return;
    };
    for field in fields {
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .expect("every declared field has a name");
        into.push(name.to_owned());
        if let Some(nested) = field.get("fields") {
            raw_order(nested, into);
        }
    }
}

/// The same walk over the lowered IR.
fn lowered_order(fields: &[kafka_wire_schema::Field], into: &mut Vec<String>) {
    for field in fields {
        into.push(field.name.protocol().to_owned());
        lowered_order(&field.fields, into);
    }
}

#[test]
fn every_schema_lowers_its_fields_in_declaration_order() {
    let root = corpus_root();
    let exceptions = exceptions();
    let mut checked = 0;
    let mut compared = 0_usize;
    let mut failures = Vec::new();

    let entries = std::fs::read_dir(&root).expect("read the vendored corpus");
    for entry in entries {
        let path = entry.expect("read a corpus entry").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }

        let Ok(message) = kafka_wire_schema::load_message_with(&path, &exceptions) else {
            // A schema the front end refuses has no lowered order to compare.
            continue;
        };
        let text = std::fs::read_to_string(&path).expect("read a schema");
        let raw: Value = serde_json::from_str(&strip_comments(&text)).expect("parse a schema");

        let mut expected = Vec::new();
        raw_order(raw.get("fields").unwrap_or(&Value::Null), &mut expected);
        for common in raw
            .get("commonStructs")
            .and_then(Value::as_array)
            .unwrap_or(&Vec::new())
        {
            raw_order(common.get("fields").unwrap_or(&Value::Null), &mut expected);
        }

        let mut actual = Vec::new();
        lowered_order(&message.fields, &mut actual);
        for common in &message.common_structs {
            lowered_order(&common.fields, &mut actual);
        }

        // A pruned field exists in no supported version and is deliberately
        // absent from the IR; see `load::prune_unreachable_fields`.
        expected.retain(|name| actual.contains(name));

        if expected != actual {
            failures.push(format!(
                "{}: lowering reordered the fields\n  declared: {expected:?}\n  lowered:  {actual:?}",
                path.file_name().unwrap_or_default().to_string_lossy()
            ));
        }
        compared += actual.len();
        checked += 1;
    }

    assert!(
        failures.is_empty(),
        "{} schema(s) lower their fields out of declaration order, which the byte \
         corpus cannot see:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert!(
        checked >= 190,
        "only {checked} schema(s) were compared; the walk is not reaching the corpus"
    );
    // Without this the comparison could pass vacuously: `retain` intersects the
    // declared list with the lowered one, so two empty lists agree. The corpus
    // has well over a thousand fields, and a walk that found few has stopped
    // measuring what this file claims to measure.
    assert!(
        compared > 1_000,
        "only {compared} field(s) were compared across {checked} schema(s); the walk          is agreeing with itself rather than checking anything"
    );
}

/// The vendored schemas are JSONC, so comments come out before `serde_json`.
fn strip_comments(text: &str) -> String {
    text.lines()
        .map(|line| match line.find("//") {
            // A `//` inside a string is not a comment; no pinned schema has one,
            // and a naive strip would silently corrupt the field list if one
            // appeared, so the case is refused rather than guessed at.
            Some(_) if line.matches('"').count() % 2 != 0 => {
                panic!("a schema line mixes a quote and a comment: {line}")
            }
            Some(at) => line[..at].to_owned(),
            None => line.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
