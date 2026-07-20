//! This repository's lowered defaults agree with Apache Kafka's own, field by field.
//!
//! Scenario: for every message and every struct it declares, compare the value
//! Kafka's generated `<Message>Data` initializes each field to — recorded in
//! `spec/defaults.json` by `cargo xtask defaults` — against the `DefaultValue`
//! this repository's front end lowered the same schema to.
//!
//! This is the property the byte corpus is structurally blind to. A vector
//! proves that decoding Kafka's bytes and re-encoding reproduces them; where a
//! field is absent from a version the decoder substitutes its default and the
//! encoder compares against that same default and writes nothing. A wrong default
//! agrees with itself and the round trip stays green. The two sides here are
//! Kafka's `MessageDataGenerator` and this repository's `lower_default` — two
//! independent readings of one schema — so a disagreement is a real defect, not
//! a self-consistency artifact.
//!
//! Field NAMES are checked elsewhere, by the byte oracle refusing a JSON key it
//! does not recognise at mint time, and field ORDER by `kafka-wire-schema`'s
//! `field_order` test against upstream's declaration array. What is left to this
//! file is the value each absent field takes, for every field of every message.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod defaults_corpus;

use std::collections::BTreeMap;

use kafka_wire_schema::{DefaultValue, Field, Message};

use defaults_corpus::{
    DefaultKind, FIELDS, MESSAGES, MessageDefaults, STRUCTS, StructDefaults, load_transcript,
    lower_every_message,
};

/// Defaults where this repository deliberately reads the schema differently from
/// Kafka's generated class, each with the reason it is not a defect.
///
/// This list is exact. An entry that stops naming a real disagreement fails the
/// test, the same way `broker_authored_bytes.rs` self-audits its own list: a
/// divergence that is silently tolerated is a divergence no one is watching.
const RECORDED_DIVERGENCES: &[Divergence] = &[
    Divergence {
        message: "FetchSnapshotResponse",
        struct_name: "PartitionSnapshot",
        field: "unalignedRecords",
        reason: "\
Kafka's generator returns null for every records-typed field unconditionally, ignoring \
the declared default (FieldSpec.java, the `type.isRecords()` arm). unalignedRecords is \
not nullable in any version FetchSnapshotResponse supports, so Kafka's generated class \
holds a null it could never legally write. This repository cannot hold None in a \
non-Option field and does not try: it keeps the empty batch, which is what the wire \
actually carries. A quirk of Kafka's codegen, not a default worth copying.",
    },
    Divergence {
        message: "ShareFetchResponse",
        struct_name: "PartitionData",
        field: "records",
        reason: "\
The same unconditional `type.isRecords() -> null`. This records field declares \
nullableVersions \"0\", outside ShareFetchResponse's validVersions 1-2, so it is \
non-nullable across every supported version and lowers to a non-Option Bytes. Kafka's \
class again holds a null it cannot write; this repository keeps the empty batch rather \
than bending the type to reach a value it has no way to represent.",
    },
    Divergence {
        message: "ShareFetchRequest",
        struct_name: "FetchPartition",
        field: "partitionMaxBytes",
        reason: "\
partitionMaxBytes is declared \"versions\": \"0\" while ShareFetchRequest declares \
\"validVersions\": \"1-2\", so it exists in no supported version and the front end prunes \
it from the IR (load::prune_unreachable_fields; reviewed in \
spec/overrides/schema_exceptions.toml as KAFKA_SCHEMA_UNUSED_FIELD, dead weight left \
when KIP-932 dropped ShareFetch v0). Kafka's generator emits a Java field for it \
regardless of version, so its class carries a default this repository has no field to \
hold. This is the one divergence that is a field-set difference rather than a value \
one; if upstream makes the field reachable again the prune stops, the IR grows the \
field, and this entry stops matching — which is when it should fail.",
    },
];

#[test]
fn every_field_default_matches_kafkas_own() {
    let transcript = load_transcript();
    let lowered = lower_every_message();

    let mut disagreements = Vec::new();
    let mut structs = 0_usize;
    let mut fields = 0_usize;

    for message in &transcript.messages {
        structs += message.structs.len();
        fields += message
            .structs
            .iter()
            .map(|entry| entry.fields.len())
            .sum::<usize>();

        let Some(ir) = lowered.get(message.message.as_str()) else {
            disagreements.push(Disagreement {
                message: message.message.clone(),
                struct_name: String::new(),
                field: String::new(),
                detail: "Kafka reports this message but the front end lowered no such schema"
                    .to_owned(),
            });
            continue;
        };

        compare_message(message, ir, &mut disagreements);
    }

    assert_eq!(
        transcript.messages.len(),
        MESSAGES,
        "the transcript names {} message(s), not the {MESSAGES} measured; \
         a comparison over a truncated file proves nothing",
        transcript.messages.len()
    );
    assert_eq!(
        structs, STRUCTS,
        "the transcript walks {structs} struct(s), not the {STRUCTS} measured"
    );
    assert_eq!(
        fields, FIELDS,
        "the transcript walks {fields} field(s), not the {FIELDS} measured"
    );

    report(&disagreements);
}

/// Compare one message's whole struct tree, both directions at every level.
fn compare_message(reported: &MessageDefaults, ir: &Message, into: &mut Vec<Disagreement>) {
    let mut ours = Vec::new();
    collect_structs(ir, &mut ours);
    let ours: BTreeMap<&str, &[Field]> = ours.into_iter().collect();

    let mut theirs = BTreeMap::new();
    for entry in &reported.structs {
        theirs.insert(entry.struct_name.as_str(), entry);
    }

    // Struct-set disagreement, both directions. A struct one side declares and
    // the other does not is reported once, against the struct, before any field.
    for name in theirs.keys() {
        if !ours.contains_key(name) {
            into.push(disagree(
                &reported.message,
                name,
                "",
                "Kafka declares this struct; the lowered IR has no such struct",
            ));
        }
    }
    for name in ours.keys() {
        if !theirs.contains_key(name) {
            into.push(disagree(
                &reported.message,
                name,
                "",
                "the lowered IR declares this struct; Kafka reports no such struct",
            ));
        }
    }

    for (name, entry) in &theirs {
        if let Some(our_fields) = ours.get(name) {
            compare_struct(&reported.message, name, entry, our_fields, into);
        }
    }
}

/// Compare one struct's fields, matching by normalized name, both directions.
fn compare_struct(
    message: &str,
    struct_name: &str,
    reported: &StructDefaults,
    ours: &[Field],
    into: &mut Vec<Disagreement>,
) {
    let mut our_fields = BTreeMap::new();
    for field in ours {
        our_fields.insert(normalize(field.name.protocol()), field);
    }
    let mut their_fields = BTreeMap::new();
    for field in &reported.fields {
        their_fields.insert(normalize(&field.field), field);
    }

    for (key, field) in &their_fields {
        match our_fields.get(key) {
            None => into.push(disagree(
                message,
                struct_name,
                &field.field,
                "Kafka declares this field; the lowered IR has no such field",
            )),
            Some(ours) => {
                if !default_matches(&ours.default, &field.default) {
                    into.push(disagree(
                        message,
                        struct_name,
                        &field.field,
                        &format!(
                            "default disagrees: ours {}, Kafka {}",
                            describe_ours(&ours.default),
                            describe_kafka(&field.default),
                        ),
                    ));
                }
            }
        }
    }
    for (key, field) in &our_fields {
        if !their_fields.contains_key(key) {
            into.push(disagree(
                message,
                struct_name,
                field.name.protocol(),
                "the lowered IR declares this field; Kafka reports no such field",
            ));
        }
    }
}

/// Whether the IR default and Kafka's reported default denote the same value.
fn default_matches(ours: &DefaultValue, kafka: &DefaultKind) -> bool {
    match (ours, kafka) {
        (DefaultValue::Null, DefaultKind::Null)
        | (DefaultValue::Empty, DefaultKind::Empty)
        | (DefaultValue::StructDefaults, DefaultKind::Struct) => true,
        (DefaultValue::Bool(ours), DefaultKind::Bool { value }) => ours == value,
        (DefaultValue::Integer(ours), DefaultKind::Int { value }) => ours == value,
        // Compared by bits: the protocol question is whether the same declaration
        // was written on both sides, and IEEE equality answers a different one.
        (DefaultValue::Float(ours), DefaultKind::Float { value }) => {
            ours.get().to_bits() == value.to_bits()
        }
        (DefaultValue::String(ours), DefaultKind::String { value }) => ours == value,
        (DefaultValue::Uuid(ours), DefaultKind::Uuid { value }) => &base64url(ours) == value,
        _ => false,
    }
}

/// Enumerate every struct one message declares, keyed the way the oracle keys
/// them: the root by message name, each nested struct by upstream's own spelling.
fn collect_structs<'a>(message: &'a Message, into: &mut Vec<(&'a str, &'a [Field])>) {
    into.push((message.name.protocol(), &message.fields));
    for field in &message.fields {
        collect_nested(field, into);
    }
    for common in &message.common_structs {
        into.push((common.name.declared(), &common.fields));
        for field in &common.fields {
            collect_nested(field, into);
        }
    }
}

/// A struct-declaring field carries its members inline; a reference carries none.
fn collect_nested<'a>(field: &'a Field, into: &mut Vec<(&'a str, &'a [Field])>) {
    if field.declares_struct() {
        let name = field
            .ty
            .struct_reference()
            .expect("a struct-declaring field refers to a struct")
            .declared();
        into.push((name, &field.fields));
    }
    for child in &field.fields {
        collect_nested(child, into);
    }
}

/// Lowercase and drop underscores, so `partition_max_bytes`, `partitionMaxBytes`,
/// and `PartitionMaxBytes` are one name across the two spellings compared.
fn normalize(name: &str) -> String {
    name.chars()
        .filter(|character| *character != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

/// Kafka spells a uuid base64url without padding; the zero uuid is 22 `A`s.
fn base64url(bytes: &[u8; 16]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::with_capacity(22);
    for chunk in bytes.chunks(3) {
        let packed = chunk
            .iter()
            .enumerate()
            .fold(0_u32, |packed, (index, byte)| {
                packed | (u32::from(*byte) << (16 - index * 8))
            });
        for index in 0..=chunk.len() {
            encoded.push(char::from(
                ALPHABET[(packed >> (18 - index * 6) & 0x3f) as usize],
            ));
        }
    }
    encoded
}

/// Split the collected disagreements against the recorded divergences and fail
/// on anything left over in either direction.
fn report(disagreements: &[Disagreement]) {
    let mut unexpected = Vec::new();
    let mut matched = vec![false; RECORDED_DIVERGENCES.len()];

    for disagreement in disagreements {
        match RECORDED_DIVERGENCES
            .iter()
            .position(|divergence| divergence.covers(disagreement))
        {
            Some(index) => matched[index] = true,
            None => unexpected.push(disagreement.to_string()),
        }
    }

    let unused = RECORDED_DIVERGENCES
        .iter()
        .zip(&matched)
        .filter(|(_, hit)| !**hit)
        .map(|(divergence, _)| {
            format!(
                "{}/{}/{}: recorded as a deliberate divergence but Kafka and the IR now agree; \
                 remove the entry, whose recorded reason was:\n  {}",
                divergence.message, divergence.struct_name, divergence.field, divergence.reason
            )
        })
        .collect::<Vec<_>>();

    assert!(
        unexpected.is_empty() && unused.is_empty(),
        "the lowered defaults disagree with Apache Kafka's own beyond what is recorded:\n\n{}\n{}",
        unexpected.join("\n"),
        unused.join("\n"),
    );
}

// ---- disagreements and the recorded divergences that explain a few of them ----

/// One field- or struct-level disagreement, identified so an allowlist entry can
/// match it exactly and its diagnostic can name where to look.
struct Disagreement {
    message: String,
    struct_name: String,
    field: String,
    detail: String,
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}/{}/{}: {}",
            self.message, self.struct_name, self.field, self.detail
        )
    }
}

fn disagree(message: &str, struct_name: &str, field: &str, detail: &str) -> Disagreement {
    Disagreement {
        message: message.to_owned(),
        struct_name: struct_name.to_owned(),
        field: field.to_owned(),
        detail: detail.to_owned(),
    }
}

/// A deliberate reading difference, matched by identity against a disagreement.
///
/// `reason` is not matched against anything — it is the written justification a
/// reviewer reads, in the idiom of the file-size baselines in `architecture.toml`,
/// and it is surfaced when an entry goes stale so the maintainer knows what it
/// was protecting.
struct Divergence {
    message: &'static str,
    struct_name: &'static str,
    field: &'static str,
    reason: &'static str,
}

impl Divergence {
    /// A divergence covers a disagreement when it names the same field, matched
    /// with the same normalization the comparison uses.
    fn covers(&self, disagreement: &Disagreement) -> bool {
        self.message == disagreement.message
            && self.struct_name == disagreement.struct_name
            && normalize(self.field) == normalize(&disagreement.field)
    }
}

fn describe_ours(default: &DefaultValue) -> String {
    match default {
        DefaultValue::Null => "null".to_owned(),
        DefaultValue::Bool(value) => format!("bool {value}"),
        DefaultValue::Integer(value) => format!("int {value}"),
        DefaultValue::Float(value) => format!("float {}", value.get()),
        DefaultValue::String(value) => format!("string {value:?}"),
        DefaultValue::Uuid(value) => format!("uuid {}", base64url(value)),
        DefaultValue::Empty => "Empty".to_owned(),
        DefaultValue::StructDefaults => "struct".to_owned(),
    }
}

fn describe_kafka(default: &DefaultKind) -> String {
    match default {
        DefaultKind::Null => "null".to_owned(),
        DefaultKind::Bool { value } => format!("bool {value}"),
        DefaultKind::Int { value } => format!("int {value}"),
        DefaultKind::Float { value } => format!("float {value}"),
        DefaultKind::String { value } => format!("string {value:?}"),
        DefaultKind::Uuid { value } => format!("uuid {value}"),
        DefaultKind::Empty => "empty".to_owned(),
        DefaultKind::Struct => "struct".to_owned(),
    }
}
