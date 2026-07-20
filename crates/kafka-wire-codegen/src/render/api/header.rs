//! Emission for the schemas that answer to no API key.
//!
//! Kafka's request and response headers are the frame around a message rather
//! than a message: they carry no API key, no descriptor, and no request/response
//! pairing. What they do carry is a version range and a flexible window of their
//! own, which makes each one exactly the standalone struct `structs` already
//! knows how to emit — so this file owns the module around them and nothing
//! about how a field becomes Rust.

use kafka_wire_schema::FieldType;

use crate::{GenerationError, provenance::generated_banner, source::MessageSource};

use super::structs::{render_declared_structs, render_standalone};
use crate::render::text::RustText;

/// Renders every unkeyed schema into one module.
pub(crate) fn render_unkeyed(
    sources: &[MessageSource],
    commit: &str,
) -> Result<String, GenerationError> {
    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Framing and data schemas from Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.line("//!");
    rust.line("//! These answer to no API key: a header frames a message rather than being");
    rust.line("//! one, so nothing here carries a descriptor or a request/response pair.");
    rust.blank();
    render_imports(&mut rust, sources);
    for source in sources {
        render_declared_structs(&mut rust, &source.message)?;
        render_standalone(&mut rust, &source.message)?;
    }
    Ok(rust.finish())
}

fn render_imports(rust: &mut RustText, sources: &[MessageSource]) {
    let flexible = sources
        .iter()
        .any(|source| !source.message.effective_flexible_versions().is_empty());
    let mut wire = vec![
        "ApiVersion",
        "DecodeError",
        "Decoder",
        "EncodeError",
        "EncodeTarget",
        "Encoder",
        "KafkaDecode",
        "KafkaEncode",
    ];
    let uses = |ty: &FieldType| {
        sources
            .iter()
            .any(|source| crate::render::field::uses_type(&source.message, ty))
    };
    if sources
        .iter()
        .any(|source| crate::render::field::uses_bytes(&source.message))
    {
        wire.push("Bytes");
    }
    if uses(&FieldType::String) {
        wire.push("StrBytes");
    }
    if flexible {
        wire.push("TaggedFields");
    }
    if sources
        .iter()
        .any(|source| super::tagged::declares_a_tag(&source.message))
    {
        wire.push("KnownTags");
        wire.push("TagOutcome");
    }
    if uses(&FieldType::Uuid) {
        wire.push("Uuid");
    }
    wire.push("VersionRange");
    rust.line(format!("use kafka_wire_core::{{{}}};", wire.join(", ")));
    rust.blank();
    rust.line("use crate::KafkaMessage;");
    rust.blank();
}
