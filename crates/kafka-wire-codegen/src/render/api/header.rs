//! Emission for the schemas that answer to no API key.
//!
//! Kafka's request and response headers are the frame around a message rather
//! than a message: they carry no API key, no descriptor, and no request/response
//! pairing. What they do carry is a version range and a flexible window of their
//! own, which makes each one exactly the standalone struct `structs` already
//! knows how to emit — so this file owns the module around them and nothing
//! about how a field becomes Rust.
//!
//! Each schema gets a module of its own, for the reason the module-scoped naming rule gives every
//! message one. These were rendered flat into a single `framing.rs`, and the
//! moment struct names went bare `TopicPartition` and `Voter` each collided
//! there — `LeaderChangeMessage` and `VotersRecord` both declare `Voter`, with
//! different fields. Measured by attempting it, not predicted.

use kafka_wire_schema::{FieldType, Message};

use crate::{GenerationError, provenance::generated_banner, source::MessageSource};

use super::file::render_braced_use;
use super::imports::{self, ExternalSymbol as S};
use super::structs::{render_declared_structs, render_standalone};
use crate::render::text::RustText;

/// Renders every unkeyed schema into one file, one module each.
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

    for source in sources {
        super::file::render_module_doc(&mut rust, &source.message);
        rust.open(format!("pub mod {}", source.message.name.rust_module()));
        render_imports(&mut rust, &source.message);
        render_declared_structs(&mut rust, &source.message)?;
        render_standalone(&mut rust, &source.message)?;
        rust.close("");
        rust.blank();
    }

    for source in sources {
        rust.line(format!(
            "pub use {}::{};",
            source.message.name.rust_module(),
            source.message.name.rust_type()
        ));
    }
    Ok(rust.finish())
}

fn render_imports(rust: &mut RustText, message: &Message) {
    let flexible = !message.effective_flexible_versions().is_empty();
    let mut wire = vec![
        S::ApiVersion,
        S::BytesMut,
        S::DecodeError,
        S::Decoder,
        S::EncodeError,
        S::EncodeTarget,
        S::Encoder,
        S::KafkaDecode,
        S::KafkaEncode,
        S::EncodeIntoWith,
        S::EncodedLenWith,
    ];
    let uses = |ty: &FieldType| crate::render::field::uses_type(message, ty);
    if crate::render::field::uses_bytes(message) {
        wire.push(S::Bytes);
    }
    if uses(&FieldType::String) {
        wire.push(S::StrBytes);
    }
    if flexible {
        wire.push(S::TaggedFields);
    }
    if super::tagged::declares_a_tag(message) {
        wire.push(S::KnownTags);
        wire.push(S::TagOutcome);
        wire.push(S::TaggedFieldsError);
    }
    if uses(&FieldType::Uuid) {
        wire.push(S::Uuid);
    }
    wire.push(S::VersionRange);
    render_braced_use(
        rust,
        "kafka_wire_core",
        &imports::importable(message, &wire),
    );
    rust.blank();
    render_braced_use(
        rust,
        "crate",
        &imports::importable(message, &[S::KafkaMessage, S::ProtocolEq]),
    );
    rust.blank();
}
