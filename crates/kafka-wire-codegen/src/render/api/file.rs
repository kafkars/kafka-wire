//! Complete-file orchestration, provenance, and import rendering for one API key.
//!
//! the module-scoped naming rule makes the module the scope a nested struct name is unique in, so
//! this file owns two levels rather than one. Each message becomes a `pub mod`
//! holding its own imports, its own type, and every struct it declares under
//! upstream's spelling. The file level holds only what has to see both
//! directions at once: the flat re-export of each message type, and the
//! descriptors, which name those types and are what the crate facade exports.

use kafka_wire_schema::FieldType;

use crate::{GenerationError, group::ApiGroup, provenance::generated_banner};

use super::{
    descriptor::render_descriptor, imports, message::render_message, tagged::declares_a_tag,
};
use crate::render::{field, text::RustText};

pub(crate) fn render_api(group: &ApiGroup, commit: &str) -> Result<String, GenerationError> {
    for source in group.messages() {
        field::validate_supported(&source.message)?;
    }

    let mut rust = RustText::default();
    render_header(&mut rust, group, commit);

    for source in group.messages() {
        render_module_doc(&mut rust, &source.message);
        rust.open(format!("pub mod {}", source.message.name.rust_module()));
        render_imports(&mut rust, &source.message, group);
        render_message(&mut rust, &source.message, group)?;
        rust.close("");
        rust.blank();
    }

    // Descriptors sit outside the modules: they name both directions of the key
    // and are what `generated/mod.rs` re-exports, so they read the flat
    // re-exports below rather than reaching into a module.
    render_braced_use(&mut rust, "kafka_wire_core", &["VersionRange"]);
    rust.blank();
    render_braced_use(
        &mut rust,
        "crate",
        &["MessageDescriptor", "MessageDirection"],
    );
    rust.blank();

    for source in group.messages() {
        rust.line(format!(
            "pub use {}::{};",
            source.message.name.rust_module(),
            source.message.name.rust_type()
        ));
    }
    rust.blank();

    for source in group.messages() {
        render_descriptor(&mut rust, &source.message, group.api_key)?;
    }
    Ok(rust.finish())
}

fn render_header(rust: &mut RustText, group: &ApiGroup, commit: &str) {
    rust.line(generated_banner());
    rust.line("//!");
    match (&group.request, &group.response) {
        (Some(request), Some(response)) => {
            rust.line(format!(
                "//! API key {} from `{}` and `{}`",
                group.api_key, request.filename, response.filename
            ));
            rust.line(format!("//! at Apache Kafka commit `{commit}`."));
            rust.line(format!("//! Request SHA-256: `{}`.", request.sha256));
            rust.line(format!("//! Response SHA-256: `{}`.", response.sha256));
        }
        (Some(source), None) | (None, Some(source)) => {
            rust.line(format!(
                "//! API key {} from `{}` at Apache Kafka commit",
                group.api_key, source.filename
            ));
            rust.line(format!("//! `{commit}`."));
            rust.line(format!("//! Source SHA-256: `{}`.", source.sha256));
        }
        (None, None) => {}
    }
    rust.blank();
}

/// Renders the import block for one message's module.
///
/// Asked of the message alone rather than of the API group. Under a per-file
/// import block a request pulled in whatever its response happened to use, which
/// was harmless because nothing could clash; now each module binds names into a
/// scope that also holds upstream's own struct spellings, so a name imported
/// without being used is a name that can collide for no reason.
fn render_imports(rust: &mut RustText, message: &kafka_wire_schema::Message, group: &ApiGroup) {
    let flexible = !message.effective_flexible_versions().is_empty();
    let mut wire = vec![
        "ApiKey",
        "ApiVersion",
        "BytesMut",
        "DecodeError",
        "Decoder",
        "EncodeError",
        "EncodeTarget",
        "Encoder",
        "KafkaDecode",
        "KafkaEncode",
        "encode_into_with",
        "encoded_len_with",
    ];
    if field::uses_type(message, &FieldType::String) {
        wire.push("StrBytes");
    }
    if flexible {
        wire.push("TaggedFields");
    }
    // The known-tag machinery is pulled in only by a message that declares one,
    // so the many APIs with a purely unknown section name neither type.
    if declares_a_tag(message) {
        wire.push("KnownTags");
        wire.push("TagOutcome");
    }
    if field::uses_bytes(message) {
        wire.push("Bytes");
    }
    if field::uses_type(message, &FieldType::Uuid) {
        wire.push("Uuid");
    }
    wire.push("VersionRange");
    render_braced_use(
        rust,
        "kafka_wire_core",
        &imports::importable(message, &wire),
    );
    rust.blank();

    let mut local = vec!["KafkaMessage"];
    match message.kind {
        kafka_wire_schema::MessageKind::Request => {
            local.push("KafkaRequest");
            // The pairing is the one reference that crosses a module boundary,
            // and only a request writes it.
            if group.response.is_some() {
                local.push("RequestResponsePair");
            }
        }
        kafka_wire_schema::MessageKind::Response => local.push("KafkaResponse"),
        kafka_wire_schema::MessageKind::Header | kafka_wire_schema::MessageKind::Data => {}
    }
    local.sort_unstable();
    render_braced_use(rust, "crate", &imports::importable(message, &local));
    rust.blank();
}

/// Documents one message module: what it holds, and why it is a module at all.
///
/// The doc is the scope stated where a reader meets it. A nested struct here is
/// spelled as upstream spells it, which is unique in this module and deliberately
/// not across the crate, so the module path is part of the type's identity rather
/// than an implementation detail of where it happens to live.
pub(super) fn render_module_doc(rust: &mut RustText, message: &kafka_wire_schema::Message) {
    rust.line(format!(
        "/// `{}` and every struct it declares, under upstream's own names.",
        message.name.protocol()
    ));
    rust.line("///");
    rust.line(format!(
        "/// [`{0}`](crate::{0}) is re-exported flat, so this path never has to be",
        message.name.rust_type()
    ));
    rust.line("/// written to name the message itself.");
}

pub(super) fn render_braced_use(rust: &mut RustText, path: &str, items: &[&str]) {
    if items.is_empty() {
        return;
    }
    // Brace collapsing and line breaking are rustfmt's, not the emitter's.
    rust.line(format!("use {path}::{{{}}};", items.join(", ")));
}
