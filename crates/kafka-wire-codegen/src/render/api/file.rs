//! Complete-file orchestration, provenance, and import rendering for one API key.

use crate::{GenerationError, group::ApiGroup, provenance::generated_banner};

use super::{descriptor::render_descriptor, message::render_message};
use crate::render::{field, text::RustText};

pub(crate) fn render_api(group: &ApiGroup, commit: &str) -> Result<String, GenerationError> {
    for source in group.messages() {
        field::validate_supported(&source.message)?;
    }

    let mut rust = RustText::default();
    render_header(&mut rust, group, commit);
    render_imports(&mut rust, group);
    for source in group.messages() {
        render_message(&mut rust, &source.message, group)?;
    }
    for source in group.messages() {
        render_descriptor(&mut rust, &source.message, group.api_key);
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

fn render_imports(rust: &mut RustText, group: &ApiGroup) {
    let has_flexible = group
        .messages()
        .any(|source| !source.message.effective_flexible_versions().is_empty());
    let mut wire = vec![
        "ApiKey",
        "ApiVersion",
        "DecodeError",
        "Decoder",
        "EncodeError",
        "EncodeTarget",
        "Encoder",
        "KafkaDecode",
        "KafkaEncode",
        "StrBytes",
    ];
    if has_flexible {
        wire.push("TaggedFields");
    }
    if group
        .messages()
        .any(|source| field::uses_uuid(&source.message))
    {
        wire.push("Uuid");
    }
    wire.push("VersionRange");
    render_braced_use(rust, "kafka_wire_core", &wire);
    rust.blank();

    let mut local = vec!["KafkaMessage", "MessageDescriptor", "MessageDirection"];
    if group.request.is_some() {
        local.push("KafkaRequest");
    }
    if group.response.is_some() {
        local.push("KafkaResponse");
    }
    if group.request.is_some() && group.response.is_some() {
        local.push("RequestResponsePair");
    }
    local.sort_unstable();
    render_braced_use(rust, "crate", &local);
    rust.blank();
}

fn render_braced_use(rust: &mut RustText, path: &str, items: &[&str]) {
    // Brace collapsing and line breaking are rustfmt's, not the emitter's.
    rust.line(format!("use {path}::{{{}}};", items.join(", ")));
}
