//! Static fuzz-dispatch rendering across every generated protocol message.
//!
//! This output belongs only to the fuzz harness. It deliberately adds no
//! erased decoder or fuzz-specific entry point to the public runtime crate.

use kafka_wire_schema::MessageKind;

use crate::{GenerationError, group::ApiGroup, provenance::generated_banner};

use super::{invariant, text::RustText};

pub(crate) fn render_fuzz_dispatch(
    groups: &[ApiGroup],
    commit: &str,
) -> Result<String, GenerationError> {
    let messages = groups
        .iter()
        .flat_map(ApiGroup::messages)
        .filter(|source| {
            matches!(
                source.message.kind,
                MessageKind::Request | MessageKind::Response
            ) && !source.message.valid_versions.is_empty()
        })
        .collect::<Vec<_>>();
    if messages.is_empty() {
        return Err(GenerationError::InternalInvariant {
            message: "<fuzz dispatch>".to_owned(),
            invariant: "enabled corpus contains no versioned request or response".to_owned(),
        });
    }

    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Fuzz-only message dispatch for Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.blank();
    rust.line("use kafka_wire_core::ApiVersion;");
    rust.blank();
    rust.line("use super::round_trip;");
    rust.blank();
    rust.open("pub(super) fn dispatch(message_selector: u16, version_selector: u16, body: &[u8])");
    rust.open(format!(
        "match usize::from(message_selector) % {}",
        messages.len()
    ));
    for (selector, source) in messages.iter().enumerate() {
        let message = &source.message;
        let (first, last) = invariant::bounded(message, &message.valid_versions, "valid versions")?;
        rust.line(format!(
            "{selector} => round_trip::<kafka_wire::{}>(body, \
             select_version(version_selector, {first}, {last})),",
            message.name.rust_type()
        ));
    }
    rust.line("_ => unreachable!(\"modulo result exceeded the generated dispatch table\"),");
    rust.close("");
    rust.close("");
    rust.blank();
    rust.open("fn select_version(selector: u16, first: i16, last: i16) -> ApiVersion");
    rust.line("let width = u16::try_from(i32::from(last) - i32::from(first) + 1).unwrap_or(1);");
    rust.line("let offset = i16::try_from(selector % width).unwrap_or(0);");
    rust.line("ApiVersion::new(first + offset)");
    rust.close("");
    Ok(rust.finish())
}
