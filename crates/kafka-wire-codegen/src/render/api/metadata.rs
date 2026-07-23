//! Direction traits and version metadata for one generated message.

use kafka_wire_schema::{Message, MessageKind, VersionSet};

use crate::{GenerationError, group::ApiGroup, render::invariant};

use super::{
    descriptor::api_descriptor_name,
    imports::{ExternalSymbol as S, spell},
};
use crate::render::text::RustText;

pub(super) fn render_metadata(
    rust: &mut RustText,
    message: &Message,
    group: &ApiGroup,
) -> Result<(), GenerationError> {
    let (start, end) = invariant::bounded(message, &message.valid_versions, "valid versions")?;
    let range = spell(message, S::VersionRange);
    let flexible = option_range(message, &message.effective_flexible_versions(), &range)?;
    rust.open(format!(
        "impl {} for {}",
        spell(message, S::KafkaMessage),
        message.name.rust_type()
    ));
    rust.line(format!(
        "const NAME: &'static str = {:?};",
        message.name.protocol()
    ));
    rust.line(format!(
        "const SUPPORTED_VERSIONS: {range} = {range}::new({start}, {end});"
    ));
    rust.line(format!(
        "const FLEXIBLE_VERSIONS: {}<{range}> = {flexible};",
        spell(message, S::Option)
    ));
    rust.close("");
    rust.blank();

    match message.kind {
        MessageKind::Request => render_request_metadata(rust, message, group),
        MessageKind::Response => render_response_metadata(rust, message, group),
        MessageKind::Header | MessageKind::Data => {}
    }
    Ok(())
}

fn render_response_metadata(rust: &mut RustText, message: &Message, group: &ApiGroup) {
    rust.open(format!(
        "impl {} for {}",
        spell(message, S::KafkaResponse),
        message.name.rust_type()
    ));
    rust.line(format!(
        "const API_KEY: {0} = {0}::new({1});",
        spell(message, S::ApiKey),
        group.api_key
    ));
    rust.close("");
    rust.blank();
}

fn render_request_metadata(rust: &mut RustText, message: &Message, group: &ApiGroup) {
    rust.open(format!(
        "impl {} for {}",
        spell(message, S::KafkaRequest),
        message.name.rust_type()
    ));
    rust.line(format!(
        "const API_KEY: {0} = {0}::new({1});",
        spell(message, S::ApiKey),
        group.api_key
    ));
    rust.line(format!(
        "const API_DESCRIPTOR: &'static {} = &super::{};",
        spell(message, S::ApiDescriptor),
        api_descriptor_name(group)
    ));
    rust.close("");
    rust.blank();

    rust.open(format!(
        "impl {} for {}",
        spell(message, S::RequestResponsePair),
        message.name.rust_type()
    ));
    rust.line(format!(
        "type Response = super::{};",
        group.response.message.name.rust_type()
    ));
    rust.close("");
    rust.blank();
}

fn option_range(
    message: &Message,
    versions: &VersionSet,
    range: &str,
) -> Result<String, GenerationError> {
    Ok(
        invariant::optional_bounded(message, versions, "effective flexible versions")?.map_or_else(
            || spell(message, S::None),
            |(start, end)| format!("{}({range}::new({start}, {end}))", spell(message, S::Some)),
        ),
    )
}
