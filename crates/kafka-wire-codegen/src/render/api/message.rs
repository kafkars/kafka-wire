//! Struct, default, and direction-trait rendering for one normalized message.

use kafka_wire_schema::{Message, MessageKind};

use crate::{
    group::ApiGroup,
    render::{field, text::RustText},
};

use super::codec::{render_decode, render_encode};

pub(super) fn render_message(rust: &mut RustText, message: &Message, group: &ApiGroup) {
    let direction = match message.kind {
        MessageKind::Request => "Request",
        MessageKind::Response => "Response",
    };
    rust.line(format!(
        "/// {direction} body for the `{}` API.",
        message.name.api_stem()
    ));
    rust.line("#[non_exhaustive]");
    let derive_default = message.fields.iter().all(field::uses_rust_default);
    if derive_default {
        rust.line("#[derive(Clone, Debug, Default, Eq, PartialEq)]");
    } else {
        rust.line("#[derive(Clone, Debug, Eq, PartialEq)]");
    }
    rust.open(format!("pub struct {}", message.name.rust_type()));
    for field in &message.fields {
        rust.line(format!("/// {}", sentence(&field.about)));
        rust.line(format!(
            "pub {}: {},",
            field.name.rust_field(),
            field::rust_type(field, message)
        ));
    }
    if !message.effective_flexible_versions().is_empty() {
        rust.line("/// Unknown flexible-version tagged fields retained for forwarding.");
        rust.line("pub unknown_tagged_fields: TaggedFields,");
    }
    rust.close("");
    rust.blank();

    if !derive_default {
        render_default(rust, message);
    }
    render_metadata_impls(rust, message, group);
    render_decode(rust, message);
    render_encode(rust, message);
}

fn render_default(rust: &mut RustText, message: &Message) {
    rust.open(format!("impl Default for {}", message.name.rust_type()));
    rust.open("fn default() -> Self");
    rust.open("Self");
    for field in &message.fields {
        rust.line(format!(
            "{}: {},",
            field.name.rust_field(),
            field::default_expression(field)
        ));
    }
    if !message.effective_flexible_versions().is_empty() {
        rust.line("unknown_tagged_fields: TaggedFields::default(),");
    }
    rust.close("");
    rust.close("");
    rust.close("");
    rust.blank();
}

fn render_metadata_impls(rust: &mut RustText, message: &Message, group: &ApiGroup) {
    let (start, end) = message.valid_versions.single_bounded().unwrap_or((0, 0));
    let flexible = option_range(&message.effective_flexible_versions());
    rust.open(format!(
        "impl KafkaMessage for {}",
        message.name.rust_type()
    ));
    rust.line(format!(
        "const NAME: &'static str = {:?};",
        message.name.protocol()
    ));
    rust.line(format!(
        "const SUPPORTED_VERSIONS: VersionRange = VersionRange::new({start}, {end});"
    ));
    rust.line(format!(
        "const FLEXIBLE_VERSIONS: Option<VersionRange> = {flexible};"
    ));
    rust.close("");
    rust.blank();

    match message.kind {
        MessageKind::Request => render_request_metadata(rust, message, group),
        MessageKind::Response => {
            rust.open(format!(
                "impl KafkaResponse for {}",
                message.name.rust_type()
            ));
            rust.line(format!(
                "const API_KEY: ApiKey = ApiKey::new({});",
                message.api_key
            ));
            rust.close("");
            rust.blank();
        }
    }
}

fn render_request_metadata(rust: &mut RustText, message: &Message, group: &ApiGroup) {
    rust.open(format!(
        "impl KafkaRequest for {}",
        message.name.rust_type()
    ));
    rust.line(format!(
        "const API_KEY: ApiKey = ApiKey::new({});",
        message.api_key
    ));
    rust.close("");
    rust.blank();

    if let Some(response) = &group.response {
        rust.open(format!(
            "impl RequestResponsePair for {}",
            message.name.rust_type()
        ));
        rust.line(format!(
            "type Response = {};",
            response.message.name.rust_type()
        ));
        rust.close("");
        rust.blank();
    }
}

fn option_range(versions: &kafka_wire_schema::VersionSet) -> String {
    versions.single_bounded().map_or_else(
        || "None".to_owned(),
        |(start, end)| format!("Some(VersionRange::new({start}, {end}))"),
    )
}

fn sentence(source: &str) -> String {
    let source = source
        .split_whitespace()
        .map(mark_protocol_identifier)
        .collect::<Vec<_>>()
        .join(" ");
    if source.ends_with('.') || source.ends_with('!') || source.ends_with('?') {
        source
    } else {
        format!("{source}.")
    }
}

fn mark_protocol_identifier(token: &str) -> String {
    if token.contains('`') {
        return token.to_owned();
    }
    let is_identifier = |character: char| character.is_alphanumeric() || character == '_';
    let Some(start) = token.find(is_identifier) else {
        return token.to_owned();
    };
    let Some(last) = token.rfind(is_identifier) else {
        return token.to_owned();
    };
    let end = last + token[last..].chars().next().map_or(0, char::len_utf8);
    let identifier = &token[start..end];
    let mut characters = identifier.chars();
    let _ = characters.next();
    let has_internal_uppercase = characters.any(char::is_uppercase);
    let has_lowercase = identifier.chars().any(char::is_lowercase);
    let is_plain_identifier = identifier.chars().all(is_identifier);
    if has_internal_uppercase && has_lowercase && is_plain_identifier {
        format!("{}`{identifier}`{}", &token[..start], &token[end..])
    } else {
        token.to_owned()
    }
}
