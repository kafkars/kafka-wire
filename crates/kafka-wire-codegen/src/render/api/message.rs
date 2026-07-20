//! Struct, default, and direction-trait rendering for one normalized message.

use kafka_wire_schema::{FieldType, Message, MessageKind};

use crate::{
    GenerationError,
    group::ApiGroup,
    render::{field, text::RustText},
};

use super::{
    codec::{render_decode, render_encode},
    prose::sentence,
    structs::render_declared_structs,
};

pub(super) fn render_message(
    rust: &mut RustText,
    message: &Message,
    group: &ApiGroup,
) -> Result<(), GenerationError> {
    // Grouping already rejected every kind without a direction, so this is a
    // totality guard rather than a policy decision.
    let direction = match message.kind {
        MessageKind::Request => "Request",
        MessageKind::Response => "Response",
        MessageKind::Header | MessageKind::Data => return Ok(()),
    };
    render_declared_structs(rust, message)?;

    rust.line(format!(
        "/// {direction} body for the `{}` API.",
        message.name.api_stem()
    ));
    rust.line("#[non_exhaustive]");
    let derive_default = message.fields.iter().all(field::uses_rust_default);
    // `f64` is not `Eq`, and `Eq` does not propagate: a struct holding a
    // `Vec<T>` where `T` is not `Eq` cannot be either. Asked of the whole
    // message rather than these fields alone, so a container and the struct it
    // holds always agree. Conservative for a struct in a float-carrying message
    // that holds no float itself, which costs a derive and nothing else.
    let equality = if field::uses_type(message, &FieldType::Float64) {
        "PartialEq"
    } else {
        "Eq, PartialEq"
    };
    if derive_default {
        rust.line(format!("#[derive(Clone, Debug, Default, {equality})]"));
    } else {
        rust.line(format!("#[derive(Clone, Debug, {equality})]"));
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
    render_decode(rust, message)?;
    render_encode(rust, message)?;
    Ok(())
}

fn render_default(rust: &mut RustText, message: &Message) {
    rust.open(format!("impl Default for {}", message.name.rust_type()));
    rust.open("fn default() -> Self");
    rust.open("Self");
    for field in &message.fields {
        rust.line(format!(
            "{}: {},",
            field.name.rust_field(),
            field::default_expression(field, message)
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
                group.api_key
            ));
            rust.close("");
            rust.blank();
        }
        // Rejected during grouping; the arm keeps the match total.
        MessageKind::Header | MessageKind::Data => {}
    }
}

fn render_request_metadata(rust: &mut RustText, message: &Message, group: &ApiGroup) {
    rust.open(format!(
        "impl KafkaRequest for {}",
        message.name.rust_type()
    ));
    rust.line(format!(
        "const API_KEY: ApiKey = ApiKey::new({});",
        group.api_key
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
