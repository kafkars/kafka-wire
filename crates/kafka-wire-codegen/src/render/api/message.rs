//! Struct, default, and direction-trait rendering for one normalized message.

use kafka_wire_schema::{FieldType, Message, MessageKind};

use crate::{
    GenerationError,
    group::ApiGroup,
    render::{field, invariant, text::RustText},
};

use super::{
    codec::{render_decode, render_encode},
    descriptor::api_descriptor_name,
    imports::{ExternalSymbol as S, spell},
    prose::sentence,
    protocol_eq::render_protocol_eq,
    structs::render_declared_structs,
    validation::{Owner, render_validation},
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

    rust.doc_line(format!(
        "{direction} body for the `{}` API.",
        message.name.api_stem()
    ));
    rust.line("#[non_exhaustive]");
    let derive_default = message
        .fields
        .iter()
        .all(|member| field::uses_rust_default(member, message));
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
        rust.line(format!(
            "#[derive(Clone, Debug, {}, {equality})]",
            spell(message, S::Default)
        ));
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
        rust.line(format!(
            "pub unknown_tagged_fields: {},",
            spell(message, S::TaggedFields)
        ));
    }
    rust.close("");
    rust.blank();

    if !derive_default {
        render_default(rust, message);
    }
    render_protocol_eq(
        rust,
        message.name.rust_type(),
        &message.fields,
        message,
        !message.effective_flexible_versions().is_empty(),
    );
    render_metadata_impls(rust, message, group)?;
    render_validation(
        rust,
        message.name.rust_type(),
        &message.fields,
        message,
        Owner::Message,
        !message.effective_flexible_versions().is_empty(),
    );
    render_decode(rust, message)?;
    render_encode(rust, message)?;
    Ok(())
}

fn render_default(rust: &mut RustText, message: &Message) {
    rust.open(format!(
        "impl {} for {}",
        spell(message, S::Default),
        message.name.rust_type()
    ));
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
        rust.line(format!(
            "unknown_tagged_fields: {}::default(),",
            spell(message, S::TaggedFields)
        ));
    }
    rust.close("");
    rust.close("");
    rust.close("");
    rust.blank();
}

fn render_metadata_impls(
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
        MessageKind::Response => {
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
        // Rejected during grouping; the arm keeps the match total.
        MessageKind::Header | MessageKind::Data => {}
    }
    Ok(())
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
    // The one reference that crosses a module boundary, and it reads the
    // file-level flat re-export rather than the response's own module: the
    // re-export is what `kafka_wire::ProduceResponse` already resolves
    // to, so the pairing names exactly the type a caller names.
    rust.line(format!(
        "type Response = super::{};",
        group.response.message.name.rust_type()
    ));
    rust.close("");
    rust.blank();
}

fn option_range(
    message: &Message,
    versions: &kafka_wire_schema::VersionSet,
    range: &str,
) -> Result<String, GenerationError> {
    Ok(
        invariant::optional_bounded(message, versions, "effective flexible versions")?.map_or_else(
            || spell(message, S::None),
            |(start, end)| format!("{}({range}::new({start}, {end}))", spell(message, S::Some)),
        ),
    )
}
