//! Struct, default, and direction-trait rendering for one normalized message.

use kafka_wire_schema::{FieldType, Message, MessageKind};

use crate::{
    GenerationError,
    group::ApiGroup,
    render::{field, tag_plan::known_tag_plans, text::RustText},
};

use super::{
    codec::{render_decode, render_encode},
    imports::{ExternalSymbol as S, spell},
    metadata::render_metadata,
    prose::sentence,
    protocol_eq::render_protocol_eq,
    retained_size::render_retained_size,
    structs::render_declared_structs,
    tagged_proof::verify_known_tag_rendering,
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
    render_retained_size(
        rust,
        message.name.rust_type(),
        &message.fields,
        message,
        !message.effective_flexible_versions().is_empty(),
    );
    render_metadata(rust, message, group)?;
    let tag_plans = known_tag_plans(&message.fields, message);
    let validated_tags = render_validation(
        rust,
        message.name.rust_type(),
        &message.fields,
        message,
        Owner::Message,
        !message.effective_flexible_versions().is_empty(),
        &tag_plans,
    );
    let decoded_tags = render_decode(rust, message, &tag_plans)?;
    let encoded_tags = render_encode(rust, message, &tag_plans)?;
    verify_known_tag_rendering(
        &tag_plans,
        message,
        message.name.rust_type(),
        &decoded_tags,
        &validated_tags,
        &encoded_tags,
    )?;
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
