//! Target-generic payload writers for generated known tagged fields.
//!
//! This file owns measuring and emitting one known tag's value. Ordering and
//! merging the complete tagged-field section remain in the tagged renderer and
//! the wire kernel.

use kafka_wire_schema::{Field, FieldType, Message};

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

use super::{
    codec::{render_array_encode, render_nullable_array_encode},
    imports::{ExternalSymbol as S, spell},
    tagged::known_tags,
};

pub(super) fn tag_helper(tag: u32) -> String {
    format!("__kw_encode_known_tag_{tag}")
}

/// Emits one generic direct writer per known tagged value.
///
/// The same helper is run first against `SizeTarget` and then against the outer
/// target, so the generated value logic exists once and payload bytes are never
/// staged in an intermediate buffer.
pub(super) fn render_known_tag_helpers(
    rust: &mut RustText,
    fields: &[Field],
    message: &Message,
) -> Result<(), GenerationError> {
    for (tag, _, field) in known_tags(fields) {
        let version = if field::encoded_value_uses_version(field, message) {
            "version"
        } else {
            "_version"
        };
        rust.line(format!(
            "fn {}<T: {}>(",
            tag_helper(tag),
            spell(message, S::EncodeTarget)
        ));
        rust.line("    &self,");
        rust.line(format!(
            "    encoder: &mut {}<T>,",
            spell(message, S::Encoder)
        ));
        rust.line(format!("    {version}: {},", spell(message, S::ApiVersion)));
        rust.open(format!(
            ") -> {}<(), {}>",
            spell(message, S::Result),
            spell(message, S::EncodeError)
        ));
        render_tag_payload(rust, field, message)?;
        rust.line(format!("{}(())", spell(message, S::Ok)));
        rust.close("");
        rust.blank();
    }
    Ok(())
}

fn render_tag_payload(
    rust: &mut RustText,
    field: &Field,
    message: &Message,
) -> Result<(), GenerationError> {
    if let FieldType::Array(element) = &field.ty {
        let (_, write) = field::element_codec(element, field, message)?;
        let (_, length) = field::array_length_codec(field, message);
        let name = field.name.rust_field();
        if field::is_nullable(field, message) {
            render_nullable_array_encode(rust, message, name, &length, &write);
        } else {
            render_array_encode(rust, name, &length, &write);
        }
    } else {
        rust.line(field::write_statement(field, message)?);
    }
    Ok(())
}
