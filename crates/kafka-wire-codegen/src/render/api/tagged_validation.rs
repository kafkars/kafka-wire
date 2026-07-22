//! Generated validation for retained fields that overlap active known tags.
//!
//! General representability and recursive descent belong to `validation`; this
//! module owns only the tag-ownership phase and its richer diagnostic.

use kafka_wire_schema::{Field, Message};

use crate::render::{field, text::RustText};

use super::{
    imports::{ExternalSymbol as S, spell},
    tagged::known_tags,
    tagged_proof::RenderedKnownTags,
};

pub(super) fn render_known_tag_ownership(
    rust: &mut RustText,
    fields: &[Field],
    message: &Message,
    owner_name: &str,
) -> RenderedKnownTags {
    let mut rendered = RenderedKnownTags::default();
    if known_tags(fields).is_empty() {
        return rendered;
    }
    rust.open(format!(
        "pub(crate) fn validate_known_tag_ownership(&self, version: {}) -> {}<(), {}>",
        spell(message, S::ApiVersion),
        spell(message, S::Result),
        spell(message, S::EncodeError),
    ));
    for (tag, _, field) in known_tags(fields) {
        rendered.record(tag);
        let collision = format!("self.unknown_tagged_fields.contains_tag({tag})");
        let condition = field::presence_condition(field, message)
            .map_or(collision.clone(), |presence| {
                format!("{} && {collision}", field::as_conjunct(&presence))
            });
        rust.open(format!("if {condition}"));
        rust.open(format!(
            "return {}({}::KnownTagConflict",
            spell(message, S::Err),
            spell(message, S::EncodeError)
        ));
        rust.line(format!("message: {owner_name},"));
        rust.line(format!("tag: {tag},"));
        rust.line("version,");
        rust.close(");");
        rust.close("");
    }
    rust.line(format!("{}(())", spell(message, S::Ok)));
    rust.close("");
    rust.blank();
    rendered
}
