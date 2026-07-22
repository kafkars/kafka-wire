//! Emission for the known half of a tagged-field section.
//!
//! A flexible structure ends in one section carrying two populations. The tags
//! this build does not know are retained bytes and belong to `kafka-wire-core`; the
//! tags it does know are ordinary fields of the structure that happen to travel
//! in that section, and turning those into Rust is what this file owns.
//!
//! It owns nothing about ordering. The section is ascending across both
//! populations, which means known tags cannot be emitted as a block of their
//! own — that merge lives in `kafka-wire-core`, where the invariant is enforced once.
//! Known and unknown tags share one ordered merge.

use kafka_wire_schema::{Field, FieldType, Message};

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

use super::codec::{local, render_array_body};
use super::imports::{ExternalSymbol as S, spell};
use super::tagged_payload::tag_helper;

/// Whether this field travels in the tagged-field section rather than inline.
pub(super) fn is_tagged(field: &Field) -> bool {
    field.tag.is_some()
}

/// Whether anything this message emits declares a known tag.
///
/// Asked of the whole message, structs included, because the import list is
/// per message module and every struct a message declares lands in it: a tag on
/// one nested struct is enough to make the module name the dispatch types, and a
/// message with none must not name them at all.
pub(super) fn declares_a_tag(message: &Message) -> bool {
    fn any(fields: &[Field]) -> bool {
        fields
            .iter()
            .any(|field| is_tagged(field) || any(&field.fields))
    }
    any(&message.fields)
        || message
            .common_structs
            .iter()
            .any(|common| any(&common.fields))
}

/// The known tags of one field list, paired with their numbers, ascending.
///
/// Sorted here rather than trusted from the schema: declaration order is not
/// tag order, and the generated `match` and the writes both read better in the
/// order the wire uses.
pub(super) fn known_tags(fields: &[Field]) -> Vec<(u32, usize, &Field)> {
    let mut tagged = fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| field.tag.map(|tag| (tag, index, field)))
        .collect::<Vec<_>>();
    tagged.sort_by_key(|(tag, _, _)| *tag);
    tagged
}

/// Reads the tagged-field section, decoding known tags and retaining the rest.
///
/// Known tags are declared before the section is read and assigned from inside
/// the dispatch, rather than bound by it: a tag is optional on the wire, so the
/// local has to exist holding its default whether or not the peer sent it.
pub(super) fn render_tagged_decode(
    rust: &mut RustText,
    fields: &[Field],
    message: &Message,
) -> Result<(), GenerationError> {
    let tagged = known_tags(fields);
    if tagged.is_empty() {
        rust.open("let unknown_tagged_fields = if Self::is_flexible(version)");
        rust.line("decoder.read_tagged_fields()?");
        rust.reopen("} else {");
        rust.line(format!("{}::default()", spell(message, S::TaggedFields)));
        rust.close(";");
        return Ok(());
    }

    for (_, index, field) in &tagged {
        // Spelled with its type because there is no read branch to infer from:
        // a bare `-1` in a `let mut` is an `i32` whatever the field says.
        rust.line(format!(
            "let mut {}: {} = {};",
            local(*index),
            field::rust_type(field, message),
            field::default_expression(field, message)
        ));
    }
    rust.line(format!(
        "let mut unknown_tagged_fields = {}::default();",
        spell(message, S::TaggedFields)
    ));
    rust.open("if Self::is_flexible(version)");
    rust.open("unknown_tagged_fields = decoder.read_tagged_fields_with(|tag, decoder| match tag");
    for (tag, index, field) in &tagged {
        render_tag_arm(rust, *tag, *index, field, message)?;
    }
    // A tag this build has no arm for — or one arriving at a version where its
    // field does not exist — is kept verbatim rather than refused. That is
    // exactly what the section is for: an entry it cannot interpret survives
    // the round trip untouched, and "cannot interpret" includes "not here yet".
    rust.line(format!(
        "_ => {}({}::Retained),",
        spell(message, S::Ok),
        spell(message, S::TagOutcome)
    ));
    rust.close(")?;");
    rust.close("");
    Ok(())
}

/// One `match` arm: the tag number, its version gate, and the value it reads.
fn render_tag_arm(
    rust: &mut RustText,
    tag: u32,
    index: usize,
    field: &Field,
    message: &Message,
) -> Result<(), GenerationError> {
    let arm = match field::tagged_presence_condition(field, message) {
        Some(condition) => format!("{tag} if {condition} =>"),
        None => format!("{tag} =>"),
    };
    rust.open(arm);
    let name = local(index);
    if let FieldType::Array(element) = &field.ty {
        let (read, _) = field::element_codec(element, field, message)?;
        let (length, _) = field::array_length_codec(field, message);
        rust.open(format!("{name} ="));
        render_array_body(
            rust,
            message,
            &length,
            &read,
            field::is_nullable(field, message),
        );
        rust.close(";");
    } else {
        rust.line(format!(
            "{name} = {};",
            field::read_expression(field, message)?
        ));
    }
    rust.line(format!(
        "{}({}::Decoded)",
        spell(message, S::Ok),
        spell(message, S::TagOutcome)
    ));
    rust.close("");
    Ok(())
}

/// Writes the tagged-field section, merging known tags with retained ones.
pub(super) fn render_tagged_encode(rust: &mut RustText, fields: &[Field], message: &Message) {
    let tagged = known_tags(fields);
    rust.blank();
    rust.open("if Self::is_flexible(version)");
    if tagged.is_empty() {
        rust.line("encoder.write_tagged_fields(&self.unknown_tagged_fields)?;");
    } else {
        rust.line(format!(
            "let mut known = {}::new();",
            spell(message, S::KnownTags)
        ));
        for (tag, _, field) in &tagged {
            render_tag_claim(rust, *tag, field, message);
        }
        rust.line("encoder.write_merged_tagged_fields(");
        rust.line("    known,");
        rust.line("    &self.unknown_tagged_fields,");
        rust.open("    |tag, encoder| match tag");
        for (tag, _, _) in &tagged {
            rust.line(format!(
                "{tag} => Self::{}(self, encoder, version),",
                tag_helper(*tag)
            ));
        }
        rust.line("_ => unreachable!(\"KnownTags yielded an unmeasured tag\"),");
        rust.close("");
        rust.line(")?;");
    }
    rust.close("");
}

/// Claims one active tag and measures it only when its value is non-default.
///
/// Default omission keeps the section sparse but never relinquishes the tag
/// number: an active known tag cannot also be forwarded as retained unknown
/// state.
fn render_tag_claim(rust: &mut RustText, tag: u32, field: &Field, message: &Message) {
    let non_default = field::non_default_condition(field, message);
    let presence = field::tagged_presence_condition(field, message);
    if let Some(condition) = &presence {
        rust.open(format!("if {condition}"));
    }
    rust.line(format!("known.claim({tag})?;"));
    rust.open(format!("if {non_default}"));
    rust.line(format!(
        "known.measure({tag}, |encoder| Self::{}(self, encoder, version))?;",
        tag_helper(tag)
    ));
    rust.close("");
    if presence.is_some() {
        rust.close("");
    }
}
