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

use super::codec::{local, render_array_body, render_array_encode, render_nullable_array_encode};

/// What a structure does with retained tags at a version carrying no section.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum LegacyTags {
    /// A message refuses: the caller handed it tags this version cannot carry,
    /// and writing the message without them would silently drop what a peer
    /// sent.
    Refuse,
    /// A struct says nothing, because the message that owns it already did.
    Ignore,
}

/// Whether this field travels in the tagged-field section rather than inline.
pub(super) fn is_tagged(field: &Field) -> bool {
    field.tag.is_some()
}

/// Whether anything this message emits declares a known tag.
///
/// Asked of the whole message, structs included, because the import list is
/// per file: a tag on one nested struct is enough to make the module name the
/// dispatch types, and a message with none must not name them at all.
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
fn known_tags(fields: &[Field]) -> Vec<(u32, &Field)> {
    let mut tagged = fields
        .iter()
        .filter_map(|field| field.tag.map(|tag| (tag, field)))
        .collect::<Vec<_>>();
    tagged.sort_by_key(|(tag, _)| *tag);
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
        rust.line("TaggedFields::default()");
        rust.close(";");
        return Ok(());
    }

    for (_, field) in &tagged {
        // Spelled with its type because there is no read branch to infer from:
        // a bare `-1` in a `let mut` is an `i32` whatever the field says.
        rust.line(format!(
            "let mut {}: {} = {};",
            local(field),
            field::rust_type(field, message),
            field::default_expression(field, message)
        ));
    }
    rust.line("let mut unknown_tagged_fields = TaggedFields::default();");
    rust.open("if Self::is_flexible(version)");
    rust.open("unknown_tagged_fields = decoder.read_tagged_fields_with(|tag, decoder| match tag");
    for (tag, field) in &tagged {
        render_tag_arm(rust, *tag, field, message)?;
    }
    // A tag this build has no arm for — or one arriving at a version where its
    // field does not exist — is kept verbatim rather than refused. That is
    // exactly what the section is for: an entry it cannot interpret survives
    // the round trip untouched, and "cannot interpret" includes "not here yet".
    rust.line("_ => Ok(TagOutcome::Retained),");
    rust.close(")?;");
    rust.close("");
    Ok(())
}

/// One `match` arm: the tag number, its version gate, and the value it reads.
fn render_tag_arm(
    rust: &mut RustText,
    tag: u32,
    field: &Field,
    message: &Message,
) -> Result<(), GenerationError> {
    let arm = match field::tagged_presence_condition(field, message) {
        Some(condition) => format!("{tag} if {condition} =>"),
        None => format!("{tag} =>"),
    };
    rust.open(arm);
    let name = local(field);
    if let FieldType::Array(element) = &field.ty {
        let (read, _) = field::element_codec(element, field, message)?;
        let (length, _) = field::array_length_codec(field, message);
        rust.open(format!("{name} ="));
        render_array_body(rust, &length, &read, field::is_nullable(field, message));
        rust.close(";");
    } else {
        rust.line(format!(
            "{name} = {};",
            field::read_expression(field, message)?
        ));
    }
    rust.line("Ok(TagOutcome::Decoded)");
    rust.close("");
    Ok(())
}

/// Writes the tagged-field section, merging known tags with retained ones.
pub(super) fn render_tagged_encode(
    rust: &mut RustText,
    fields: &[Field],
    message: &Message,
    legacy: LegacyTags,
) -> Result<(), GenerationError> {
    let tagged = known_tags(fields);
    rust.blank();
    rust.open("if Self::is_flexible(version)");
    if tagged.is_empty() {
        rust.line("encoder.write_tagged_fields(&self.unknown_tagged_fields)?;");
    } else {
        rust.line("let mut known = KnownTags::new();");
        for (tag, field) in &tagged {
            render_tag_write(rust, *tag, field, message)?;
        }
        rust.line("encoder.write_merged_tagged_fields(known, &self.unknown_tagged_fields)?;");
    }
    match legacy {
        LegacyTags::Refuse => {
            rust.reopen("} else if !self.unknown_tagged_fields.is_empty() {");
            rust.open("return Err(EncodeError::TaggedFieldsNotRepresentable");
            rust.line("message: Self::NAME,");
            rust.line("version,");
            rust.close(");");
            rust.close("");
        }
        LegacyTags::Ignore => rust.close(""),
    }
    Ok(())
}

/// One known tag's contribution: written only when present and non-default.
///
/// Omitting a default-valued tag is the whole point of the construct. The
/// section is sparse, and writing a tag that says nothing would inflate every
/// message for no information — so this reuses the same non-default test that
/// already decides whether a version-gated inline field is representable.
fn render_tag_write(
    rust: &mut RustText,
    tag: u32,
    field: &Field,
    message: &Message,
) -> Result<(), GenerationError> {
    let non_default = field::non_default_condition(field, message);
    let condition = match field::tagged_presence_condition(field, message) {
        Some(presence) => format!("{presence} && {non_default}"),
        None => non_default,
    };
    rust.open(format!("if {condition}"));
    rust.open(format!("known.write({tag}, |encoder|"));
    if let FieldType::Array(element) = &field.ty {
        let (_, write) = field::element_codec(element, field, message)?;
        let (_, length) = field::array_length_codec(field, message);
        let name = field.name.rust_field();
        if field::is_nullable(field, message) {
            render_nullable_array_encode(rust, name, &length, &write);
        } else {
            render_array_encode(rust, name, &length, &write);
        }
    } else {
        rust.line(field::write_statement(field, message)?);
    }
    rust.line("Ok(())");
    rust.close(")?;");
    rust.close("");
    Ok(())
}
