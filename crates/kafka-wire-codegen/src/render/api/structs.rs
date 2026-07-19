//! Emission for the structs a message declares.
//!
//! This file owns turning one message's `commonStructs` and inline field bodies
//! into Rust: the struct definition, its default, and its codecs. It
//! deliberately owns no message-level concern — a struct has no API key, no
//! supported version range, and no descriptor, so nothing here emits one.

use kafka_wire_schema::{Field, FieldType, Message, StructRef};

use crate::{
    GenerationError,
    render::{field, text::RustText},
};

use super::codec::{render_construction, render_reads, render_writes};
use super::prose::sentence;

/// Renders every struct this message declares, in protocol declaration order.
pub(super) fn render_declared_structs(
    rust: &mut RustText,
    message: &Message,
) -> Result<(), GenerationError> {
    for (rust_type, fields) in declared_structs(message) {
        render_struct(rust, &rust_type, fields, message)?;
    }
    Ok(())
}

/// Every struct this message declares, paired with its members, in protocol
/// declaration order.
///
/// `commonStructs` and an inline body are two spellings of one concept, but the
/// members live in different places: a common struct carries its own field
/// list, while an inline body hangs off the field that declares the shape. This
/// walks both so the emitter sees one ordered list.
pub(super) fn declared_structs(message: &Message) -> Vec<(String, &[Field])> {
    let mut declared = Vec::new();
    for common in &message.common_structs {
        declared.push((common.name.rust_type().to_owned(), common.fields.as_slice()));
    }
    collect_inline(&message.fields, &mut declared);
    declared
}

fn collect_inline<'a>(fields: &'a [Field], declared: &mut Vec<(String, &'a [Field])>) {
    for field in fields {
        if field.fields.is_empty() {
            continue;
        }
        if let Some(reference) = struct_reference(&field.ty) {
            declared.push((reference.rust_type().to_owned(), field.fields.as_slice()));
        }
        collect_inline(&field.fields, declared);
    }
}

/// The struct a type denotes, looking through an array element.
fn struct_reference(ty: &FieldType) -> Option<&StructRef> {
    match ty {
        FieldType::Struct(reference) => Some(reference),
        FieldType::Array(element) => struct_reference(element),
        _ => None,
    }
}

/// Renders one declared struct: its definition, its default, and its codecs.
///
/// A struct gets no `KafkaMessage` impl. It has no API key, no supported range,
/// and no name of its own on the wire; it is a shape its owning message reads
/// at the version the message already validated.
fn render_struct(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[Field],
    message: &Message,
) -> Result<(), GenerationError> {
    rust.line(format!(
        "/// `{rust_type}` as declared by the `{}` API.",
        message.name.api_stem()
    ));
    rust.line("#[non_exhaustive]");
    let derive_default = fields.iter().all(field::uses_rust_default);
    if derive_default {
        rust.line("#[derive(Clone, Debug, Default, Eq, PartialEq)]");
    } else {
        rust.line("#[derive(Clone, Debug, Eq, PartialEq)]");
    }
    rust.open(format!("pub struct {rust_type}"));
    for member in fields {
        rust.line(format!("/// {}", sentence(&member.about)));
        rust.line(format!(
            "pub {}: {},",
            member.name.rust_field(),
            field::rust_type(member, message)?
        ));
    }
    rust.close("");
    rust.blank();

    if !derive_default {
        rust.open(format!("impl Default for {rust_type}"));
        rust.open("fn default() -> Self");
        rust.open("Self");
        for member in fields {
            rust.line(format!(
                "{}: {},",
                member.name.rust_field(),
                field::default_expression(member, message)?
            ));
        }
        rust.close("");
        rust.close("");
        rust.close("");
        rust.blank();
    }

    render_struct_decode(rust, rust_type, fields, message)?;
    render_struct_encode(rust, rust_type, fields, message)?;
    Ok(())
}

/// Whether a struct body mentions the version it was handed.
///
/// A struct with no gated member, no flexible split, and no nested struct never
/// reads its version. Binding it as `version` there would emit an unused
/// variable into checked-in source, which the workspace lints deny, so the
/// binding is named for what the body actually does with it.
fn body_uses_version(
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<bool, GenerationError> {
    for field in fields {
        if field::presence_condition(field, message).is_some() {
            return Ok(true);
        }
        let (read, write) = if let FieldType::Array(element) = &field.ty {
            field::element_codec(element, field, message)?
        } else {
            (
                field::read_expression(field, message)?,
                field::write_statement(field, message)?,
            )
        };
        if read.contains("version") || write.contains("version") {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Decode body for one struct a message declares.
///
/// A struct carries no version check of its own: the message validated the
/// version before any member was read, and re-checking here would claim the
/// struct has a supported range independent of its owner, which it does not.
pub(super) fn render_struct_decode(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<(), GenerationError> {
    let version = if body_uses_version(fields, message)? {
        "version"
    } else {
        "_version"
    };
    rust.open(format!("impl KafkaDecode for {rust_type}"));
    rust.open(format!(
        "fn decode(decoder: &mut Decoder, {version}: ApiVersion) -> Result<Self, DecodeError>"
    ));
    render_reads(rust, fields, message)?;
    rust.blank();
    render_construction(rust, fields, false);
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}

/// Encode body for one struct a message declares.
pub(super) fn render_struct_encode(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
) -> Result<(), GenerationError> {
    let version = if body_uses_version(fields, message)? {
        "version"
    } else {
        "_version"
    };
    rust.open(format!("impl KafkaEncode for {rust_type}"));
    rust.line("fn encode<T: EncodeTarget>(");
    rust.line("    &self,");
    rust.line("    encoder: &mut Encoder<T>,");
    rust.line(format!("    {version}: ApiVersion,"));
    rust.open(") -> Result<(), EncodeError>");
    render_writes(rust, fields, message)?;
    rust.blank();
    rust.line("Ok(())");
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}
