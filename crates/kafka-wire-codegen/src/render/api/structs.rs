//! Emission for the structs a message declares.
//!
//! This file owns turning one message's `commonStructs` and inline field bodies
//! into Rust: the struct definition, its default, and its codecs. It
//! deliberately owns no message-level concern — a struct has no API key or
//! descriptor, while its own effective supported range comes from the schema
//! declaration table rather than from its owner message.

use kafka_wire_schema::{Field, FieldType, Message, VersionSet};

use crate::{
    GenerationError,
    render::{field, invariant, text::RustText},
};

use super::codec::{render_construction, render_reads, render_struct_encode};
use super::declarations::{RenderableStruct, declared_structs};
use super::imports::spell;
use super::prose::sentence;
use super::tagged::render_tagged_decode;
use super::validation::{Owner, render_validation};

/// Renders one whole schema as a standalone struct.
///
/// A header is versioned like a message and split on flexibility like one, but
/// it is dispatched by nothing, so it gets the struct treatment: definition,
/// default, its own flexible window, and codecs.
pub(crate) fn render_standalone(
    rust: &mut RustText,
    message: &Message,
) -> Result<(), GenerationError> {
    let flexible_versions = message.effective_flexible_versions();
    let versions = CodecVersions {
        supported: &message.valid_versions,
        flexible: &flexible_versions,
    };
    render_struct_with(
        rust,
        message.name.rust_type(),
        message.name.protocol(),
        &message.fields,
        message,
        versions,
        Identity::Message,
    )
}

/// Renders every struct this message declares, in protocol declaration order.
pub(crate) fn render_declared_structs(
    rust: &mut RustText,
    message: &Message,
) -> Result<(), GenerationError> {
    for declaration in declared_structs(message)? {
        render_struct(rust, &declaration, message)?;
    }
    Ok(())
}

/// Renders one declared struct: its definition, its default, and its codecs.
///
/// A struct gets no `KafkaMessage` impl. It has no API key or independent wire
/// name, but its public codec does own the effective declaration range and the
/// part of that range using flexible encoding.
/// How a rendered struct states the flexible window its codecs read.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Identity {
    /// A struct nested inside a message: it owns both version constants
    /// inherently because it does not implement `KafkaMessage`.
    Nested,
    /// A schema that stands alone. `KafkaMessage` carries exactly what a header
    /// has — a protocol name, a supported range, and a flexible window — while
    /// the API key lives on the direction traits, which a header does not
    /// implement. So a header is a `KafkaMessage` without being an API message.
    Message,
}

/// The exact version universe one generated public codec promises to accept.
#[derive(Clone, Copy)]
struct CodecVersions<'a> {
    supported: &'a VersionSet,
    flexible: &'a VersionSet,
}

fn render_struct(
    rust: &mut RustText,
    declaration: &RenderableStruct<'_>,
    message: &Message,
) -> Result<(), GenerationError> {
    // Field rendering asks the message for its version universe. A nested
    // declaration has a narrower universe of its own, so give every field
    // decision the same effective windows used by the public codec guards.
    let mut context = message.clone();
    context.valid_versions = declaration.versions.clone();
    context.flexible_versions = declaration.flexible_versions.clone();
    let versions = CodecVersions {
        supported: declaration.versions,
        flexible: &declaration.flexible_versions,
    };
    render_struct_with(
        rust,
        declaration.name.rust_type(),
        declaration.name.declared(),
        declaration.fields,
        &context,
        versions,
        Identity::Nested,
    )
}

fn render_struct_with(
    rust: &mut RustText,
    rust_type: &str,
    declared: &str,
    fields: &[Field],
    message: &Message,
    versions: CodecVersions<'_>,
    identity: Identity,
) -> Result<(), GenerationError> {
    // Upstream's own spelling, not the Rust one the next line already shows.
    // The qualified type no longer repeats the API stem, so this is what a
    // reader greps back to the schema.
    rust.line(format!(
        "/// `{declared}` as declared by the `{}` API.",
        message.name.api_stem()
    ));
    rust.line("#[non_exhaustive]");
    let derive_default = fields
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
        rust.line(format!("#[derive(Clone, Debug, Default, {equality})]"));
    } else {
        rust.line(format!("#[derive(Clone, Debug, {equality})]"));
    }
    rust.open(format!("pub struct {rust_type}"));
    for member in fields {
        rust.line(format!("/// {}", sentence(&member.about)));
        rust.line(format!(
            "pub {}: {},",
            member.name.rust_field(),
            field::rust_type(member, message)
        ));
    }
    let flexible = !versions.flexible.is_empty();
    if flexible {
        rust.line("/// Unknown flexible-version tagged fields retained for forwarding.");
        rust.line(format!(
            "pub unknown_tagged_fields: {},",
            spell(message, "TaggedFields")
        ));
    }
    rust.close("");
    rust.blank();

    render_identity(rust, rust_type, message, versions, identity)?;
    let owner = match identity {
        Identity::Message => Owner::Message,
        Identity::Nested => Owner::Struct(rust_type),
    };
    render_validation(rust, rust_type, fields, message, owner, flexible);

    if !derive_default {
        rust.open(format!("impl Default for {rust_type}"));
        rust.open("fn default() -> Self");
        rust.open("Self");
        for member in fields {
            rust.line(format!(
                "{}: {},",
                member.name.rust_field(),
                field::default_expression(member, message)
            ));
        }
        if flexible {
            rust.line(format!(
                "unknown_tagged_fields: {}::default(),",
                spell(message, "TaggedFields")
            ));
        }
        rust.close("");
        rust.close("");
        rust.close("");
        rust.blank();
    }

    render_struct_decode(rust, rust_type, fields, message, identity, flexible)?;
    render_struct_encode(rust, rust_type, fields, message, flexible)?;
    Ok(())
}

/// States the flexible window the rendered codecs read, as its identity spells
/// it.
///
/// Split out because this is the one place the two identities differ: a
/// standalone schema states the windows through `KafkaMessage`, while a nested
/// struct states them as inherent constants. Everything around it is identical
/// for both, so the seam is the distinction rather than a line count.
fn render_identity(
    rust: &mut RustText,
    rust_type: &str,
    message: &Message,
    versions: CodecVersions<'_>,
    identity: Identity,
) -> Result<(), GenerationError> {
    let version_range = spell(message, "VersionRange");
    let range =
        invariant::optional_bounded(message, versions.flexible, "effective flexible versions")?
            .map_or_else(
                || "None".to_owned(),
                |(start, end)| format!("Some({version_range}::new({start}, {end}))"),
            );
    match identity {
        Identity::Message => {
            let (start, end) = invariant::bounded(message, versions.supported, "valid versions")?;
            rust.open(format!(
                "impl {} for {rust_type}",
                spell(message, "KafkaMessage")
            ));
            rust.line(format!(
                "const NAME: &'static str = {:?};",
                message.name.protocol()
            ));
            rust.line(format!(
                "const SUPPORTED_VERSIONS: {version_range} = \
                 {version_range}::new({start}, {end});"
            ));
            rust.line(format!(
                "const FLEXIBLE_VERSIONS: Option<{version_range}> = {range};"
            ));
            rust.close("");
            rust.blank();
        }
        Identity::Nested => {
            let (start, end) =
                invariant::bounded(message, versions.supported, "struct effective versions")?;
            rust.open(format!("impl {rust_type}"));
            rust.line(format!(
                "const SUPPORTED_VERSIONS: {version_range} = \
                 {version_range}::new({start}, {end});"
            ));
            if !versions.flexible.is_empty() {
                rust.line(format!(
                    "const FLEXIBLE_VERSIONS: Option<{version_range}> = {range};"
                ));
                rust.blank();
                rust.open(format!(
                    "fn is_flexible(version: {}) -> bool",
                    spell(message, "ApiVersion")
                ));
                rust.line("Self::FLEXIBLE_VERSIONS.is_some_and(|range| range.contains(version))");
                rust.close("");
            }
            rust.close("");
            rust.blank();
        }
    }
    Ok(())
}

/// Decode body for one struct a message declares.
///
/// A nested struct checks its own effective declaration range before reading.
fn render_struct_decode(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[kafka_wire_schema::Field],
    message: &Message,
    identity: Identity,
    flexible: bool,
) -> Result<(), GenerationError> {
    rust.open(format!(
        "impl {} for {rust_type}",
        spell(message, "KafkaDecode")
    ));
    rust.open(format!(
        "fn decode(decoder: &mut {}, version: {}) -> Result<Self, {}>",
        spell(message, "Decoder"),
        spell(message, "ApiVersion"),
        spell(message, "DecodeError"),
    ));
    match identity {
        Identity::Message => rust.line("crate::message::ensure_decode_version::<Self>(version)?;"),
        Identity::Nested => {
            rust.open("if !Self::SUPPORTED_VERSIONS.contains(version)");
            rust.open(format!(
                "return Err({}::UnsupportedVersion",
                spell(message, "DecodeError")
            ));
            rust.line(format!("message: {rust_type:?},"));
            rust.line("version,");
            rust.line("supported: Self::SUPPORTED_VERSIONS,");
            rust.close(");");
            rust.close("");
        }
    }
    rust.blank();
    render_reads(rust, fields, message)?;
    if flexible {
        render_tagged_decode(rust, fields, message)?;
    }
    rust.blank();
    render_construction(rust, fields, flexible);
    rust.close("");
    rust.close("");
    rust.blank();
    Ok(())
}
