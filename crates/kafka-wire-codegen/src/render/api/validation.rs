//! Recursive preflight validation emitted for messages and nested structures.
//!
//! Validation owns representability, nullability, retained tags, and descent;
//! byte reads and writes remain in the sibling codec module.

use kafka_wire_schema::{Field, FieldType, Message};

use crate::render::{field, text::RustText};

use super::imports::{ExternalSymbol as S, spell};
use super::tagged_proof::RenderedKnownTags;
use super::tagged_validation::render_known_tag_ownership;

/// How a structure names itself in an encode error.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Owner<'a> {
    /// A message, which carries its protocol name as a constant.
    Message,
    /// A nested struct, named by its generated Rust type.
    Struct(&'a str),
}

impl Owner<'_> {
    fn name(self) -> String {
        match self {
            Self::Message => "Self::NAME".to_owned(),
            Self::Struct(rust_type) => format!("{rust_type:?}"),
        }
    }
}

/// Emits the preflight every generated encoder runs before writing a byte.
pub(super) fn render_validation(
    rust: &mut RustText,
    rust_type: &str,
    fields: &[Field],
    message: &Message,
    owner: Owner<'_>,
    flexible: bool,
) -> RenderedKnownTags {
    rust.open(format!("impl {rust_type}"));
    let rendered = render_known_tag_ownership(rust, fields, message, &owner.name());
    if !validation_uses_self(fields, message, flexible) {
        rust.line("#[allow(clippy::unused_self)]");
    }
    rust.open(format!(
        "fn validate_for_version(&self, version: {}) -> {}<(), {}>",
        spell(message, S::ApiVersion),
        spell(message, S::Result),
        spell(message, S::EncodeError),
    ));
    match owner {
        Owner::Message => rust.line("crate::message::ensure_encode_version::<Self>(version)?;"),
        Owner::Struct(_) => render_struct_version_guard(rust, message, owner),
    }
    rust.blank();

    if !rendered.is_empty() {
        rust.line("self.validate_known_tag_ownership(version)?;");
    }

    render_representability_checks(rust, fields, message, owner);
    for field in fields {
        render_null_guard(rust, field, message, owner);
    }
    render_nested_validation(rust, fields, message);
    if flexible {
        rust.open("if !Self::is_flexible(version) && !self.unknown_tagged_fields.is_empty()");
        rust.open(format!(
            "return {}({}::TaggedFieldsNotRepresentable",
            spell(message, S::Err),
            spell(message, S::EncodeError)
        ));
        rust.line(format!("message: {},", owner.name()));
        rust.line("version,");
        rust.close(");");
        rust.close("");
    }
    rust.blank();
    rust.line(format!("{}(())", spell(message, S::Ok)));
    rust.close("");
    rust.close("");
    rust.blank();
    rendered
}

fn validation_uses_self(fields: &[Field], message: &Message, flexible: bool) -> bool {
    flexible
        || fields.iter().any(|field| {
            field.ty.struct_reference().is_some()
                || field::null_forbidden_condition(field, message).is_some()
                || (!field.ignorable && field::absence_condition(field, message).is_some())
        })
}

fn render_representability_checks(
    rust: &mut RustText,
    fields: &[Field],
    message: &Message,
    owner: Owner<'_>,
) {
    for (candidate, condition) in fields.iter().filter_map(|candidate| {
        field::absence_condition(candidate, message)
            .filter(|_| !candidate.ignorable)
            .map(|condition| (candidate, condition))
    }) {
        rust.open(format!(
            "if {} && {}",
            field::as_conjunct(&condition),
            field::non_default_condition(candidate, message)
        ));
        rust.open(format!(
            "return {}({}::FieldNotRepresentable",
            spell(message, S::Err),
            spell(message, S::EncodeError)
        ));
        rust.line(format!("message: {},", owner.name()));
        rust.line(format!("field: {:?},", candidate.name.protocol()));
        rust.line("version,");
        rust.close(");");
        rust.close("");
    }
}

fn render_null_guard(rust: &mut RustText, field: &Field, message: &Message, owner: Owner<'_>) {
    let Some(condition) = field::null_forbidden_condition(field, message) else {
        return;
    };
    let name = field.name.rust_field();
    rust.open(format!(
        "if {} && self.{name}.is_none()",
        field::as_conjunct(&condition)
    ));
    rust.open(format!(
        "return {}({}::NullNotAllowed",
        spell(message, S::Err),
        spell(message, S::EncodeError)
    ));
    rust.line(format!("message: {},", owner.name()));
    rust.line(format!("field: {:?},", field.name.protocol()));
    rust.line("version,");
    rust.close(");");
    rust.close("");
}

fn render_struct_version_guard(rust: &mut RustText, message: &Message, owner: Owner<'_>) {
    rust.open("if !Self::SUPPORTED_VERSIONS.contains(version)");
    rust.open(format!(
        "return {}({}::UnsupportedVersion",
        spell(message, S::Err),
        spell(message, S::EncodeError)
    ));
    rust.line(format!("message: {},", owner.name()));
    rust.line("version,");
    rust.line("supported: Self::SUPPORTED_VERSIONS,");
    rust.close(");");
    rust.close("");
}

fn render_nested_validation(rust: &mut RustText, fields: &[Field], message: &Message) {
    for field in fields {
        if field.ty.struct_reference().is_none() {
            continue;
        }
        let gate = field::presence_condition(field, message);
        if let Some(condition) = &gate {
            rust.open(format!("if {condition}"));
        }
        let name = field.name.rust_field();
        match &field.ty {
            FieldType::Struct(_) if field::is_nullable(field, message) => {
                rust.open(format!(
                    "if let {}(value) = &self.{name}",
                    spell(message, S::Some)
                ));
                rust.line("value.validate_for_version(version)?;");
                rust.close("");
            }
            FieldType::Struct(_) => {
                rust.line(format!("self.{name}.validate_for_version(version)?;"));
            }
            FieldType::Array(_) if field::is_nullable(field, message) => {
                rust.open(format!(
                    "if let {}(values) = &self.{name}",
                    spell(message, S::Some)
                ));
                render_validation_loop(rust);
                rust.close("");
            }
            FieldType::Array(_) => {
                rust.open(format!("for value in &self.{name}"));
                rust.line("value.validate_for_version(version)?;");
                rust.close("");
            }
            _ => {}
        }
        if gate.is_some() {
            rust.close("");
        }
    }
}

fn render_validation_loop(rust: &mut RustText) {
    rust.open("for value in values");
    rust.line("value.validate_for_version(version)?;");
    rust.close("");
}
