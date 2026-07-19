//! Primitive read and write expressions selected by field encoding mode.
//!
//! This file owns one decision: given a field's type, its nullability, and
//! whether the versions it appears in are flexible, which `Decoder` or `Encoder`
//! method carries it. Compact and legacy are spelled as two named methods per
//! shape because they are two wire formats, not two settings of one.
//!
//! It deliberately owns no aggregate emission. Arrays and structs are rendered
//! as statement blocks by `render::api::codec`, so reaching one here means a
//! caller routed an aggregate into the scalar path. That is reported as a named
//! error rather than absorbed by a placeholder comment: a comment where a codec
//! belongs still compiles, and encodes nothing.

use kafka_wire_schema::{Field, FieldType, Message};

use crate::GenerationError;

pub(crate) fn read_expression(field: &Field, message: &Message) -> Result<String, GenerationError> {
    let nullable = is_nullable(field, message);
    match encoding_of(field, message) {
        Encoding::Compact => read_method(field, message, nullable, true),
        Encoding::Legacy => read_method(field, message, nullable, false),
        Encoding::VersionGated => {
            let compact = read_method(field, message, nullable, true)?;
            let legacy = read_method(field, message, nullable, false)?;
            Ok(format!(
                "if Self::is_flexible(version) {{ {compact} }} else {{ {legacy} }}"
            ))
        }
    }
}

pub(crate) fn write_statement(field: &Field, message: &Message) -> Result<String, GenerationError> {
    let nullable = is_nullable(field, message);
    match encoding_of(field, message) {
        Encoding::Compact => write_method(field, message, nullable, true),
        Encoding::Legacy => write_method(field, message, nullable, false),
        Encoding::VersionGated => {
            let compact = write_method(field, message, nullable, true)?;
            let legacy = write_method(field, message, nullable, false)?;
            Ok(format!(
                "if Self::is_flexible(version) {{ {compact} }} else {{ {legacy} }}"
            ))
        }
    }
}

/// Which length prefix a field uses across the versions it is present in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Encoding {
    /// Present only in flexible versions, so the compact form is unconditional.
    Compact,
    /// Present only in pre-flexible versions, so the legacy form is unconditional.
    Legacy,
    /// Present on both sides of the flexible boundary, so the gate is emitted.
    VersionGated,
}

fn encoding_of(field: &Field, message: &Message) -> Encoding {
    let present = field.versions.intersection(&message.valid_versions);
    let flexible = message.effective_flexible_versions();
    if present.is_subset_of(&flexible) {
        Encoding::Compact
    } else if present.intersection(&flexible).is_empty() {
        Encoding::Legacy
    } else {
        Encoding::VersionGated
    }
}

fn is_nullable(field: &Field, message: &Message) -> bool {
    !field
        .nullable_versions
        .intersection(&message.valid_versions)
        .is_empty()
}

fn read_method(
    field: &Field,
    message: &Message,
    nullable: bool,
    compact: bool,
) -> Result<String, GenerationError> {
    match &field.ty {
        FieldType::String if nullable && compact => {
            Ok("decoder.read_compact_nullable_string()?".to_owned())
        }
        FieldType::String if nullable => Ok("decoder.read_nullable_string()?".to_owned()),
        FieldType::String if compact => Ok("decoder.read_compact_string()?".to_owned()),
        FieldType::String => Ok("decoder.read_string()?".to_owned()),
        FieldType::Bool => Ok("decoder.read_bool()?".to_owned()),
        FieldType::Int8 => Ok("decoder.read_i8()?".to_owned()),
        FieldType::Int16 => Ok("decoder.read_i16()?".to_owned()),
        FieldType::Int32 => Ok("decoder.read_i32()?".to_owned()),
        FieldType::Int64 => Ok("decoder.read_i64()?".to_owned()),
        FieldType::Array(_) => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            "an array is read by a structured block; the scalar read path has no \
             expression for one",
        )),
        other => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            format!("field type {other:?} has no read expression in this backend"),
        )),
    }
}

fn write_method(
    field: &Field,
    message: &Message,
    nullable: bool,
    compact: bool,
) -> Result<String, GenerationError> {
    let name = field.name.rust_field();
    match &field.ty {
        FieldType::String if nullable && compact => Ok(format!(
            "encoder.write_compact_nullable_string(self.{name}.as_ref())?;"
        )),
        FieldType::String if nullable => Ok(format!(
            "encoder.write_nullable_string(self.{name}.as_ref())?;"
        )),
        FieldType::String if compact => Ok(format!("encoder.write_compact_string(&self.{name})?;")),
        FieldType::String => Ok(format!("encoder.write_string(&self.{name})?;")),
        FieldType::Bool => Ok(format!("encoder.write_bool(self.{name})?;")),
        FieldType::Int8 => Ok(format!("encoder.write_i8(self.{name})?;")),
        FieldType::Int16 => Ok(format!("encoder.write_i16(self.{name})?;")),
        FieldType::Int32 => Ok(format!("encoder.write_i32(self.{name})?;")),
        FieldType::Int64 => Ok(format!("encoder.write_i64(self.{name})?;")),
        FieldType::Array(_) => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            "an array is written by a structured block; the scalar write path has \
             no statement for one",
        )),
        other => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            format!("field type {other:?} has no write statement in this backend"),
        )),
    }
}
