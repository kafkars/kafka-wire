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
            Ok(gate(&compact, &legacy))
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
            Ok(gate(&compact, &legacy))
        }
    }
}

/// Emits the flexible/legacy choice, or just the one form when both agree.
///
/// A fixed-width type has one encoding on both sides of the boundary. Emitting
/// the gate anyway produced two identical branches in generated source — dead
/// branching that says a decision was made where none was, and that the lints
/// applied to checked-in output reject outright.
fn gate(compact: &str, legacy: &str) -> String {
    if compact == legacy {
        return compact.to_owned();
    }
    format!("if Self::is_flexible(version) {{ {compact} }} else {{ {legacy} }}")
}

/// Read expression and write statement for an array's own length prefix.
///
/// The prefix is the one part of an array that changes with the encoding
/// regime: compact arrays carry a varint of `len + 1`, legacy arrays a plain
/// `int32`. Elements are unaffected, which is why this is decided here and not
/// in `element_codec`.
pub(crate) fn array_length_codec(field: &Field, message: &Message) -> (String, String) {
    if is_nullable(field, message) {
        return nullable_array_length_codec(field, message);
    }
    let name = field.name.rust_field();
    let compact_read = "decoder.read_compact_array_len()?";
    let legacy_read = "decoder.read_array_len()?";
    let compact_write = format!("encoder.write_compact_array_len(self.{name}.len())?;");
    let legacy_write = format!("encoder.write_array_len(self.{name}.len())?;");
    match encoding_of(field, message) {
        Encoding::Compact => (compact_read.to_owned(), compact_write),
        Encoding::Legacy => (legacy_read.to_owned(), legacy_write),
        Encoding::VersionGated => (
            gate(compact_read, legacy_read),
            gate(&compact_write, &legacy_write),
        ),
    }
}

/// The same prefix for an array that may itself be null.
///
/// The nullable readers return `Option<usize>`, which is what lets the decode
/// block tell an absent array from a present empty one — two distinct wire
/// encodings that a plain length cannot separate.
fn nullable_array_length_codec(field: &Field, message: &Message) -> (String, String) {
    let name = field.name.rust_field();
    let compact_read = "decoder.read_compact_nullable_array_len()?";
    let legacy_read = "decoder.read_nullable_array_len()?";
    let compact_write =
        format!("encoder.write_compact_nullable_array_len(self.{name}.as_ref().map(Vec::len))?;");
    let legacy_write =
        format!("encoder.write_nullable_array_len(self.{name}.as_ref().map(Vec::len))?;");
    match encoding_of(field, message) {
        Encoding::Compact => (compact_read.to_owned(), compact_write),
        Encoding::Legacy => (legacy_read.to_owned(), legacy_write),
        Encoding::VersionGated => (
            gate(compact_read, legacy_read),
            gate(&compact_write, &legacy_write),
        ),
    }
}

/// Read expression and write statement for one array element.
///
/// The generated loop binds `value` by reference, so a `Copy` scalar is
/// dereferenced at the call and a borrowed type is passed straight through.
///
/// A length-prefixed element carries its own prefix, and that prefix follows
/// the message's encoding regime exactly as a top-level field's does: a string
/// inside a compact array is a compact string. Apache Kafka's own bytes caught
/// this — reading `02 03 6731 00` with a legacy element reader takes `0x0367`
/// as an int16 length and asks for 871 bytes. Only fixed-width elements are
/// regime-independent.
pub(crate) fn element_codec(
    element: &FieldType,
    field: &Field,
    message: &Message,
) -> Result<(String, String), GenerationError> {
    if let Some(pair) = length_prefixed_element(element, field, message) {
        return Ok(pair);
    }
    let pair = match element {
        FieldType::Bool => ("decoder.read_bool()?", "encoder.write_bool(*value)?;"),
        FieldType::Int8 => ("decoder.read_i8()?", "encoder.write_i8(*value)?;"),
        FieldType::Int16 => ("decoder.read_i16()?", "encoder.write_i16(*value)?;"),
        FieldType::Uint16 => ("decoder.read_u16()?", "encoder.write_u16(*value)?;"),
        FieldType::Uint32 => ("decoder.read_u32()?", "encoder.write_u32(*value)?;"),
        FieldType::Int32 => ("decoder.read_i32()?", "encoder.write_i32(*value)?;"),
        FieldType::Int64 => ("decoder.read_i64()?", "encoder.write_i64(*value)?;"),
        FieldType::Uuid => ("decoder.read_uuid()?", "encoder.write_uuid(*value)?;"),
        FieldType::Struct(reference) => {
            return Ok((
                format!("{}::decode(decoder, version)?", reference.rust_type()),
                "value.encode(encoder, version)?;".to_owned(),
            ));
        }
        other => {
            return Err(GenerationError::unsupported(
                message,
                field.name.protocol(),
                format!("array element type {other:?} has no codec in this backend"),
            ));
        }
    };
    Ok((pair.0.to_owned(), pair.1.to_owned()))
}

/// The regime-dependent codec for an element that carries its own length.
fn length_prefixed_element(
    element: &FieldType,
    field: &Field,
    message: &Message,
) -> Option<(String, String)> {
    let (compact_read, legacy_read, compact_write, legacy_write) = match element {
        FieldType::String => (
            "decoder.read_compact_string()?",
            "decoder.read_string()?",
            "encoder.write_compact_string(value)?;",
            "encoder.write_string(value)?;",
        ),
        FieldType::Bytes => (
            "decoder.read_compact_bytes()?",
            "decoder.read_bytes()?",
            "encoder.write_compact_bytes(value)?;",
            "encoder.write_bytes(value)?;",
        ),
        _ => return None,
    };
    Some(match encoding_of(field, message) {
        Encoding::Compact => (compact_read.to_owned(), compact_write.to_owned()),
        Encoding::Legacy => (legacy_read.to_owned(), legacy_write.to_owned()),
        Encoding::VersionGated => (
            gate(compact_read, legacy_read),
            gate(compact_write, legacy_write),
        ),
    })
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
    // A field may pin itself to an encoding its message would not otherwise
    // use. `RequestHeader.ClientId` declares `flexibleVersions: "none"` and so
    // keeps the legacy two-byte prefix even in a flexible header, which is what
    // lets a broker read the header of a request before it knows the version
    // the client chose. Ignoring the override would put a varint there.
    let flexible = field
        .flexible_versions
        .clone()
        .unwrap_or_else(|| message.effective_flexible_versions());
    if present.is_subset_of(&flexible) {
        Encoding::Compact
    } else if present.intersection(&flexible).is_empty() {
        Encoding::Legacy
    } else {
        Encoding::VersionGated
    }
}

pub(crate) fn is_nullable(field: &Field, message: &Message) -> bool {
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
        FieldType::Uint16 => Ok("decoder.read_u16()?".to_owned()),
        FieldType::Uint32 => Ok("decoder.read_u32()?".to_owned()),
        FieldType::Int32 => Ok("decoder.read_i32()?".to_owned()),
        FieldType::Int64 => Ok("decoder.read_i64()?".to_owned()),
        FieldType::Uuid => Ok("decoder.read_uuid()?".to_owned()),
        FieldType::Bytes if nullable && compact => {
            Ok("decoder.read_compact_nullable_bytes()?".to_owned())
        }
        FieldType::Bytes if nullable => Ok("decoder.read_nullable_bytes()?".to_owned()),
        FieldType::Bytes if compact => Ok("decoder.read_compact_bytes()?".to_owned()),
        FieldType::Bytes => Ok("decoder.read_bytes()?".to_owned()),
        FieldType::Struct(reference) => Ok(format!(
            "{}::decode(decoder, version)?",
            reference.rust_type()
        )),
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
        FieldType::Uint16 => Ok(format!("encoder.write_u16(self.{name})?;")),
        FieldType::Uint32 => Ok(format!("encoder.write_u32(self.{name})?;")),
        FieldType::Int32 => Ok(format!("encoder.write_i32(self.{name})?;")),
        FieldType::Int64 => Ok(format!("encoder.write_i64(self.{name})?;")),
        FieldType::Uuid => Ok(format!("encoder.write_uuid(self.{name})?;")),
        FieldType::Bytes if nullable && compact => Ok(format!(
            "encoder.write_compact_nullable_bytes(self.{name}.as_deref())?;"
        )),
        FieldType::Bytes if nullable => Ok(format!(
            "encoder.write_nullable_bytes(self.{name}.as_deref())?;"
        )),
        FieldType::Bytes if compact => Ok(format!("encoder.write_compact_bytes(&self.{name})?;")),
        FieldType::Bytes => Ok(format!("encoder.write_bytes(&self.{name})?;")),
        FieldType::Struct(_) => Ok(format!("self.{name}.encode(encoder, version)?;")),
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
