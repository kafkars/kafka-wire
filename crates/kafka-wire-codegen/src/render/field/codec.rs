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

use kafka_wire_schema::{Field, FieldType, Message, VersionSet};

use crate::{
    GenerationError,
    render::field::{
        regime::{
            Encoding, Nullability, encoding_of, encoding_over, is_nullable, nullability_of, present,
        },
        version,
    },
};

pub(crate) fn read_expression(field: &Field, message: &Message) -> Result<String, GenerationError> {
    match nullability_of(field, message) {
        Nullability::Never => read_over(&present(field, message), field, message, false),
        Nullability::Always => read_over(&present(field, message), field, message, true),
        // A field nullable in only some of the versions it appears in has to
        // read through two different methods, because the non-null reader
        // REJECTS the null sentinel. Using the nullable one throughout would
        // silently accept a null the protocol forbids at that version — the
        // encoding regime is chosen per window too, since the versions where a
        // field is nullable need not be the versions where it is compact.
        Nullability::Gated { nullable, plain } => {
            let condition = version::condition_for(&nullable, message);
            let null_read = read_over(&nullable, field, message, true)?;
            let plain_read = read_over(&plain, field, message, false)?;
            Ok(format!(
                "if {condition} {{ {null_read} }} else {{ Some({plain_read}) }}"
            ))
        }
    }
}

/// The read expression over one window, with the regime that window implies.
fn read_over(
    window: &VersionSet,
    field: &Field,
    message: &Message,
    nullable: bool,
) -> Result<String, GenerationError> {
    match encoding_over(window, field, message) {
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
    let whole = present(field, message);
    match nullability_of(field, message) {
        Nullability::Never => (
            array_len_read(&whole, field, message, false),
            array_len_write(&whole, field, message, false),
        ),
        Nullability::Always => (
            array_len_read(&whole, field, message, true),
            array_len_write(&whole, field, message, true),
        ),
        Nullability::Gated { nullable, plain } => {
            let condition = version::condition_for(&nullable, message);
            let null_read = array_len_read(&nullable, field, message, true);
            let plain_read = array_len_read(&plain, field, message, false);
            (
                format!("if {condition} {{ {null_read} }} else {{ Some({plain_read}) }}"),
                // The write needs no gate. Every nullable writer delegates to
                // its non-null counterpart for a present value, so `Some(n)`
                // is byte-identical under both; only `None` differs, and that
                // case is refused outright by the guard `render_writes` emits.
                array_len_write(&whole, field, message, true),
            )
        }
    }
}

/// The length-prefix read over one window.
///
/// The nullable readers return `Option<usize>`, which is what lets the decode
/// block tell an absent array from a present empty one — two distinct wire
/// encodings that a plain length cannot separate.
fn array_len_read(window: &VersionSet, field: &Field, message: &Message, nullable: bool) -> String {
    let (compact, legacy) = if nullable {
        (
            "decoder.read_compact_nullable_array_len()?",
            "decoder.read_nullable_array_len()?",
        )
    } else {
        (
            "decoder.read_compact_array_len()?",
            "decoder.read_array_len()?",
        )
    };
    match encoding_over(window, field, message) {
        Encoding::Compact => compact.to_owned(),
        Encoding::Legacy => legacy.to_owned(),
        Encoding::VersionGated => gate(compact, legacy),
    }
}

/// The length-prefix write over one window.
fn array_len_write(
    window: &VersionSet,
    field: &Field,
    message: &Message,
    nullable: bool,
) -> String {
    let name = field.name.rust_field();
    let (compact, legacy) = if nullable {
        (
            format!(
                "encoder.write_compact_nullable_array_len(self.{name}.as_ref().map(Vec::len))?;"
            ),
            format!("encoder.write_nullable_array_len(self.{name}.as_ref().map(Vec::len))?;"),
        )
    } else {
        (
            format!("encoder.write_compact_array_len(self.{name}.len())?;"),
            format!("encoder.write_array_len(self.{name}.len())?;"),
        )
    };
    match encoding_over(window, field, message) {
        Encoding::Compact => compact,
        Encoding::Legacy => legacy,
        Encoding::VersionGated => gate(&compact, &legacy),
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
        FieldType::Float64 => ("decoder.read_float64()?", "encoder.write_float64(*value)?;"),
        FieldType::Struct(reference) => {
            return Ok((
                format!("{}::decode(decoder, version)?", reference.rust_type()),
                "value.encode(encoder, version)?;".to_owned(),
            ));
        }
        // String and Bytes returned above through `length_prefixed_element`;
        // they are named here only because the match must stay total.
        // String, Bytes, and Records returned above through
        // `length_prefixed_element`; named here only so the match stays total.
        other @ (FieldType::String
        | FieldType::Bytes
        | FieldType::Records
        | FieldType::Array(_)) => {
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
        FieldType::Bytes | FieldType::Records => (
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
        FieldType::Float64 => Ok("decoder.read_float64()?".to_owned()),
        FieldType::Bytes | FieldType::Records if nullable && compact => {
            Ok("decoder.read_compact_nullable_bytes()?".to_owned())
        }
        FieldType::Bytes | FieldType::Records if nullable => {
            Ok("decoder.read_nullable_bytes()?".to_owned())
        }
        FieldType::Bytes | FieldType::Records if compact => {
            Ok("decoder.read_compact_bytes()?".to_owned())
        }
        FieldType::Bytes | FieldType::Records => Ok("decoder.read_bytes()?".to_owned()),
        // The presence marker is regime-independent, which is why `compact` is
        // not consulted here: it is a raw int8 in a flexible message too.
        FieldType::Struct(reference) if nullable => Ok(format!(
            "if decoder.read_struct_presence()? {{ Some({}::decode(decoder, version)?) }} \
             else {{ None }}",
            reference.rust_type()
        )),
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
        FieldType::Float64 => Ok(format!("encoder.write_float64(self.{name})?;")),
        FieldType::Bytes | FieldType::Records if nullable && compact => Ok(format!(
            "encoder.write_compact_nullable_bytes(self.{name}.as_deref())?;"
        )),
        FieldType::Bytes | FieldType::Records if nullable => Ok(format!(
            "encoder.write_nullable_bytes(self.{name}.as_deref())?;"
        )),
        // `Records` belongs beside `Bytes` in every arm, and was missing from
        // this one alone. The read path had it, so a non-nullable records field
        // in a flexible message was read with a compact prefix and written with
        // a legacy one — the exact encode/decode divergence this repository's
        // single-encode-path design exists to make impossible, reintroduced by
        // one absent variant in one match arm.
        FieldType::Bytes | FieldType::Records if compact => {
            Ok(format!("encoder.write_compact_bytes(&self.{name})?;"))
        }
        FieldType::Bytes | FieldType::Records => Ok(format!("encoder.write_bytes(&self.{name})?;")),
        FieldType::Struct(_) if nullable => Ok(format!(
            "if let Some(value) = &self.{name} {{ encoder.write_struct_presence(true)?; \
             value.encode(encoder, version)?; }} \
             else {{ encoder.write_struct_presence(false)?; }}"
        )),
        FieldType::Struct(_) => Ok(format!("self.{name}.encode(encoder, version)?;")),
        FieldType::Array(_) => Err(GenerationError::unsupported(
            message,
            field.name.protocol(),
            "an array is written by a structured block; the scalar write path has \
             no statement for one",
        )),
    }
}
