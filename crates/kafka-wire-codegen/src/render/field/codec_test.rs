//! The read/write half of the field-emission table.
//!
//! Scenario: for every field type the backend claims to support, crossed with
//! nullability and with the three encoding regimes a message can put a field
//! in, assert the exact `Decoder` and `Encoder` call that is emitted. These
//! strings are the generator's specification. A change to any one of them
//! changes the bytes every generated message reads and writes, so it must show
//! up here as a reviewable diff rather than propagate silently.
//!
//! The encoding regime is expressed the way the generator decides it — through
//! the message's `flexibleVersions` — not by poking a boolean. `"none"` is a
//! message with no flexible version, `"0+"` is flexible throughout, and `"3+"`
//! against `validVersions` `"0-4"` straddles the boundary and must emit a gate.

use kafka_wire_schema::FieldType;

use super::{
    codec::{read_expression, write_statement},
    probe::{field, message, nullable, struct_type},
};

/// Every message in this table declares the same version window.
///
/// A fixed window is what lets `flexibleVersions` alone select the regime: the
/// field is present in every version, so nothing but the compact/legacy split
/// can vary between rows.
const VALID: &str = "0-4";

/// One cell of the read/write table.
struct Cell {
    /// The protocol situation this cell pins down.
    situation: &'static str,
    ty: FieldType,
    /// Whether `nullableVersions` covers the field.
    nullable: bool,
    /// The message's `flexibleVersions` declaration.
    flexible: &'static str,
    /// Exact decode expression.
    read: &'static str,
    /// Exact encode statement.
    write: &'static str,
}

fn table() -> Vec<Cell> {
    vec![
        Cell {
            situation: "a non-null string in a message with no flexible version",
            ty: FieldType::String,
            nullable: false,
            flexible: "none",
            read: "decoder.read_string()?",
            write: "encoder.write_string(&self.probe)?;",
        },
        Cell {
            situation: "a non-null string present only in flexible versions",
            ty: FieldType::String,
            nullable: false,
            flexible: "0+",
            read: "decoder.read_compact_string()?",
            write: "encoder.write_compact_string(&self.probe)?;",
        },
        Cell {
            situation: "a non-null string straddling the flexible boundary",
            ty: FieldType::String,
            nullable: false,
            flexible: "3+",
            read: "if Self::is_flexible(version) { decoder.read_compact_string()? } \
                   else { decoder.read_string()? }",
            write: "if Self::is_flexible(version) { encoder.write_compact_string(&self.probe)?; } \
                    else { encoder.write_string(&self.probe)?; }",
        },
        Cell {
            situation: "a nullable string in a message with no flexible version",
            ty: FieldType::String,
            nullable: true,
            flexible: "none",
            read: "decoder.read_nullable_string()?",
            write: "encoder.write_nullable_string(self.probe.as_ref())?;",
        },
        Cell {
            situation: "a nullable string present only in flexible versions",
            ty: FieldType::String,
            nullable: true,
            flexible: "0+",
            read: "decoder.read_compact_nullable_string()?",
            write: "encoder.write_compact_nullable_string(self.probe.as_ref())?;",
        },
        Cell {
            situation: "a nullable string straddling the flexible boundary",
            ty: FieldType::String,
            nullable: true,
            flexible: "3+",
            read: "if Self::is_flexible(version) { decoder.read_compact_nullable_string()? } \
                   else { decoder.read_nullable_string()? }",
            write: "if Self::is_flexible(version) { \
                    encoder.write_compact_nullable_string(self.probe.as_ref())?; } \
                    else { encoder.write_nullable_string(self.probe.as_ref())?; }",
        },
        Cell {
            situation: "non-null bytes in a message with no flexible version",
            ty: FieldType::Bytes,
            nullable: false,
            flexible: "none",
            read: "decoder.read_bytes()?",
            write: "encoder.write_bytes(&self.probe)?;",
        },
        Cell {
            situation: "nullable bytes present only in flexible versions",
            ty: FieldType::Bytes,
            nullable: true,
            flexible: "0+",
            read: "decoder.read_compact_nullable_bytes()?",
            write: "encoder.write_compact_nullable_bytes(self.probe.as_deref())?;",
        },
    ]
}

/// The fixed-width scalar half of the table.
///
/// int16, int32, int8, int64, and bool share one wire method on both sides of
/// the flexible boundary, so their rows differ only in the method name and
/// carry no nullable or straddling variants. They are split from the
/// length-prefixed string rows above so neither table function outgrows a
/// single screen.
fn fixed_width_cells() -> Vec<Cell> {
    vec![
        Cell {
            situation: "an int16 in a message with no flexible version",
            ty: FieldType::Int16,
            nullable: false,
            flexible: "none",
            read: "decoder.read_i16()?",
            write: "encoder.write_i16(self.probe)?;",
        },
        Cell {
            situation: "an int16 present only in flexible versions",
            ty: FieldType::Int16,
            nullable: false,
            flexible: "0+",
            read: "decoder.read_i16()?",
            write: "encoder.write_i16(self.probe)?;",
        },
        Cell {
            situation: "an int32 in a message with no flexible version",
            ty: FieldType::Int32,
            nullable: false,
            flexible: "none",
            read: "decoder.read_i32()?",
            write: "encoder.write_i32(self.probe)?;",
        },
        Cell {
            situation: "an int32 present only in flexible versions",
            ty: FieldType::Int32,
            nullable: false,
            flexible: "0+",
            read: "decoder.read_i32()?",
            write: "encoder.write_i32(self.probe)?;",
        },
        Cell {
            situation: "a bool in a message with no flexible version",
            ty: FieldType::Bool,
            nullable: false,
            flexible: "none",
            read: "decoder.read_bool()?",
            write: "encoder.write_bool(self.probe)?;",
        },
        Cell {
            situation: "a bool present only in flexible versions",
            ty: FieldType::Bool,
            nullable: false,
            flexible: "0+",
            read: "decoder.read_bool()?",
            write: "encoder.write_bool(self.probe)?;",
        },
        Cell {
            situation: "an int8 in a message with no flexible version",
            ty: FieldType::Int8,
            nullable: false,
            flexible: "none",
            read: "decoder.read_i8()?",
            write: "encoder.write_i8(self.probe)?;",
        },
        Cell {
            situation: "an int64 in a message with no flexible version",
            ty: FieldType::Int64,
            nullable: false,
            flexible: "none",
            read: "decoder.read_i64()?",
            write: "encoder.write_i64(self.probe)?;",
        },
        Cell {
            situation: "an int64 present only in flexible versions",
            ty: FieldType::Int64,
            nullable: false,
            flexible: "0+",
            read: "decoder.read_i64()?",
            write: "encoder.write_i64(self.probe)?;",
        },
        Cell {
            situation: "a uuid in a message with no flexible version",
            ty: FieldType::Uuid,
            nullable: false,
            flexible: "none",
            read: "decoder.read_uuid()?",
            write: "encoder.write_uuid(self.probe)?;",
        },
        Cell {
            situation: "a struct field, which delegates to the struct's own codec",
            ty: struct_type("TopicData"),
            nullable: false,
            flexible: "none",
            read: "ProbeRequestTopicData::decode(decoder, version)?",
            write: "self.probe.encode(encoder, version)?;",
        },
    ]
}

/// Emits `cell` and returns its rendered read expression and write statement.
fn emit(cell: &Cell) -> (String, String) {
    let mut probe = field("Probe", cell.ty.clone(), "0+");
    if cell.nullable {
        probe = nullable(probe);
    }
    let message = message(VALID, cell.flexible, vec![probe]);
    let probe = &message.fields[0];

    let read = read_expression(probe, &message)
        .unwrap_or_else(|error| panic!("{}: read was rejected: {error}", cell.situation));
    let write = write_statement(probe, &message)
        .unwrap_or_else(|error| panic!("{}: write was rejected: {error}", cell.situation));
    (read, write)
}

#[test]
fn every_supported_field_shape_emits_its_exact_codec_call() {
    for cell in table().into_iter().chain(fixed_width_cells()) {
        let (read, write) = emit(&cell);
        assert_eq!(read, cell.read, "read expression for {}", cell.situation);
        assert_eq!(write, cell.write, "write statement for {}", cell.situation);
    }
}

#[test]
fn a_fixed_width_field_straddling_the_flexible_boundary_emits_no_gate() {
    // int16 and int32 have one encoding on both sides of the boundary. The
    // regime is still chosen before the type is consulted, but a gate whose
    // arms agree says a decision was made where none was, and the lints applied
    // to checked-in output reject the identical branches outright. The
    // collapse is asserted here so that reintroducing the gate is a visible
    // change to this file.
    let probe = field("Probe", FieldType::Int32, "0+");
    let message = message(VALID, "3+", vec![probe]);
    let probe = &message.fields[0];

    assert_eq!(
        read_expression(probe, &message).unwrap_or_else(|error| panic!("{error}")),
        "decoder.read_i32()?"
    );
    assert_eq!(
        write_statement(probe, &message).unwrap_or_else(|error| panic!("{error}")),
        "encoder.write_i32(self.probe)?;"
    );
}

/// Field types with no scalar codec, each of which must be refused by name.
fn types_outside_the_scalar_slice() -> Vec<FieldType> {
    vec![FieldType::Float64, FieldType::Records]
}

#[test]
fn a_type_with_no_codec_fails_generation_instead_of_emitting_a_comment() {
    for ty in types_outside_the_scalar_slice() {
        let probe = field("Probe", ty.clone(), "0+");
        let message = message(VALID, "none", vec![probe]);
        let probe = &message.fields[0];

        let read = read_expression(probe, &message);
        let write = write_statement(probe, &message);

        let read = read.err().unwrap_or_else(|| {
            panic!("{ty:?} rendered a read expression instead of failing generation")
        });
        let write = write.err().unwrap_or_else(|| {
            panic!("{ty:?} rendered a write statement instead of failing generation")
        });

        for (direction, error) in [("read", read.to_string()), ("write", write.to_string())] {
            assert!(
                error.contains("ProbeRequest.Probe") && error.contains(&format!("{ty:?}")),
                "the {direction} rejection for {ty:?} must name the message, the field, \
                 and the construct: {error}"
            );
        }
    }
}

#[test]
fn an_array_routed_into_the_scalar_path_is_refused_rather_than_commented() {
    // `render::api::codec` sends arrays to a statement block, so an array
    // arriving here means a caller lost track of the routing. The previous
    // placeholder comment compiled into the decode body and read nothing.
    let probe = field("Probe", FieldType::Array(Box::new(FieldType::String)), "0+");
    let message = message(VALID, "none", vec![probe]);
    let probe = &message.fields[0];

    let read = read_expression(probe, &message)
        .err()
        .unwrap_or_else(|| panic!("an array rendered a scalar read expression"));
    let write = write_statement(probe, &message)
        .err()
        .unwrap_or_else(|| panic!("an array rendered a scalar write statement"));

    assert!(
        read.to_string().contains("structured block"),
        "the array read rejection must say where arrays are emitted: {read}"
    );
    assert!(
        write.to_string().contains("structured block"),
        "the array write rejection must say where arrays are emitted: {write}"
    );
}
