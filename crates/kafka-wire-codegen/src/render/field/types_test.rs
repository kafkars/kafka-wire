//! The type/default half of the field-emission table.
//!
//! Scenario: for every field type and every protocol default the backend
//! claims to support, assert the exact Rust type declared for the struct field,
//! the exact initializer used when the field is absent from a version, and the
//! exact comparison that decides whether the value still holds its default.
//!
//! Those three answers must agree with each other. A default expression that
//! does not typecheck against the declared type, or a comparison that does not
//! match the initializer, produces a generated file that fails to compile — or,
//! worse, one that compiles and silently writes a value the peer never sent.

use kafka_wire_schema::{DefaultValue, FieldType, FloatDefault};

use super::{
    probe::{field, message, nullable, struct_type},
    types::{default_expression, non_default_condition, rust_type, uses_rust_default},
};

const VALID: &str = "0-4";

/// One cell of the declared-type table.
struct TypeCell {
    ty: FieldType,
    /// Whether `nullableVersions` covers the field.
    nullable: bool,
    /// Exact Rust type emitted into the struct.
    declared: &'static str,
}

fn declared_types() -> Vec<TypeCell> {
    vec![
        TypeCell {
            ty: FieldType::String,
            nullable: false,
            declared: "StrBytes",
        },
        TypeCell {
            ty: FieldType::String,
            nullable: true,
            declared: "Option<StrBytes>",
        },
        TypeCell {
            ty: FieldType::Bool,
            nullable: false,
            declared: "bool",
        },
        TypeCell {
            ty: FieldType::Int8,
            nullable: false,
            declared: "i8",
        },
        TypeCell {
            ty: FieldType::Int16,
            nullable: false,
            declared: "i16",
        },
        TypeCell {
            ty: FieldType::Int32,
            nullable: false,
            declared: "i32",
        },
        TypeCell {
            ty: FieldType::Int64,
            nullable: false,
            declared: "i64",
        },
        TypeCell {
            ty: FieldType::Uuid,
            nullable: false,
            declared: "Uuid",
        },
        TypeCell {
            ty: FieldType::Array(Box::new(FieldType::String)),
            nullable: false,
            declared: "Vec<StrBytes>",
        },
        TypeCell {
            ty: FieldType::Array(Box::new(FieldType::String)),
            nullable: true,
            declared: "Option<Vec<StrBytes>>",
        },
        TypeCell {
            ty: FieldType::Array(Box::new(FieldType::Int32)),
            nullable: false,
            declared: "Vec<i32>",
        },
        TypeCell {
            ty: struct_type("TopicData"),
            nullable: false,
            declared: "ProbeRequestTopicData",
        },
        TypeCell {
            ty: FieldType::Array(Box::new(struct_type("TopicData"))),
            nullable: false,
            declared: "Vec<ProbeRequestTopicData>",
        },
    ]
}

#[test]
fn every_supported_field_shape_declares_its_exact_rust_type() {
    for cell in declared_types() {
        let mut probe = field("Probe", cell.ty.clone(), "0+");
        if cell.nullable {
            probe = nullable(probe);
        }
        let message = message(VALID, "none", vec![probe]);
        let rendered = rust_type(&message.fields[0], &message)
            .unwrap_or_else(|error| panic!("{:?} has no Rust type: {error}", cell.ty));

        assert_eq!(rendered, cell.declared, "declared type for {:?}", cell.ty);
    }
}

#[test]
fn a_type_outside_the_slice_fails_generation_instead_of_emitting_a_comment() {
    // `/* unsupported Float64 */` in a struct-field position is a syntax error,
    // so rustfmt would have caught it. The same placeholder in a default or a
    // comparison position is valid Rust, which is why all three go through the
    // same refusal.
    for ty in [
        FieldType::Uint16,
        FieldType::Uint32,
        FieldType::Float64,
        FieldType::Bytes,
        FieldType::Records,
    ] {
        let probe = field("Probe", ty.clone(), "0+");
        let message = message(VALID, "none", vec![probe]);
        let error = rust_type(&message.fields[0], &message)
            .err()
            .unwrap_or_else(|| panic!("{ty:?} was given a Rust type outside the backend slice"));

        assert!(
            error.to_string().contains("ProbeRequest.Probe")
                && error.to_string().contains(&format!("{ty:?}")),
            "the type rejection must name the message, the field, and the construct: {error}"
        );
    }
}

/// One cell of the default table: initializer and the test for "not default".
struct DefaultCell {
    /// The protocol situation this cell pins down.
    situation: &'static str,
    ty: FieldType,
    default: DefaultValue,
    /// Exact initializer used where the field is absent from a version.
    initializer: &'static str,
    /// Exact condition that is true when the value differs from the default.
    non_default: &'static str,
    /// Whether `#[derive(Default)]` alone reproduces this default.
    derivable: bool,
    /// Whether `nullableVersions` covers the field.
    nullable: bool,
}

fn defaults() -> Vec<DefaultCell> {
    vec![
        DefaultCell {
            situation: "a field whose protocol default is null",
            ty: FieldType::String,
            default: DefaultValue::Null,
            initializer: "None",
            non_default: "self.probe.is_some()",
            derivable: true,
            nullable: false,
        },
        DefaultCell {
            situation: "an int16 defaulting to zero",
            ty: FieldType::Int16,
            default: DefaultValue::Integer(0),
            initializer: "0",
            non_default: "self.probe != 0",
            derivable: true,
            nullable: false,
        },
        DefaultCell {
            situation: "an int32 defaulting to the sentinel -1",
            ty: FieldType::Int32,
            default: DefaultValue::Integer(-1),
            initializer: "-1",
            non_default: "self.probe != -1",
            derivable: false,
            nullable: false,
        },
        DefaultCell {
            situation: "a string defaulting to empty",
            ty: FieldType::String,
            default: DefaultValue::String(String::new()),
            initializer: "StrBytes::default()",
            non_default: "!self.probe.is_empty()",
            derivable: true,
            nullable: false,
        },
        DefaultCell {
            situation: "a string defaulting to a protocol literal",
            ty: FieldType::String,
            default: DefaultValue::String("PLAINTEXT".to_owned()),
            initializer: "StrBytes::from(\"PLAINTEXT\")",
            non_default: "self.probe.as_str() != \"PLAINTEXT\"",
            derivable: false,
            nullable: false,
        },
        DefaultCell {
            situation: "an array defaulting to empty",
            ty: FieldType::Array(Box::new(FieldType::String)),
            default: DefaultValue::Empty,
            initializer: "Vec::new()",
            non_default: "!self.probe.is_empty()",
            derivable: true,
            nullable: false,
        },
        DefaultCell {
            situation: "a bool defaulting to false",
            ty: FieldType::Bool,
            default: DefaultValue::Bool(false),
            initializer: "false",
            non_default: "self.probe != false",
            derivable: true,
            nullable: false,
        },
        DefaultCell {
            situation: "a uuid defaulting to the all-zero sentinel",
            ty: FieldType::Uuid,
            default: DefaultValue::Uuid([0; 16]),
            initializer: "Uuid::ZERO",
            non_default: "self.probe != Uuid::ZERO",
            derivable: true,
            nullable: false,
        },
        DefaultCell {
            situation: "a nullable string defaulting to a protocol literal",
            ty: FieldType::String,
            default: DefaultValue::String("PLAINTEXT".to_owned()),
            initializer: "Some(StrBytes::from(\"PLAINTEXT\"))",
            non_default: "self.probe != Some(StrBytes::from(\"PLAINTEXT\"))",
            derivable: false,
            nullable: true,
        },
        DefaultCell {
            situation: "a non-nullable struct, absent as every member at its own default",
            ty: struct_type("TopicData"),
            default: DefaultValue::StructDefaults,
            initializer: "ProbeRequestTopicData::default()",
            non_default: "self.probe != ProbeRequestTopicData::default()",
            derivable: true,
            nullable: false,
        },
    ]
}

#[test]
fn every_supported_default_emits_its_exact_initializer_and_comparison() {
    for cell in defaults() {
        let mut probe = field("Probe", cell.ty.clone(), "0+");
        if cell.nullable {
            probe = nullable(probe);
        }
        probe.default = cell.default.clone();
        let message = message(VALID, "none", vec![probe]);
        let probe = &message.fields[0];

        let initializer = default_expression(probe, &message)
            .unwrap_or_else(|error| panic!("{}: no initializer: {error}", cell.situation));
        let non_default = non_default_condition(probe, &message)
            .unwrap_or_else(|error| panic!("{}: no comparison: {error}", cell.situation));

        assert_eq!(
            initializer, cell.initializer,
            "initializer for {}",
            cell.situation
        );
        assert_eq!(
            non_default, cell.non_default,
            "comparison for {}",
            cell.situation
        );
        assert_eq!(
            uses_rust_default(probe),
            cell.derivable,
            "derive(Default) suitability for {}",
            cell.situation
        );
    }
}

#[test]
fn a_default_with_no_rust_form_fails_generation_instead_of_emitting_a_comment() {
    // These arrived with wave 1's front end, which now lowers float64 and
    // non-nullable struct fields to typed defaults. The backend has no Rust
    // form for either yet. Emitting `/* unsupported */` as the initializer of a
    // `Default` impl is valid Rust in exactly the position where being wrong is
    // unobservable, so it must fail instead. (Uuid defaults now render, so they
    // have moved to the positive default table above.)
    for default in [DefaultValue::Float(FloatDefault::new(1.0))] {
        let mut probe = field("Probe", FieldType::String, "0+");
        probe.default = default.clone();
        let message = message(VALID, "none", vec![probe]);
        let probe = &message.fields[0];

        for (role, error) in [
            ("initializer", default_expression(probe, &message).err()),
            ("comparison", non_default_condition(probe, &message).err()),
        ] {
            let error = error.unwrap_or_else(|| {
                panic!("{default:?} rendered a {role} instead of failing generation")
            });
            assert!(
                error.to_string().contains("ProbeRequest.Probe"),
                "the {role} rejection must name the message and field: {error}"
            );
        }
    }
}

#[test]
fn a_nullable_fixed_width_field_is_excluded_by_the_front_end_not_by_this_backend() {
    // Rendered in isolation, an int32 declared nullable produces `Option<i32>`
    // paired with `decoder.read_i32()?`, which does not typecheck. Nothing in
    // this backend rejects the combination: `FieldType::permits_null` in
    // kafka-wire-schema is the only thing standing between it and a generated file
    // that will not compile. This test records that dependency so that relaxing
    // the front-end rule fails here first.
    let probe = nullable(field("Probe", FieldType::Int32, "0+"));
    let message = message(VALID, "none", vec![probe]);
    let probe = &message.fields[0];

    assert_eq!(
        rust_type(probe, &message).unwrap_or_else(|error| panic!("{error}")),
        "Option<i32>"
    );
    assert!(
        !FieldType::Int32.permits_null(),
        "kafka-wire-schema must keep rejecting nullableVersions on a fixed-width type; \
         this backend does not check it"
    );
}
