//! Rust identifier normalization across editions and hostile source spellings.
//!
//! The same checked type owns casing and keyword escaping for every generated
//! namespace; malformed schema names are rejected before entering the IR.

#![allow(clippy::unwrap_used)]

use kafka_wire_schema::{FieldName, MessageName, RustIdent, StructRef};

#[test]
fn current_and_future_reserved_words_share_one_escape_policy() {
    assert_eq!(RustIdent::snake("type").unwrap().as_str(), "type_");
    assert_eq!(RustIdent::snake("gen").unwrap().as_str(), "gen_");
    assert_eq!(RustIdent::upper_camel("Self").unwrap().as_str(), "Self_");

    let message = MessageName::try_new("gen").unwrap();
    assert_eq!(message.rust_module(), "gen_");
    assert_eq!(message.descriptor_symbol(), "GEN_");
}

#[test]
fn malformed_normalized_names_are_rejected() {
    for source in ["", "_", "---", "123Name"] {
        assert!(
            RustIdent::snake(source).is_err(),
            "{source:?} unexpectedly became a Rust identifier"
        );
    }
}

#[test]
fn source_names_cannot_carry_physical_line_boundaries() {
    for source in [
        "Line\nBreak",
        "Carriage\rReturn",
        "Control\u{7f}Byte",
        "Unicode\u{2028}Line",
        "Unicode\u{2029}Paragraph",
    ] {
        assert!(MessageName::try_new(source).is_err(), "message {source:?}");
        assert!(FieldName::try_new(source).is_err(), "field {source:?}");
        let owner = MessageName::new("ExampleRequest");
        assert!(
            StructRef::try_qualify(&owner, source).is_err(),
            "struct {source:?}"
        );
    }
}

#[test]
fn unicode_identifiers_are_validated_by_the_rust_parser() {
    assert_eq!(RustIdent::snake("ÜberCafé").unwrap().as_str(), "über_café");
}

#[test]
fn collisions_are_visible_after_casing_and_keyword_escaping() {
    assert_eq!(
        FieldName::try_new("HostName").unwrap().rust_field(),
        FieldName::try_new("hostName").unwrap().rust_field()
    );
    assert_eq!(
        FieldName::try_new("type").unwrap().rust_field(),
        FieldName::try_new("type_").unwrap().rust_field()
    );
}
