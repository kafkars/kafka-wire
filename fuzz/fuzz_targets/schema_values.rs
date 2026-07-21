//! Fuzzes version algebra, type parsing, and every Rust name normalization case.

#![no_main]

use std::str::FromStr;

use kafka_wire_schema::{FieldName, FieldType, MessageName, RustIdent, VersionSet};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(source) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(versions) = VersionSet::from_str(source) {
        let _ = versions.intersection(&versions);
        let _ = versions.difference(&versions);
        let _ = versions.to_string();
    }
    let _ = RustIdent::snake(source);
    let _ = RustIdent::upper_camel(source);
    let _ = FieldName::try_new(source);
    let _ = MessageName::try_new(source);

    let owner = MessageName::new("FuzzRequest");
    let _ = FieldType::parse(source, &owner);
});
