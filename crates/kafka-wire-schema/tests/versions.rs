//! Version expressions normalize into stable interval algebra.

#![allow(clippy::unwrap_used)]

use kafka_wire_schema::VersionSet;

#[test]
fn parses_and_merges_adjacent_ranges() {
    let versions: VersionSet = "0-2,3,5+".parse().unwrap();

    assert_eq!(versions.to_string(), "0-3,5+");
    assert!(versions.contains(0));
    assert!(!versions.contains(4));
    assert!(versions.contains(99));
}

#[test]
fn intersects_open_ranges_with_bounded_message_versions() {
    let declared: VersionSet = "3+".parse().unwrap();
    let valid: VersionSet = "0-5".parse().unwrap();

    assert_eq!(declared.intersection(&valid).to_string(), "3-5");
}
