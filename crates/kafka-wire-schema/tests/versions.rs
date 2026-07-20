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

#[test]
fn subtracts_open_ranges_without_losing_the_unbounded_tail() {
    let left: VersionSet = "3+".parse().unwrap();
    let open_right: VersionSet = "5+".parse().unwrap();
    let bounded_right: VersionSet = "5-7".parse().unwrap();

    assert_eq!(left.difference(&open_right).to_string(), "3-4");
    assert_eq!(left.difference(&bounded_right).to_string(), "3-4,8+");
}

#[test]
fn difference_membership_matches_set_subtraction() {
    let left: VersionSet = "0-4,7+".parse().unwrap();
    let right: VersionSet = "2-3,9-11,14+".parse().unwrap();
    let difference = left.difference(&right);

    for version in 0..=20 {
        assert_eq!(
            difference.contains(version),
            left.contains(version) && !right.contains(version),
            "v{version} disagrees with set subtraction"
        );
    }
    assert_eq!(difference.to_string(), "0-1,4,7-8,12-13");
}
