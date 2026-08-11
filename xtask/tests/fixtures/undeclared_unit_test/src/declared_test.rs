//! Scenario: a declared sibling unit test that the compiler actually builds.

use super::limits::DecodeLimits;

#[test]
fn permits_a_length_within_the_limit() {
    assert!(DecodeLimits { max_bytes: 8 }.permits(8));
}
