//! Scenario: a correctly declared and gated sibling unit test.

use super::limits::DecodeLimits;

#[test]
fn permits_a_length_within_the_limit() {
    assert!(DecodeLimits { max_bytes: 8 }.permits(8));
}
