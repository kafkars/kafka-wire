//! Scenario: a sibling unit test declared without a `#[cfg(test)]` gate.

use super::limits::DecodeLimits;

#[test]
fn rejects_a_length_past_the_limit() {
    assert!(!DecodeLimits { max_bytes: 8 }.permits(9));
}
