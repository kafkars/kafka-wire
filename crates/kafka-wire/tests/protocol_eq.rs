//! Protocol equality preserves every IEEE-754 payload through containers.
//!
//! Scenario: identical NaNs compare equal, opposite zero signs compare
//! different, and both rules recurse through `Option` and `Vec`.

use kafka_wire::ProtocolEq;

#[test]
fn floats_compare_by_bits_at_every_container_depth() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    assert!(nan.protocol_eq(&nan));
    assert!(!0.0_f64.protocol_eq(&-0.0));

    let left = Some(vec![nan, -0.0]);
    let same = Some(vec![nan, -0.0]);
    let changed_sign = Some(vec![nan, 0.0]);
    assert!(left.protocol_eq(&same));
    assert!(!left.protocol_eq(&changed_sign));
}
