//! IEEE-754 defaults retain their exact payload through Rust emission.
//!
//! Scenario: positive and negative finite values, both zero signs, infinities,
//! and a NaN payload render through `from_bits` and compare by the same bits.

use kafka_wire_schema::{DefaultValue, FieldType, FloatDefault};

use super::{
    probe::{field, message},
    types::{default_expression, non_default_condition},
};

#[test]
fn every_float_shape_emits_its_exact_bits() {
    for (situation, value, bits) in [
        ("positive finite", 1.0, 0x3ff0_0000_0000_0000_u64),
        ("positive fractional", 0.25, 0x3fd0_0000_0000_0000),
        ("negative finite", -1.0, 0xbff0_0000_0000_0000),
        ("positive zero", 0.0, 0x0000_0000_0000_0000),
        ("negative zero", -0.0, 0x8000_0000_0000_0000),
        ("positive infinity", f64::INFINITY, 0x7ff0_0000_0000_0000),
        (
            "negative infinity",
            f64::NEG_INFINITY,
            0xfff0_0000_0000_0000,
        ),
        (
            "NaN payload",
            f64::from_bits(0x7ff8_0000_0000_0042),
            0x7ff8_0000_0000_0042,
        ),
    ] {
        let mut probe = field("Probe", FieldType::Float64, "0+");
        probe.default = DefaultValue::Float(FloatDefault::new(value));
        let message = message("0-4", "none", vec![probe]);
        let field = &message.fields[0];
        let grouped = format_bits(bits);

        assert_eq!(
            default_expression(field, &message),
            format!("f64::from_bits({grouped})"),
            "initializer for {situation}"
        );
        assert_eq!(
            non_default_condition(field, &message),
            format!("self.probe.to_bits() != {grouped}"),
            "comparison for {situation}"
        );
    }
}

fn format_bits(bits: u64) -> String {
    format!(
        "0x{:04x}_{:04x}_{:04x}_{:04x}_u64",
        bits >> 48,
        (bits >> 32) & 0xffff,
        (bits >> 16) & 0xffff,
        bits & 0xffff
    )
}
