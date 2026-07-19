//! The presence-gate half of the field-emission table.
//!
//! Scenario: cross a field's `versions` declaration with its message's
//! `validVersions` and assert the exact predicate emitted around the field's
//! read and write. This predicate decides, at runtime, whether a byte is on the
//! wire at all, so an off-by-one here is a framing bug in every message that
//! carries the field.
//!
//! The absent gate matters as much as the present one: a field spanning the
//! whole valid window must emit no condition, because wrapping it in a
//! tautology would put a branch in the hot path for nothing.

use kafka_wire_schema::FieldType;

use super::{
    probe::{field, message},
    validate::validate_supported,
    version::presence_condition,
};

/// One cell of the presence-gate table.
struct Gate {
    /// The protocol situation this cell pins down.
    situation: &'static str,
    /// The message's `validVersions`.
    valid: &'static str,
    /// The field's `versions`.
    present: &'static str,
    /// Exact emitted predicate, or `None` where the field is never gated.
    condition: Option<&'static str>,
}

fn table() -> Vec<Gate> {
    vec![
        Gate {
            situation: "a field present in exactly the message's valid versions",
            valid: "0-4",
            present: "0-4",
            condition: None,
        },
        Gate {
            situation: "an open-ended field in a bounded message",
            valid: "0-4",
            present: "0+",
            condition: None,
        },
        Gate {
            situation: "a field dropped after an early version",
            valid: "0-4",
            present: "0-2",
            condition: Some("version.value() <= 2"),
        },
        Gate {
            situation: "a field added at a later version and never removed",
            valid: "0-4",
            present: "2+",
            condition: Some("version.value() >= 2"),
        },
        Gate {
            situation: "a field that exists in exactly one version",
            valid: "0-4",
            present: "2",
            condition: Some("version.value() == 2"),
        },
        Gate {
            situation: "a field added and later removed",
            valid: "0-4",
            present: "1-3",
            condition: Some("version.value() >= 1 && version.value() <= 3"),
        },
        Gate {
            situation: "a field removed and reinstated",
            valid: "0-4",
            present: "0-1,3-4",
            condition: Some(
                "(version.value() >= 0 && version.value() <= 1) \
                 || (version.value() >= 3 && version.value() <= 4)",
            ),
        },
    ]
}

#[test]
fn every_presence_declaration_emits_its_exact_version_predicate() {
    for gate in table() {
        let probe = field("Probe", FieldType::Int32, gate.present);
        let message = message(gate.valid, "none", vec![probe]);
        let rendered = presence_condition(&message.fields[0], &message);

        assert_eq!(
            rendered.as_deref(),
            gate.condition,
            "presence gate for {}",
            gate.situation
        );
    }
}

#[test]
fn a_field_present_in_no_valid_version_renders_an_empty_predicate() {
    // `versions: "0-1"` against `validVersions: "2-4"` describes a field that
    // exists in no supported version. Upstream ships exactly this shape —
    // `ShareFetchRequest.PartitionMaxBytes` is `versions: "0"` under
    // `validVersions: "1-2"` — so it is reachable, not hypothetical.
    //
    // This function has no error channel and answers with an empty string,
    // which would render `if  { .. }`. Nothing downstream of here would catch
    // that as anything but a rustfmt parse failure, so `validate_supported`
    // rejects the field first and names the real defect. Both halves are
    // asserted together: the day the gate stops being unreachable, this fails.
    let probe = field("Probe", FieldType::Int32, "0-1");
    let message = message("2-4", "none", vec![probe]);

    assert_eq!(
        presence_condition(&message.fields[0], &message).as_deref(),
        Some(""),
        "the version renderer is expected to have no answer for an empty presence set"
    );

    let refusal = validate_supported(&message)
        .err()
        .unwrap_or_else(|| panic!("a field present in no valid version reached the renderer"));
    assert!(
        refusal
            .to_string()
            .contains("declared in no version this message supports"),
        "the refusal must name the empty presence rather than the interval count: {refusal}"
    );
}
