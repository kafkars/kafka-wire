//! Focused failures for metadata the plan and Kafka-authored vector repeat.
//!
//! These tests keep the offline checker honest about descriptive intent and
//! unknown tagged bytes, neither of which may drift independently after the
//! Java oracle has authored a vector.

use serde_json::json;

use crate::vectors::{
    Direction, Plan, PlanCase, SCHEMA, TaggedFieldPlan, Vector, VectorFile, judge_file,
};

#[test]
fn changed_case_rationale_is_rejected() {
    let (plan, mut file) = matching_pair();
    file.vectors[0].why = "different evidence".into();

    let findings = judge_file("spec/vectors/Example/v3.json", &plan, 3, &file);

    assert_eq!(
        findings,
        ["spec/vectors/Example/v3.json [non_default]: why has drifted from the plan"]
    );
}

#[test]
fn changed_unknown_tagged_bytes_are_rejected() {
    let (plan, mut file) = matching_pair();
    file.vectors[0].unknown_tagged_fields[0].data_hex = "cafe".into();

    let findings = judge_file("spec/vectors/Example/v3.json", &plan, 3, &file);

    assert_eq!(
        findings,
        [
            "spec/vectors/Example/v3.json [non_default]: unknown_tagged_fields have drifted \
             from the plan"
        ]
    );
}

fn matching_pair() -> (Plan, VectorFile) {
    let tagged_field = TaggedFieldPlan {
        tag: 7,
        data_hex: "beef".into(),
    };
    let plan = Plan {
        schema: SCHEMA,
        message: "Example".into(),
        api_key: Some(42),
        direction: Direction::Request,
        valid_versions: vec![3],
        flexible_versions: vec![3],
        source: "Example.json".into(),
        cases: vec![PlanCase {
            name: "non_default".into(),
            why: "exercise the full shape".into(),
            versions: vec![3],
            json_value: json!({"value": 1}),
            unknown_tagged_fields: vec![tagged_field.clone()],
        }],
    };
    let file = VectorFile {
        schema: SCHEMA,
        vectors: vec![Vector {
            name: "non_default".into(),
            why: "exercise the full shape".into(),
            message: "Example".into(),
            api_key: Some(42),
            direction: Direction::Request,
            version: 3,
            flexible: true,
            json_value: json!({"value": 1}),
            unknown_tagged_fields: vec![tagged_field],
            hex: "00".into(),
        }],
    };
    (plan, file)
}
