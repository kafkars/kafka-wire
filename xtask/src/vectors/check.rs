//! Verifying the corpus: prove it still describes the plans, without Java.
//!
//! This module owns the reading direction of `cargo xtask vectors`, and it is
//! the half CI runs. It reaches no process, no network, and no container: it
//! compares the checked-in vector files against the authored plans and refuses a
//! corpus that has drifted, gone missing, or shrunk to nothing.
//!
//! It deliberately does not decode or re-encode a single vector. Proving that
//! this repository agrees with these bytes is the conformance crate's job, and
//! keeping the two apart is what stops the corpus from being validated by the
//! implementation it judges.

use std::{collections::BTreeMap, path::Path};

use super::corpus::{self, Plan, SCHEMA, VectorFile};

/// Smallest corpus that can honestly be called coverage.
///
/// A check that passes over an empty `spec/vectors/` is worse than no check: it
/// reports success for a repository that proves nothing. This floor, together
/// with the per-version coverage requirement below, makes a vacuous pass
/// impossible rather than merely unlikely.
const MINIMUM_VECTORS: usize = 48;

pub(crate) fn check(workspace: &Path) -> Result<(), String> {
    let plans = corpus::load_plans(workspace)?;
    let files = corpus::load_vector_files(workspace)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    let mut findings = Vec::new();
    let mut expected = Vec::new();
    let mut total = 0;

    for plan in &plans {
        for version in &plan.valid_versions {
            let path = format!("spec/vectors/{}/v{version}.json", plan.message);
            expected.push(path.clone());

            let Some(file) = files.get(&path) else {
                findings.push(format!(
                    "{path} is missing; {} declares version {version} valid, so leaving it \
                     uncovered would hide every encoding decision that version makes",
                    plan.message
                ));
                continue;
            };
            if file.schema != SCHEMA {
                findings.push(format!("{path}: schema {} is not {SCHEMA}", file.schema));
            }
            total += file.vectors.len();
            findings.extend(judge_file(&path, plan, *version, file));
        }
    }

    for orphan in files.keys().filter(|path| !expected.contains(path)) {
        findings.push(format!(
            "{orphan} has no authoring plan; refresh the corpus or remove the file"
        ));
    }
    if total < MINIMUM_VECTORS {
        findings.push(format!(
            "the corpus holds {total} vector(s), below the floor of {MINIMUM_VECTORS}; \
             a check that passes over an empty corpus proves nothing"
        ));
    }

    if !findings.is_empty() {
        return Err(format!(
            "vector corpus findings:\n  {}",
            findings.join("\n  ")
        ));
    }
    println!(
        "vector corpus is current: {total} vector(s) across {} file(s), authored by Apache Kafka",
        files.len()
    );
    Ok(())
}

/// Compare one checked-in file against the cases its plan authors for it.
pub(crate) fn judge_file(path: &str, plan: &Plan, version: i16, file: &VectorFile) -> Vec<String> {
    let authored = plan
        .cases
        .iter()
        .filter(|case| case.versions.contains(&version))
        .collect::<Vec<_>>();
    let mut findings = Vec::new();

    if authored.len() != file.vectors.len() {
        findings.push(format!(
            "{path}: plan authors {} case(s) at this version but the file holds {}",
            authored.len(),
            file.vectors.len()
        ));
        return findings;
    }

    for (case, vector) in authored.iter().zip(&file.vectors) {
        let at = format!("{path} [{}]", vector.name);
        if case.name != vector.name {
            findings.push(format!("{at}: plan authors case `{}` here", case.name));
        }
        if case.why != vector.why {
            findings.push(format!("{at}: why has drifted from the plan"));
        }
        if case.json_value != vector.json_value {
            findings.push(format!("{at}: json_value has drifted from the plan"));
        }
        if case.unknown_tagged_fields != vector.unknown_tagged_fields {
            findings.push(format!(
                "{at}: unknown_tagged_fields have drifted from the plan"
            ));
        }
        if vector.message != plan.message || vector.api_key != plan.api_key {
            findings.push(format!("{at}: message identity disagrees with the plan"));
        }
        if vector.direction != plan.direction || vector.version != version {
            findings.push(format!(
                "{at}: direction or version disagrees with the plan"
            ));
        }
        if vector.flexible != plan.flexible_versions.contains(&version) {
            findings.push(format!(
                "{at}: flexible is {} but the plan declares flexible versions {:?}",
                vector.flexible, plan.flexible_versions
            ));
        }
        findings.extend(judge_hex(&at, &vector.hex));
    }

    findings
}

/// Reject a hex body that no byte string could have produced.
fn judge_hex(at: &str, hex: &str) -> Option<String> {
    if hex.len() % 2 != 0 {
        return Some(format!("{at}: hex has an odd number of digits"));
    }
    if !hex
        .bytes()
        .all(|digit| matches!(digit, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Some(format!(
            "{at}: hex must be lowercase hexadecimal so the corpus has one spelling"
        ));
    }
    None
}
