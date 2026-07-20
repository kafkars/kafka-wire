//! Authoring the corpus: ask Apache Kafka what the bytes are, then record them.
//!
//! This module owns the writing direction of `cargo xtask vectors`. It reads the
//! authored plans, proves the oracle's version guard still fires, asks Kafka's
//! own generated writer for every case, and rewrites `spec/vectors/`. It needs a
//! Java toolchain and the pinned jar, and is run by a human on purpose.
//!
//! It deliberately computes no byte. Every `hex` it writes arrives from the
//! oracle unmodified: a vector this repository derived from its own encoder
//! would prove only that the encoder agrees with itself.

use std::{collections::BTreeMap, path::Path};

use super::corpus::{self, Plan, PlanCase, SCHEMA, Vector, VectorFile};
use super::oracle;

pub(crate) fn refresh(workspace: &Path) -> Result<(), String> {
    let plans = corpus::load_plans(workspace)?;

    println!("proving the oracle version guard before authoring anything:");
    for line in oracle::self_test(workspace)?.lines() {
        println!("  {line}");
    }

    let mut answers = oracle::encode(workspace, &plans)?.into_iter();
    let mut files: BTreeMap<(String, i16), Vec<Vector>> = BTreeMap::new();

    for plan in &plans {
        for case in &plan.cases {
            for version in &case.versions {
                let answer = answers
                    .next()
                    .ok_or_else(|| "the oracle returned fewer answers than asked".to_owned())?;

                if answer.message != plan.message || answer.version != *version {
                    return Err(format!(
                        "the oracle answered {} v{} where {} v{version} was asked; \
                         batch order is not reliable",
                        answer.message, answer.version, plan.message
                    ));
                }
                if plan.api_key.is_some_and(|key| key != answer.api_key) {
                    return Err(format!(
                        "{} declares api key {} but Kafka reports {}",
                        plan.message,
                        plan.api_key.unwrap_or(answer.api_key),
                        answer.api_key
                    ));
                }

                files
                    .entry((plan.message.clone(), *version))
                    .or_default()
                    .push(vector(plan, case, *version, answer.hex));
            }
        }
    }

    if answers.next().is_some() {
        return Err("the oracle returned more answers than asked".to_owned());
    }
    write_all(workspace, &files)
}

fn vector(plan: &Plan, case: &PlanCase, version: i16, hex: String) -> Vector {
    Vector {
        name: case.name.clone(),
        why: case.why.clone(),
        message: plan.message.clone(),
        api_key: plan.api_key,
        direction: plan.direction,
        version,
        flexible: plan.flexible_versions.contains(&version),
        json_value: case.json_value.clone(),
        unknown_tagged_fields: case.unknown_tagged_fields.clone(),
        hex,
    }
}

/// Write the refreshed corpus and remove files no plan authors any more.
fn write_all(workspace: &Path, files: &BTreeMap<(String, i16), Vec<Vector>>) -> Result<(), String> {
    let existing = corpus::load_vector_files(workspace)
        .unwrap_or_default()
        .into_iter()
        .map(|(path, _)| path)
        .collect::<Vec<_>>();

    let mut written = Vec::new();
    let mut total = 0;
    for ((message, version), vectors) in files {
        total += vectors.len();
        let file = VectorFile {
            schema: SCHEMA,
            vectors: vectors.clone(),
        };
        written.push(corpus::write_vector_file(
            workspace, message, *version, &file,
        )?);
    }

    for stale in existing.iter().filter(|path| !written.contains(path)) {
        corpus::remove_vector_file(workspace, stale)?;
        println!("removed no-longer-authored vector file: {stale}");
    }

    println!(
        "refreshed {total} broker-authored vector(s) across {} file(s)",
        written.len()
    );
    Ok(())
}
