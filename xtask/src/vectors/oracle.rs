//! The repository's only appeal to an outside protocol authority.
//!
//! This module owns invoking `tools/oracle-java/Oracle.java` against the pinned
//! Apache Kafka `clients` jar, and is the only file under `xtask/src/vectors/`
//! permitted to spawn a process. It is reachable from exactly one place: the
//! human-invoked `cargo xtask vectors --refresh`. Nothing on the build, test,
//! check, or generation path may call it, which is what keeps `cargo test` and
//! `cargo xtask vectors --check` free of Java entirely.
//!
//! It deliberately owns no vector policy and no file format, and it does not
//! decide which jar is legitimate — `oracle_lock` holds that authority and this
//! module refuses to run until it has answered.

use std::{path::Path, process::Command as Process};

use serde::Deserialize;

use super::corpus::{Plan, PlanCase, TaggedFieldPlan};
use super::oracle_lock;

/// One encoding question for the oracle.
#[derive(Debug, serde::Serialize)]
struct OracleRequest<'a> {
    message: &'a str,
    version: i16,
    json_value: &'a serde_json::Value,
    unknown_tagged_fields: &'a [TaggedFieldPlan],
}

#[derive(Debug, serde::Serialize)]
struct OracleBatch<'a> {
    requests: Vec<OracleRequest<'a>>,
}

/// One answer: the bytes Kafka's generated writer produced.
#[derive(Debug, Deserialize)]
pub(crate) struct OracleAnswer {
    pub(crate) message: String,
    pub(crate) version: i16,
    pub(crate) api_key: i16,
    pub(crate) hex: String,
}

#[derive(Debug, Deserialize)]
struct OracleResponse {
    results: Vec<OracleAnswer>,
}

/// Prove the version guard fires before letting the oracle mint any vector.
///
/// Kafka's generated `write` gates every field with `if (_version >= N)` and
/// checks no upper bound, so asked to write at a version the jar does not know
/// it emits the nearest layout it does know and returns normally. A corpus
/// refreshed without this proof could be confidently wrong at exactly the
/// versions that matter most. Running it every refresh means the guard cannot
/// rot unnoticed.
pub(crate) fn self_test(workspace: &Path) -> Result<String, String> {
    let classpath = ready(workspace)?;
    let output = Process::new(java_executable())
        .arg("-cp")
        .arg(classpath)
        .arg(program(workspace))
        .arg("--self-test")
        .output()
        .map_err(|error| format!("could not launch java: {error}"))?;

    if !output.status.success() {
        return Err(format!(
            "the oracle version guard failed its own proof; refusing to author vectors:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Ask Kafka's own writer for every vector the plans call for, in one batch.
pub(crate) fn encode(workspace: &Path, plans: &[Plan]) -> Result<Vec<OracleAnswer>, String> {
    let classpath = ready(workspace)?;

    let requests = plans
        .iter()
        .flat_map(|plan| {
            plan.cases
                .iter()
                .flat_map(move |case| requests_for(plan, case))
        })
        .collect::<Vec<_>>();
    let batch = serde_json::to_string(&OracleBatch { requests })
        .map_err(|error| format!("render oracle batch: {error}"))?;

    let output = run(workspace, &classpath, &batch)?;
    let response: OracleResponse = serde_json::from_str(&output)
        .map_err(|error| format!("parse oracle response: {error}\n{output}"))?;
    Ok(response.results)
}

/// Resolve the classpath, refusing unless the jar in hand is the pinned one.
fn ready(workspace: &Path) -> Result<String, String> {
    let lock = oracle_lock::read(workspace)?;
    let jar = oracle_lock::locate_jar(&lock)?;
    oracle_lock::classpath(&jar)
}

fn requests_for<'a>(plan: &'a Plan, case: &'a PlanCase) -> Vec<OracleRequest<'a>> {
    case.versions
        .iter()
        .map(|version| OracleRequest {
            message: &plan.message,
            version: *version,
            json_value: &case.json_value,
            unknown_tagged_fields: &case.unknown_tagged_fields,
        })
        .collect()
}

fn java_executable() -> String {
    std::env::var("JAVA_HOME").map_or_else(|_| "java".to_owned(), |home| format!("{home}/bin/java"))
}

fn program(workspace: &Path) -> String {
    workspace
        .join("tools")
        .join("oracle-java")
        .join("Oracle.java")
        .to_string_lossy()
        .into_owned()
}

/// Spawn the oracle with the batch on stdin and collect its stdout.
fn run(workspace: &Path, classpath: &str, batch: &str) -> Result<String, String> {
    use std::io::Write;

    let mut child = Process::new(java_executable())
        .arg("-cp")
        .arg(classpath)
        .arg(program(workspace))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| format!("could not launch java: {error}"))?;

    child
        .stdin
        .take()
        .ok_or_else(|| "oracle stdin was not available".to_owned())?
        .write_all(batch.as_bytes())
        .map_err(|error| format!("write oracle batch: {error}"))?;

    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for oracle: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "oracle refused the batch:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
