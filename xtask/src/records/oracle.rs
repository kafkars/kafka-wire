//! The record corpus's only appeal to an outside authority.
//!
//! This module owns invoking `tools/oracle-java/RecordOracle.java` against the
//! pinned Apache Kafka jar, and is the only file under `xtask/src/records/`
//! permitted to spawn a process — the same arrangement `vectors/oracle.rs` has,
//! and for the same reason. It is reachable from exactly one place, the
//! human-invoked `cargo xtask records --refresh`, which is what keeps
//! `--check` provably Java-free.
//!
//! The jar it may use is not its decision. `vectors::oracle_lock` holds that
//! authority, and this module refuses to run until it has answered — so a record
//! batch and a message vector can never be authored by two different builds.

use std::{path::Path, process::Command as Process};

use crate::vectors::oracle_lock;

fn java_executable() -> String {
    std::env::var("JAVA_HOME").map_or_else(|_| "java".to_owned(), |home| format!("{home}/bin/java"))
}

fn program(workspace: &Path) -> String {
    workspace
        .join("tools")
        .join("oracle-java")
        .join("RecordOracle.java")
        .to_string_lossy()
        .into_owned()
}

fn ready(workspace: &Path) -> Result<String, String> {
    let lock = oracle_lock::read(workspace)?;
    let jar = oracle_lock::locate_jar(&lock)?;
    oracle_lock::classpath(&jar)
}

/// Prove every codec is on the classpath and every batch builds reproducibly.
///
/// snappy, lz4, and zstd are not bundled in the clients jar, so without this the
/// refresh would fail late and name a Java class rather than a missing codec.
/// The determinism half matters more: several `MemoryRecords.builder` overloads
/// take the log-append timestamp from the wall clock, and a corpus that churned
/// on every refresh would make its own diffs meaningless.
pub(super) fn self_test(workspace: &Path) -> Result<String, String> {
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
            "the record oracle could not prove its own preconditions; refusing to author \
             anything:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Ask Kafka's producer machinery to lay out every batch the plans call for.
pub(super) fn encode(workspace: &Path, plans: &str) -> Result<String, String> {
    use std::io::Write as _;

    let classpath = ready(workspace)?;
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
        .ok_or_else(|| "the record oracle's stdin was not available".to_owned())?
        .write_all(plans.as_bytes())
        .map_err(|error| format!("write the batch plans: {error}"))?;

    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for the record oracle: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "the record oracle refused the plans:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
