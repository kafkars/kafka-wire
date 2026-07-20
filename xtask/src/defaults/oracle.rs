//! The defaults transcript's only appeal to an outside authority.
//!
//! This module owns invoking `tools/oracle-java/Oracle.java --defaults` against
//! the pinned Apache Kafka jar, and is the only file under `xtask/src/defaults/`
//! permitted to spawn a process — the same arrangement `vectors/oracle.rs` and
//! `records/oracle.rs` have, and for the same reason. It is reachable from
//! exactly one place, the human-invoked `cargo xtask defaults`, which is what
//! keeps `cargo test` and every check path free of Java.
//!
//! The jar it may use is not its decision. `vectors::oracle_lock` holds that
//! authority, and this module refuses to run until it has answered — so a
//! defaults transcript and a message vector can never be authored by two
//! different builds.

use std::{path::Path, process::Command as Process};

use crate::vectors::oracle_lock;

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

fn ready(workspace: &Path) -> Result<String, String> {
    let lock = oracle_lock::read(workspace)?;
    let jar = oracle_lock::locate_jar(&lock)?;
    oracle_lock::classpath(&jar)
}

/// Ask Kafka what default each field of each named message initializes to.
///
/// The request is `{"messages": [...]}` on stdin and the answer is the transcript
/// on stdout, in the order asked. There is no version guard to prove here as
/// there is for the byte oracle: a default-constructed `<Message>Data` carries
/// no version, so nothing about a version can be got wrong before it is read.
pub(super) fn report(workspace: &Path, request: &str) -> Result<String, String> {
    use std::io::Write as _;

    let classpath = ready(workspace)?;
    let mut child = Process::new(java_executable())
        .arg("-cp")
        .arg(classpath)
        .arg(program(workspace))
        .arg("--defaults")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| format!("could not launch java: {error}"))?;

    child
        .stdin
        .take()
        .ok_or_else(|| "the defaults oracle's stdin was not available".to_owned())?
        .write_all(request.as_bytes())
        .map_err(|error| format!("write the defaults oracle's input: {error}"))?;

    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for the defaults oracle: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "the defaults oracle refused the message list:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
