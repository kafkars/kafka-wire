//! Which Apache Kafka build is allowed to author this repository's byte vectors.
//!
//! This module owns `spec/oracle.lock` and the identity check that runs before
//! any refresh: it reads the recorded provenance, locates the jar the caller
//! offered, and refuses to proceed unless that jar is the one the lock names.
//!
//! It deliberately spawns nothing and knows nothing about JSON, versions, or
//! vectors. Separating "which jar is legitimate" from "ask it a question" is
//! what keeps the process-spawning capability confined to one sibling file.

use std::{env, fs, path::Path};

use serde::{Deserialize, Serialize};

/// Environment variable naming the pinned Kafka `clients` jar.
pub(crate) const JAR_VARIABLE: &str = "KAFKA_ORACLE_JAR";

/// Environment variable naming the jar's compile dependencies, chiefly Jackson.
pub(crate) const CLASSPATH_VARIABLE: &str = "KAFKA_ORACLE_CLASSPATH";

/// Which jar authored the checked-in corpus, and under which toolchain.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OracleLock {
    pub(crate) schema: u32,
    pub(crate) oracle: OracleIdentity,
}

/// Provenance of one built oracle jar.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OracleIdentity {
    pub(crate) source_repository: String,
    /// Must equal the commit `spec/protocol.lock` pins.
    pub(crate) source_commit: String,
    pub(crate) gradle_task: String,
    pub(crate) jar_name: String,
    pub(crate) jar_sha256: String,
    pub(crate) jdk_version: String,
    pub(crate) jdk_build: String,
    /// Whether rebuilding the jar reproduces `jar_sha256` byte for byte.
    pub(crate) jar_is_reproducible: bool,
    pub(crate) note: String,
}

pub(crate) fn read(workspace: &Path) -> Result<OracleLock, String> {
    let path = workspace.join("spec").join("oracle.lock");
    let source =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let lock: OracleLock =
        toml::from_str(&source).map_err(|error| format!("parse {}: {error}", path.display()))?;

    if lock.schema != 1 {
        return Err(format!(
            "{}: oracle lock schema {} is not the supported schema 1",
            path.display(),
            lock.schema
        ));
    }
    Ok(lock)
}

/// Locate the pinned jar and prove it is the one that authored this corpus.
///
/// Kafka's build sets `preserveFileTimestamps = true`, so a rebuilt jar is a
/// different file even from identical sources. The digest therefore identifies
/// one artifact rather than one source tree, and a mismatch is reported as a
/// decision for a human rather than quietly accepted: the source commit is the
/// claim that carries protocol meaning, and re-recording the digest should
/// follow from having checked it.
pub(crate) fn locate_jar(lock: &OracleLock) -> Result<String, String> {
    let jar = env::var(JAR_VARIABLE).map_err(|_| missing_jar(lock))?;

    let bytes = fs::read(&jar).map_err(|error| format!("read {JAR_VARIABLE} {jar}: {error}"))?;
    let digest = crate::protocol_lock::digest(&bytes);
    if digest != lock.oracle.jar_sha256 {
        return Err(format!(
            "{jar}\n  has sha256 {digest}\n  but spec/oracle.lock records {}.\n\
             A jar rebuilt from the same commit legitimately differs, because Kafka's build \
             preserves file timestamps. Confirm the jar was built from commit {}, then record \
             the new digest in spec/oracle.lock deliberately. Never substitute a Maven release \
             jar: it implements a different protocol definition than the pinned commit.",
            lock.oracle.jar_sha256, lock.oracle.source_commit,
        ));
    }
    Ok(jar)
}

/// Full classpath for the oracle: the pinned jar plus its Jackson dependencies.
pub(crate) fn classpath(jar: &str) -> Result<String, String> {
    let extra = env::var(CLASSPATH_VARIABLE).map_err(|_| {
        format!(
            "{CLASSPATH_VARIABLE} is not set; the oracle needs the Jackson jars from the \
             `clients` compile classpath to read canonical JSON values."
        )
    })?;
    Ok(format!("{jar}:{extra}"))
}

fn missing_jar(lock: &OracleLock) -> String {
    format!(
        "{JAR_VARIABLE} is not set. Build the oracle jar from Apache Kafka at commit {commit}:\n  \
         git clone https://github.com/apache/kafka && git checkout {commit}\n  \
         ./gradlew {task}\n\
         then point {JAR_VARIABLE} at clients/build/libs/{jar} and {CLASSPATH_VARIABLE} at its \
         Jackson dependencies. Built with JDK {jdk}.",
        commit = lock.oracle.source_commit,
        task = lock.oracle.gradle_task,
        jar = lock.oracle.jar_name,
        jdk = lock.oracle.jdk_version,
    )
}
