//! The broker-authored byte-vector corpus, and the two directions of owning it.
//!
//! This module owns the `cargo xtask vectors` command surface and nothing else.
//! It routes to `refresh`, which asks Apache Kafka's own generated writer what
//! the bytes are and needs a Java toolchain, or to `check`, which re-reads the
//! checked-in corpus in pure Rust with no Java, no network, and no container.
//! The two are kept in separate files because they have different capabilities:
//! only `oracle` may spawn a process, and CI touches only the `check` path.
//!
//! It deliberately owns neither file format nor judgement; `corpus` owns the
//! formats, `oracle_lock` owns which jar is legitimate, and the conformance
//! crate owns whether this repository actually agrees with the bytes.

mod check;
mod corpus;
mod oracle;
mod oracle_lock;
mod refresh;

use crate::cli::VectorsMode;

pub(crate) fn run(mode: VectorsMode) -> Result<(), String> {
    let workspace = crate::workspace::root();
    match mode {
        VectorsMode::Refresh => refresh::refresh(&workspace),
        VectorsMode::Check => check::check(&workspace),
    }
}
