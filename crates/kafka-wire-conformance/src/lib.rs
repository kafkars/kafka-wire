//! Conformance of the generated Rust wire implementation against broker-authored bytes.
//!
//! This crate loads the byte vectors under `spec/vectors/` — produced by Apache
//! Kafka's own generated writer at the pinned commit, never by this repository —
//! and gives the tests under `tests/` the vocabulary to hold `kafka-wire` to
//! them. The corpus is the adjudicating authority: where this implementation and
//! a vector disagree, the vector is right.
//!
//! It deliberately owns no product behavior and appears in no consumer's
//! dependency graph. It is excluded from the workspace default members, nothing
//! depends on it, and `architecture.toml` records that it may depend on the
//! wire crates while nothing may depend on it.

mod corpus;
mod json_value;
mod subject;

pub use corpus::{Direction, TaggedField, Vector, from_hex, load, to_hex, workspace_root};
pub use subject::{Facts, Subject, facts, is_flexible};
