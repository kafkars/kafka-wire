//! Deterministic Rust backend for pinned Kafka protocol schemas.
//!
//! The generator behaves like a compiler: verify inputs, build semantic plans,
//! render a complete tree, then compare or replace the checked-in output.

mod config;
mod corpus;
mod error;
mod format;
mod group;
mod identity;
mod lockfile;
mod manifest;
mod output;
mod overrides;
mod pipeline;
mod provenance;
mod render;
mod source;

#[cfg(test)]
mod format_test;

pub use config::{GenerationMode, GeneratorConfig};
pub use corpus::{CorpusOutcome, CorpusRender, render_corpus};
pub use error::GenerationError;
pub use identity::{ProtocolIdentity, protocol_identity};
pub use output::GenerationReport;
pub use pipeline::generate;
