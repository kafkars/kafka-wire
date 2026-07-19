//! Deterministic Rust backend for pinned Kafka protocol schemas.
//!
//! The generator behaves like a compiler: verify inputs, build semantic plans,
//! render a complete tree, then compare or replace the checked-in output.

mod config;
mod error;
mod format;
mod group;
mod identity;
mod lockfile;
mod manifest;
mod output;
mod pipeline;
mod provenance;
mod render;
mod source;

pub use config::{GenerationMode, GeneratorConfig};
pub use error::GenerationError;
pub use identity::{ProtocolIdentity, protocol_identity};
pub use output::GenerationReport;
pub use pipeline::generate;
