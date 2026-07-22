//! Deterministic Rust backend for pinned Kafka protocol schemas.
//!
//! The generator behaves like a compiler: verify inputs, build semantic plans,
//! render a complete tree, then compare or replace the checked-in output.

mod config;
mod corpus;
mod corpus_output;
mod corpus_validation;
mod error;
mod format;
mod group;
mod identity;
mod lock_path;
mod lockfile;
mod manifest;
mod namespace;
mod output;
mod output_ownership;
mod output_staging;
mod overrides;
mod pair_error;
mod pipeline;
mod provenance;
mod render;
mod source;

#[cfg(test)]
mod format_test;
#[cfg(test)]
mod group_test;
#[cfg(test)]
mod namespace_test;
#[cfg(test)]
mod output_test;
#[cfg(test)]
mod overrides_test;
#[cfg(test)]
mod pipeline_test;

pub use config::{GenerationMode, GeneratorConfig};
pub use corpus::{CorpusOutcome, CorpusRender, render_corpus};
pub use error::GenerationError;
pub use identity::{ProtocolIdentity, protocol_identity};
pub use lock_path::{PortableFilename, PortablePathError, RepoRelativePath};
pub use lockfile::{
    GeneratorLock, KafkaLock, LockedFile, ProtocolLock, SUPPORTED_IR_VERSION, SourceStatus,
};
pub use output::GenerationReport;
pub use pair_error::PairError;
pub use pipeline::generate;
