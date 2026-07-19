//! Source-file loading and position-preserving JSONC parsing.

mod file;
mod jsonc;

pub use file::SourceFile;
pub use jsonc::{SourceError, parse_jsonc};
