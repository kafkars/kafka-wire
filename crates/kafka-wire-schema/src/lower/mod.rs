//! Source-language interpretation into normalized protocol semantics.

mod default;
mod error;
mod field;
mod message;
mod structs;

pub use error::LowerError;
pub use message::lower_message;
pub(crate) use structs::collect_struct_table;
