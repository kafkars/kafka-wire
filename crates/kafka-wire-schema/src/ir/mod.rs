//! Backend-neutral protocol semantics.

mod field;
mod message;
mod name;
mod value;
mod version;

pub use field::{Field, FieldType};
pub use message::{Message, MessageKind};
pub use name::{FieldName, MessageName};
pub use value::DefaultValue;
pub use version::{VersionParseError, VersionRange, VersionSet};
