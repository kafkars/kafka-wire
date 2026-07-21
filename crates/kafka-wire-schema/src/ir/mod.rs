//! Backend-neutral protocol semantics.

mod common_struct;
mod entity;
mod field;
mod field_type;
mod message;
mod name;
mod struct_ref;
mod struct_table;
mod value;
mod version;

pub use common_struct::CommonStruct;
pub use entity::{EntityType, EntityTypeParseError};
pub use field::Field;
pub use field_type::{FieldType, TypeParseError};
pub use message::{Message, MessageKind};
pub use name::{FieldName, MessageName, RustIdent, RustIdentError};
pub use struct_ref::{Qualification, StructRef};
pub use struct_table::{StructDeclaration, StructOrigin, StructTable};
pub use value::{DefaultValue, FloatDefault};
pub use version::{VersionParseError, VersionRange, VersionSet};
