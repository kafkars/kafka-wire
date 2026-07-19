//! Typed protocol defaults after source lowering.

/// Default value used when a field is absent from a wire version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultValue {
    /// Null default.
    Null,
    /// Boolean default.
    Bool(bool),
    /// Signed integer default.
    Integer(i64),
    /// String default.
    String(String),
    /// Empty array or byte sequence.
    Empty,
}
