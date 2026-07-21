//! The Kafka source type language and its normalized form.
//!
//! This file owns the spelling-to-semantics mapping for `"type"` and the rule
//! that separates a primitive from a struct reference. A struct spelling is
//! qualified by its owning message as it is parsed, so no `FieldType` in the IR
//! ever carries an unqualified name.
//!
//! It deliberately does not own the qualification rule itself
//! (`struct_ref.rs`) or *binding*: knowing that a reference names one of the
//! declarations its message actually made is validation's job.

use thiserror::Error;

use super::{MessageName, StructRef};

/// Deepest array nesting this adapter will parse.
///
/// Upstream nests arrays exactly one level (`[]TopicData`). The cap exists so a
/// crafted `[][][]...` spelling cannot drive `parse` into unbounded recursion.
const ARRAY_NESTING_LIMIT: usize = 4;

/// Backend-neutral Kafka field type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    /// Boolean.
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Unsigned 16-bit integer.
    Uint16,
    /// Signed 32-bit integer.
    Int32,
    /// Unsigned 32-bit integer.
    Uint32,
    /// Signed 64-bit integer.
    Int64,
    /// IEEE 754 double, used by the client-quota APIs.
    Float64,
    /// UUID.
    Uuid,
    /// UTF-8 protocol string.
    String,
    /// Opaque bytes.
    Bytes,
    /// Opaque record set.
    Records,
    /// Ordered array.
    Array(Box<Self>),
    /// Reference to a struct declared by the owning message, already resolved.
    ///
    /// Under module-scoped naming the payload carries the upstream spelling it is emitted
    /// under together with the owner that selects its module, so a renderer
    /// never has to re-derive a name and a name collision is a schema
    /// diagnostic rather than unbuildable generated Rust.
    Struct(StructRef),
}

impl FieldType {
    /// Parses a Kafka source type spelling on behalf of `owner`.
    ///
    /// The owning message is a parameter because a struct spelling is only
    /// meaningful relative to it: upstream scopes struct names per message, so
    /// the same word denotes different shapes in different files. Binding the
    /// owner here is what makes an unqualified name unrepresentable in the IR.
    ///
    /// Unknown spellings are an error rather than a struct reference. Treating
    /// every unrecognized word as a struct turns `float64` into a phantom type
    /// and `strng` into a dangling reference, and both survive lowering to fail
    /// much later with a diagnostic that no longer names the typo.
    pub fn parse(source: &str, owner: &MessageName) -> Result<Self, TypeParseError> {
        Self::parse_nested(source, source, owner, 0)
    }

    fn parse_nested(
        source: &str,
        spelling: &str,
        owner: &MessageName,
        depth: usize,
    ) -> Result<Self, TypeParseError> {
        if let Some(element) = source.strip_prefix("[]") {
            if depth >= ARRAY_NESTING_LIMIT {
                return Err(TypeParseError::ArrayNesting {
                    spelling: spelling.to_owned(),
                    limit: ARRAY_NESTING_LIMIT,
                });
            }
            let element = Self::parse_nested(element, spelling, owner, depth + 1)?;
            return Ok(Self::Array(Box::new(element)));
        }

        match source {
            "bool" => Ok(Self::Bool),
            "int8" => Ok(Self::Int8),
            "int16" => Ok(Self::Int16),
            "uint16" => Ok(Self::Uint16),
            "int32" => Ok(Self::Int32),
            "uint32" => Ok(Self::Uint32),
            "int64" => Ok(Self::Int64),
            "float64" => Ok(Self::Float64),
            "uuid" => Ok(Self::Uuid),
            "string" => Ok(Self::String),
            "bytes" => Ok(Self::Bytes),
            "records" => Ok(Self::Records),
            other if is_struct_spelling(other) => StructRef::try_qualify(owner, other)
                .map(Self::Struct)
                .map_err(|error| TypeParseError::Identifier {
                    spelling: other.to_owned(),
                    reason: error.to_string(),
                }),
            other => Err(TypeParseError::Unknown {
                spelling: other.to_owned(),
            }),
        }
    }

    /// Returns whether Kafka permits this shape to be nullable.
    pub const fn permits_null(&self) -> bool {
        matches!(
            self,
            Self::String | Self::Bytes | Self::Records | Self::Array(_) | Self::Struct(_)
        )
    }

    /// Returns the struct this type refers to, through any array level.
    ///
    /// Both `Foo` and `[]Foo` denote a dependency on the struct `Foo`; callers
    /// that resolve or count struct references should not have to re-derive
    /// which of the two shapes they are looking at.
    pub fn struct_reference(&self) -> Option<&StructRef> {
        match self {
            Self::Struct(reference) => Some(reference),
            Self::Array(element) => element.struct_reference(),
            _ => None,
        }
    }
}

/// A `"type"` spelling this adapter cannot interpret.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TypeParseError {
    /// The spelling matched no primitive and could not be a struct reference.
    #[error(
        "unknown type `{spelling}`: primitive type names are lowercase and \
         struct references begin with an uppercase letter"
    )]
    Unknown {
        /// The uninterpretable spelling.
        spelling: String,
    },
    /// The spelling nested arrays deeper than this adapter parses.
    #[error("type `{spelling}` nests arrays deeper than {limit} levels")]
    ArrayNesting {
        /// The offending spelling.
        spelling: String,
        /// The enforced nesting limit.
        limit: usize,
    },
    /// A struct spelling cannot become a valid Rust identifier.
    #[error("type `{spelling}` cannot be emitted as Rust: {reason}")]
    Identifier {
        /// Upstream struct spelling.
        spelling: String,
        /// Identifier diagnostic.
        reason: String,
    },
}

/// Kafka spells primitives in lowercase and struct names in `UpperCamelCase`.
///
/// This is what makes a typo detectable at all: without a shape rule, every
/// misspelled primitive is indistinguishable from a reference to a struct that
/// happens not to exist yet.
fn is_struct_spelling(source: &str) -> bool {
    source
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_uppercase())
        && source
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
}
