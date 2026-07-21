//! Validated protocol-to-Rust identifier normalization.
//!
//! Every emitted identifier is represented by `RustIdent`; this module does
//! not decide whether two valid identifiers may occupy the same namespace.

use std::fmt;

use heck::{ToSnakeCase, ToUpperCamelCase};
use thiserror::Error;

/// A normalized identifier proven parseable by this workspace's Rust parser.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RustIdent(String);

impl RustIdent {
    /// Normalizes an upstream name as a Rust type identifier.
    pub fn upper_camel(source: &str) -> Result<Self, RustIdentError> {
        Self::from_normalized(source, source.to_upper_camel_case())
    }

    /// Normalizes an upstream name as a Rust field or module identifier.
    pub fn snake(source: &str) -> Result<Self, RustIdentError> {
        Self::from_normalized(source, source.to_snake_case())
    }

    /// Validates an already-normalized emitted spelling.
    pub fn emitted(source: &str) -> Result<Self, RustIdentError> {
        Self::from_normalized(source, source.to_owned())
    }

    fn from_normalized(source: &str, normalized: String) -> Result<Self, RustIdentError> {
        if parses_as_ident(&normalized) {
            return Ok(Self(normalized));
        }

        // Rust keywords, including future-reserved words such as the 2024
        // edition's `gen`, become ordinary identifiers through one stable
        // suffix policy. Malformed names remain malformed after the suffix and
        // are rejected rather than silently invented.
        let escaped = format!("{normalized}_");
        if parses_as_ident(&escaped) {
            return Ok(Self(escaped));
        }

        Err(RustIdentError {
            input: source.to_owned(),
            normalized,
        })
    }

    /// Returns the emitted Rust spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RustIdent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

fn parses_as_ident(candidate: &str) -> bool {
    // Syn's token model is intentionally edition-neutral and therefore still
    // accepts `gen`; this workspace is Rust 2024, where it is reserved.
    if candidate == "gen" {
        return false;
    }
    // Parse in an identifier-only grammar position, not as a bare token.
    // `proc_macro2::Ident` can represent keyword-shaped tokens, whereas an item
    // declaration asks Syn to apply Rust's syntactic identifier rules.
    syn::parse_str::<syn::ItemStruct>(&format!("struct {candidate};"))
        .is_ok_and(|item| item.ident == candidate)
}

/// An upstream name cannot become a valid emitted Rust identifier.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("`{input}` normalizes to invalid Rust identifier `{normalized}`")]
pub struct RustIdentError {
    /// Upstream spelling.
    pub input: String,
    /// Case-normalized spelling rejected by the Rust parser.
    pub normalized: String,
}

/// Protocol and Rust spellings for a message.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MessageName {
    protocol: String,
    rust_type: RustIdent,
    rust_module: RustIdent,
    descriptor_symbol: RustIdent,
}

impl MessageName {
    /// Normalizes one upstream message name.
    ///
    /// # Panics
    ///
    /// Panics when the name cannot form valid Rust. Source adapters should use
    /// [`Self::try_new`] so malformed peer-controlled schema is diagnostic.
    pub fn new(protocol: impl Into<String>) -> Self {
        match Self::try_new(protocol) {
            Ok(name) => name,
            Err(error) => panic!("message name must normalize to valid Rust: {error}"),
        }
    }

    /// Normalizes one upstream message name with checked identifier creation.
    pub fn try_new(protocol: impl Into<String>) -> Result<Self, RustIdentError> {
        let protocol = protocol.into();
        let rust_type = RustIdent::upper_camel(&protocol)?;
        let rust_module = RustIdent::snake(&protocol)?;
        let descriptor_symbol = RustIdent::emitted(&rust_module.as_str().to_ascii_uppercase())?;
        Ok(Self {
            protocol,
            rust_type,
            rust_module,
            descriptor_symbol,
        })
    }

    /// Returns the upstream protocol name.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the Rust type identifier.
    pub fn rust_type(&self) -> &str {
        self.rust_type.as_str()
    }

    /// Returns the Rust module identifier.
    pub fn rust_module(&self) -> &str {
        self.rust_module.as_str()
    }

    /// Returns the upper-snake descriptor symbol identifier.
    pub fn descriptor_symbol(&self) -> &str {
        self.descriptor_symbol.as_str()
    }

    /// Returns the API-pair stem by removing a request or response suffix.
    pub fn api_stem(&self) -> &str {
        self.protocol
            .strip_suffix("Request")
            .or_else(|| self.protocol.strip_suffix("Response"))
            .unwrap_or(self.protocol.as_str())
    }
}

/// Protocol and Rust spellings for a field.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FieldName {
    protocol: String,
    rust_field: RustIdent,
}

impl FieldName {
    /// Normalizes one upstream field name.
    ///
    /// # Panics
    ///
    /// Panics when the name cannot form valid Rust. Source adapters should use
    /// [`Self::try_new`] for untrusted schema input.
    pub fn new(protocol: impl Into<String>) -> Self {
        match Self::try_new(protocol) {
            Ok(name) => name,
            Err(error) => panic!("field name must normalize to valid Rust: {error}"),
        }
    }

    /// Normalizes one upstream field name with checked identifier creation.
    pub fn try_new(protocol: impl Into<String>) -> Result<Self, RustIdentError> {
        let protocol = protocol.into();
        let rust_field = RustIdent::snake(&protocol)?;
        Ok(Self {
            protocol,
            rust_field,
        })
    }

    /// Returns the upstream protocol name.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the Rust field identifier.
    pub fn rust_field(&self) -> &str {
        self.rust_field.as_str()
    }
}
