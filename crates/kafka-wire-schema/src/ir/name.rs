//! Stable protocol-to-Rust name normalization.

use heck::{ToSnakeCase, ToUpperCamelCase};

/// Protocol and Rust spellings for a message.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MessageName {
    protocol: String,
    rust_type: String,
    rust_module: String,
}

impl MessageName {
    /// Normalizes one upstream message name.
    pub fn new(protocol: impl Into<String>) -> Self {
        let protocol = protocol.into();
        let rust_type = protocol.to_upper_camel_case();
        let rust_module = escape_keyword(protocol.to_snake_case());
        Self {
            protocol,
            rust_type,
            rust_module,
        }
    }

    /// Returns the upstream protocol name.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the Rust type identifier.
    pub fn rust_type(&self) -> &str {
        &self.rust_type
    }

    /// Returns the Rust module identifier.
    pub fn rust_module(&self) -> &str {
        &self.rust_module
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
    rust_field: String,
}

impl FieldName {
    /// Normalizes one upstream field name.
    pub fn new(protocol: impl Into<String>) -> Self {
        let protocol = protocol.into();
        let rust_field = escape_keyword(protocol.to_snake_case());
        Self {
            protocol,
            rust_field,
        }
    }

    /// Returns the upstream protocol name.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the Rust field identifier.
    pub fn rust_field(&self) -> &str {
        &self.rust_field
    }
}

fn escape_keyword(identifier: String) -> String {
    if matches!(
        identifier.as_str(),
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
            | "try"
    ) {
        format!("{identifier}_")
    } else {
        identifier
    }
}
