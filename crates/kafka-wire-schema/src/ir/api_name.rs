//! Checked identity shared by one Kafka request/response API pair.
//!
//! This file owns suffix-free protocol naming and the Rust module and
//! descriptor spellings derived from it. It does not decide whether two pairs
//! may claim the same generated namespace.

use super::{MessageName, RustIdent, RustIdentError};

/// Checked protocol and Rust identity shared by one request/response pair.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiName {
    protocol_stem: String,
    rust_module: RustIdent,
    descriptor_symbol: RustIdent,
}

impl ApiName {
    /// Validates one suffix-free protocol API name and its emitted module name.
    pub fn try_new(protocol_stem: impl Into<String>) -> Result<Self, RustIdentError> {
        let protocol_stem = protocol_stem.into();
        let rust_module = RustIdent::snake(&protocol_stem)?;
        let descriptor_symbol = RustIdent::emitted(&rust_module.as_str().to_ascii_uppercase())?;
        Ok(Self {
            protocol_stem,
            rust_module,
            descriptor_symbol,
        })
    }

    /// Derives and validates the API identity carried by a message name.
    pub fn try_from_message(message: &MessageName) -> Result<Self, RustIdentError> {
        Self::try_new(message.api_stem())
    }

    /// Returns the request/response name without its directional suffix.
    pub fn protocol_stem(&self) -> &str {
        &self.protocol_stem
    }

    /// Returns the emitted module name shared by both directions.
    pub fn rust_module(&self) -> &str {
        self.rust_module.as_str()
    }

    /// Returns the upper-snake stem used by the pair descriptor constant.
    pub fn descriptor_symbol(&self) -> &str {
        self.descriptor_symbol.as_str()
    }
}
