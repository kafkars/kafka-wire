//! Module-scoped identity for one nested struct.
//!
//! This file owns the module-scoped naming rule's naming rule: a nested struct keeps upstream's own
//! spelling, and the scope that disambiguates it is the module its owning
//! message is emitted into. It deliberately does not own whether a reference
//! binds to a declaration (`validate/structs.rs`), the per-message declaration
//! table (`struct_table.rs`), or uniqueness within a module
//! (`validate/uniqueness.rs`).

use super::{MessageName, RustIdent, RustIdentError};

/// Which scope disambiguates a struct name.
///
/// the earlier flat naming rule had three arms here, all of which built a flat, globally unique
/// identifier out of the owner and the declared name. the module-scoped naming rule replaced the
/// whole construction with a Rust module, so one arm is left. The enum stays
/// rather than being deleted because the owner is still *recorded* — it decides
/// which module the struct lands in — and a regression that reintroduced
/// qualification into the name would otherwise be invisible at this layer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Qualification {
    /// Upstream's own spelling, scoped by the owning message's module.
    ///
    /// Two messages may declare one name; they land in different modules and do
    /// not collide. What may not happen is one message declaring a name twice,
    /// counting its own message type — that is the invariant
    /// `validate/uniqueness.rs` asserts, on exactly this scope.
    ModuleScoped,
}

/// A struct reference bound to the message that owns its declaration.
///
/// Kafka scopes a struct name to the message that declares it, and the module-scoped naming rule
/// gives Rust the same scope: one module per message, holding the message type
/// and every struct it declares. A bare upstream spelling is unambiguous there
/// — `PartitionData` names 17 distinct shapes across 14 API keys, and each lands
/// in its own module. Lowering still binds the owner, because the owner is what
/// names that module and what the collision check groups by.
///
/// Both the declared and the emitted spelling are kept. They agree today, and
/// the pair is kept because they are answers to different questions: `declared`
/// is the key a reference inside the same message resolves by, `rust_type` is
/// what gets emitted.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StructRef {
    declared: String,
    owner: String,
    protocol: String,
    rust_type: RustIdent,
    qualification: Qualification,
}

impl StructRef {
    /// Binds one upstream struct spelling to the message that declares it.
    ///
    /// The name is upstream's, unconditionally: it does not depend on whether a
    /// collision was detected anywhere, and it never compares field shapes. Two
    /// same-named structs can have identical shallow field lists and still
    /// denote different types once their children resolve, so a rule that never
    /// compares shapes cannot get that wrong.
    ///
    /// Neither depth nor the owner reaches the name. The owner is recorded
    /// because it selects the module, and the module is the scope; a struct
    /// three levels down is spelled exactly as upstream spelled it, which bounds
    /// every generated name at `len(struct)` however deep upstream nests.
    pub fn qualify(owner: &MessageName, declared: impl Into<String>) -> Self {
        match Self::try_qualify(owner, declared) {
            Ok(reference) => reference,
            Err(error) => panic!("struct name must normalize to valid Rust: {error}"),
        }
    }

    /// Binds an upstream struct spelling after validating its emitted name.
    pub fn try_qualify(
        owner: &MessageName,
        declared: impl Into<String>,
    ) -> Result<Self, RustIdentError> {
        let declared = declared.into();
        let owner = owner.protocol().to_owned();

        let (protocol, qualification) = (declared.clone(), Qualification::ModuleScoped);
        let rust_type = RustIdent::upper_camel(&protocol)?;

        Ok(Self {
            declared,
            owner,
            protocol,
            rust_type,
            qualification,
        })
    }

    /// Returns the bare upstream spelling, as written in the schema.
    ///
    /// This is the key a reference resolves by, because upstream refers to a
    /// struct by the name it declared, never by the qualified one.
    pub fn declared(&self) -> &str {
        &self.declared
    }

    /// Returns the protocol name of the message that owns the declaration.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the protocol name this struct is emitted under.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the Rust type identifier this struct is emitted as.
    pub fn rust_type(&self) -> &str {
        self.rust_type.as_str()
    }

    /// Returns which scope disambiguates this name.
    pub const fn qualification(&self) -> Qualification {
        self.qualification
    }
}
