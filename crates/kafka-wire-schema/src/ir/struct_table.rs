//! The per-message struct declaration table.
//!
//! This file owns the one table that unifies the two ways a message declares a
//! struct — a `commonStructs` entry and an inline field body — so every later
//! pass asks one place which structs a message owns, in one order, with one
//! identity each. It deliberately does not own the naming rule
//! (`struct_ref.rs`) or whether the table is well formed
//! (`validate/structs.rs`).
//!
//! The table indexes declaration sites rather than copying them. A struct's
//! fields stay on the `commonStructs` entry or the field that declares them, so
//! there is exactly one place a body can be read from and no way for two copies
//! to drift.

use super::{StructRef, VersionSet};

/// Where a message wrote a struct declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructOrigin {
    /// Declared inline on the field whose element shape it is.
    Inline,
    /// Declared in the message-level `commonStructs` block.
    Common,
}

impl StructOrigin {
    /// Returns the phrase a diagnostic uses to point at this declaration site.
    pub const fn describe(self) -> &'static str {
        match self {
            Self::Inline => "inline on a field",
            Self::Common => "in commonStructs",
        }
    }
}

/// One struct a message declares.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructDeclaration {
    /// Owner-qualified identity.
    pub name: StructRef,
    /// Versions in which this declaration applies.
    ///
    /// A `commonStructs` entry states its own. An inline body takes the
    /// presence window of the field that carries it, because it exists in
    /// exactly the versions where that field does.
    pub versions: VersionSet,
    /// Where the message wrote it.
    pub origin: StructOrigin,
    /// Structs this declaration's own members refer to.
    ///
    /// Immediate members only. A nested inline body is a separate declaration
    /// with its own entry, so the reference graph carries one edge per written
    /// reference and a cycle search over it terminates.
    pub references: Vec<StructRef>,
}

/// Every struct one message declares, in protocol declaration order.
///
/// `Default` is the empty table — a message that declares no struct at all,
/// which most of the corpus is. It exists so a caller assembling a `Message`
/// by hand does not have to reach for the crate-private constructor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StructTable {
    declarations: Vec<StructDeclaration>,
}

impl StructTable {
    /// Builds a table from declarations already collected in source order.
    pub(crate) const fn new(declarations: Vec<StructDeclaration>) -> Self {
        Self { declarations }
    }

    /// Returns every declaration in protocol declaration order.
    ///
    /// The order is each `commonStructs` entry in file order followed by the
    /// inline bodies nested inside it, then the inline bodies of the root field
    /// tree in written field order. the earlier flat naming rule fixes generated item order to
    /// protocol declaration order, so this is that order rather than an
    /// incidental one, and a renderer may emit it directly.
    pub fn declarations(&self) -> &[StructDeclaration] {
        &self.declarations
    }

    /// Resolves one upstream spelling against this message's declarations.
    ///
    /// Returns the first declaration of that name. A well-formed message has
    /// only one: `validate/structs.rs` reports a second as
    /// `KAFKA_SCHEMA_DUPLICATE_STRUCT` rather than letting this quietly choose.
    ///
    /// The scan is linear because a message declares a handful of structs — the
    /// pinned corpus averages 1.5 and peaks in the single digits — and keeping
    /// the declarations in one ordered vector is what preserves the emission
    /// order above.
    pub fn resolve(&self, declared: &str) -> Option<&StructDeclaration> {
        self.declarations
            .iter()
            .find(|declaration| declaration.name.declared() == declared)
    }
}
