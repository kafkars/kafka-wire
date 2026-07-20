//! Struct declarations a message makes outside its field tree.
//!
//! This file owns the `commonStructs` record: the declaration site for a struct
//! upstream hoisted to message level. It deliberately does not own the naming
//! rule (`struct_ref.rs`) or the table that indexes every declaration a message
//! makes (`struct_table.rs`); this type is one of the two sites that table
//! points at, the other being an inline field body.

use super::{Field, StructRef, VersionSet};

/// One struct declared at message level and referred to by name.
///
/// Upstream hoists a struct here when more than one field in the same message
/// has its shape — `DescribeQuorumResponse` declares `ReplicaState` once and
/// both `CurrentVoters` and `Observers` refer to it. The block is scoped to a
/// single message and direction, so it is not shared between a request and its
/// response even when the two declare identical shapes, and under module-scoped naming it
/// lands in the same module an inline declaration lands in, under the same
/// spelling: `DescribeQuorumResponse` declares `ReplicaState`, and that is the
/// name emitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommonStruct {
    /// Module-scoped identity, keyed within the message by its declared name.
    pub name: StructRef,
    /// Versions in which this declaration applies.
    pub versions: VersionSet,
    /// Ordered struct fields.
    pub fields: Vec<Field>,
}
