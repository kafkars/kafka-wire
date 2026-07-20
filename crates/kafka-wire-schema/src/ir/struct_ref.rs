//! Owner-qualified identity for one nested struct.
//!
//! This file owns the earlier flat naming rule's naming rule: the qualified protocol and Rust
//! spellings a nested struct takes from the message that declares it, and which
//! of the rule's two arms produced them. It deliberately does not own whether a
//! reference binds to a declaration (`validate/structs.rs`), the per-message
//! declaration table (`struct_table.rs`), or uniqueness across messages
//! (`validate/uniqueness.rs`).

use heck::ToUpperCamelCase;

use super::MessageName;

/// Which arm of the earlier flat naming rule's qualification rule produced a name.
///
/// The rule has two arms and both are exercised by the pinned corpus, so this
/// records which one applied rather than leaving the distinction implicit. A
/// regression that collapsed the rule to a pure prefix would still produce
/// names, just wrong ones; it is visible here.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Qualification {
    /// The upstream spelling already begins with its owning message's name.
    ///
    /// Forty of the pinned corpus's struct declarations hand-qualify
    /// themselves. Re-prefixing those yields names such as
    /// `DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition`;
    /// eliding the repeat bounds the corpus at 74 characters rather than 75.
    AlreadyQualified,
    /// The owning message's protocol name was prefixed to the upstream spelling.
    OwnerPrefixed,
    /// The declared name repeated the API's stem, which the owner already
    /// carries, so the stem is spelled once.
    ///
    /// Upstream writes `DescribeUserScramCredentialsResult` inside
    /// `DescribeUserScramCredentialsResponse`. Prefixing the whole owner would
    /// say `DescribeUserScramCredentials` twice and produce a seventy-character
    /// type, which is a name no one reads — the qualification would be doing its
    /// job and defeating the purpose of having a name at all.
    StemDeduplicated,
}

/// A struct reference bound to the message that owns its declaration.
///
/// Kafka scopes a struct name to the message that declares it; Rust has no such
/// scope, and `kafka-wire-codegen` renders both directions of an API key into one
/// module. A bare upstream spelling is therefore ambiguous by construction —
/// `PartitionData` names 17 distinct shapes across 14 API keys, and 8 API keys
/// declare a differently-shaped struct of one name in each direction. Lowering
/// replaces the bare spelling with this, so that no renderer ever re-derives a
/// name and the collision check can be a schema diagnostic.
///
/// Both the declared and the qualified spelling are kept. The declared one is
/// what upstream wrote and the only key a reference inside the same message can
/// be resolved by; the qualified one is what gets emitted.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StructRef {
    declared: String,
    owner: String,
    protocol: String,
    rust_type: String,
    qualification: Qualification,
}

impl StructRef {
    /// Qualifies one upstream struct spelling by the message that declares it.
    ///
    /// Qualification is unconditional: it does not depend on whether a collision
    /// was detected anywhere, and it never compares field shapes. Two same-named
    /// structs can have identical shallow field lists and still denote different
    /// types once their children resolve, so a rule that never compares shapes
    /// cannot get that wrong.
    ///
    /// Depth does not participate. A struct three levels down is qualified by
    /// its message and nothing else, which bounds every generated name at
    /// `len(message) + len(struct)` however deep upstream nests.
    pub fn qualify(owner: &MessageName, declared: impl Into<String>) -> Self {
        let declared = declared.into();
        let stem = owner.api_stem().to_owned();
        let owner = owner.protocol().to_owned();

        let (protocol, qualification) = if begins_with_owner(&declared, &owner) {
            (declared.clone(), Qualification::AlreadyQualified)
        } else if let Some(rest) = trailing_after_stem(&declared, &stem) {
            (format!("{owner}{rest}"), Qualification::StemDeduplicated)
        } else {
            (format!("{owner}{declared}"), Qualification::OwnerPrefixed)
        };
        let rust_type = protocol.to_upper_camel_case();

        Self {
            declared,
            owner,
            protocol,
            rust_type,
            qualification,
        }
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

    /// Returns the qualified protocol name.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the Rust type identifier this struct is emitted as.
    pub fn rust_type(&self) -> &str {
        &self.rust_type
    }

    /// Returns which arm of the qualification rule produced this name.
    pub const fn qualification(&self) -> Qualification {
        self.qualification
    }
}

/// Reports whether `declared` already carries `owner` as a leading name segment.
///
/// The test is a camel-case boundary rather than a raw text prefix. A struct
/// spelled `FooRequestly` starts with `FooRequest` as bytes while naming
/// something unrelated, and eliding there would hand two different structs the
/// same qualified name — the exact failure this rule exists to prevent. An
/// exact match is not a segment either: a struct named like its own message
/// must still be prefixed, or it would collide with the message type in the
/// module the two share.
///
/// Measured over the pinned corpus, no declaration distinguishes this reading
/// from a raw prefix: all 40 elisions clear the boundary. The stricter test is
/// kept because the corpus changes upstream and the failure mode is silent.
/// The part of `declared` that follows the API stem it already repeats.
///
/// `None` unless the declared name opens with the stem and continues with a new
/// word, so this never fires on a coincidental prefix or splits an identifier
/// mid-word. The result stays unique for the same reason full qualification
/// does: the emitted name is still `owner` followed by something derived only
/// from `declared`, and two different declarations under one owner cannot
/// reduce to the same remainder.
fn trailing_after_stem<'a>(declared: &'a str, stem: &str) -> Option<&'a str> {
    if stem.is_empty() {
        return None;
    }
    declared
        .strip_prefix(stem)
        .filter(|rest| rest.starts_with(|first: char| first.is_ascii_uppercase()))
}

fn begins_with_owner(declared: &str, owner: &str) -> bool {
    declared
        .strip_prefix(owner)
        .is_some_and(|rest| rest.starts_with(|first: char| first.is_ascii_uppercase()))
}
