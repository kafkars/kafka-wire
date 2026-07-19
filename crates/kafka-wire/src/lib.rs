//! Generated, version-aware Kafka wire messages.
//!
//! Callers use this flat facade. Internal module placement and generated file
//! names are not part of the public API.

mod descriptor;
mod generated;
mod message;

pub use descriptor::{MessageDescriptor, MessageDirection};

// EXCEPTION to the repository rule "no wildcard public re-exports" (AGENTS.md).
//
// Rule: no wildcard public re-exports.
// Reason: the flat facade must name every generated DTO, and a message now
//   contributes its nested structs as well as its request and response. Naming
//   them here by hand would mean a handwritten edit per generated API — the one
//   thing generated output is not allowed to require — and the list would pass
//   this file's 180-line facade budget long before the corpus is enabled.
// Scope: one glob, over `generated`, which is itself a curated facade: the
//   generator emits an explicit `pub use` per module, that list is checked in,
//   and every byte of it is hashed in MANIFEST.json. The surface is therefore
//   fully reviewable in the diff, which is what the rule exists to protect.
//   Nothing else in the crate re-exports by glob.
// Removal condition: drop this when the generator emits the crate-root export
//   list directly, or when `generated` becomes the public path.
pub use generated::*;
pub use message::{KafkaMessage, KafkaRequest, KafkaResponse, RequestResponsePair};
