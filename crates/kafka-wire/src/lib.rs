//! Generated, version-aware Kafka wire messages.
//!
//! Callers use this flat facade. Internal module placement and generated file
//! names are not part of the public API.

mod descriptor;
mod frame;
mod generated;
mod message;

pub use descriptor::{MessageDescriptor, MessageDirection};
pub use frame::{encode_request, response_header_version_for};

// EXCEPTION to the repository rule "no wildcard public re-exports" (AGENTS.md).
//
// Rule: no wildcard public re-exports.
// Reason: the flat facade must name everything a caller reaches for, and each
//   message contributes three names — its type, its descriptor, and the module
//   the module-scoped naming rule scopes its nested structs to, which is the only path those structs
//   are reachable by. That is ~1200 names over 193 API keys. Naming them here by
//   hand would mean a handwritten edit per generated API — the one thing
//   generated output is not allowed to require — and the list would pass this
//   file's 180-line facade budget many times over.
// Scope: one glob, over `generated`, which is itself a curated facade: the
//   generator emits an explicit `pub use` per module, that list is checked in,
//   and every byte of it is hashed in MANIFEST.json. The surface is therefore
//   fully reviewable in the diff, which is what the rule exists to protect.
//   Nothing else in the crate re-exports by glob.
// Removal condition: drop this when the generator emits the crate-root export
//   list directly, or when `generated` becomes the public path.
pub use generated::*;
pub use message::{KafkaMessage, KafkaRequest, KafkaResponse, RequestResponsePair};
