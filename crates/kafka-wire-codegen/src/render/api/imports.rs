//! What a message module imports, and how it spells a name it cannot import.
//!
//! Each message and its declared structs share one module under upstream's own
//! spellings. That opens a scope no measurement of the schemas
//! can find, because it depends on what the *emitter* imports rather than on
//! what upstream declares: a declared struct can collide with a name its own
//! module imports. `ApiVersionsResponse` declares a struct upstream spells
//! `ApiVersion`, and every module imports `kafka_wire_core::ApiVersion` for its
//! codec signatures — `error[E0255]`.
//!
//! Two resolutions are possible; this file implements the readable one: import
//! every name that does not clash,
//! and spell the clashing one in full at its point of use. The alternative —
//! qualifying every wire type everywhere — is immune by construction and makes
//! every generated signature longer, in exactly the code this decision exists
//! to make readable.
//!
//! **Measured over the pinned corpus, one module is affected.**
//! `api_versions_response` spells `kafka_wire_core::ApiVersion`; the other 400 write
//! the bare name. `imports_test.rs` asserts that count rather than trusting it,
//! because it is a property of `IMPORTABLE` below as much as of upstream's
//! spellings — adding a name to the emitter's import list can create a clash no
//! schema changed.

use kafka_wire_schema::Message;

pub(crate) use super::symbol::ExternalSymbol;

pub(super) const IMPORTABLE: &[ExternalSymbol] = ExternalSymbol::IMPORTABLE;

/// Whether this message's module declares an item of this name.
///
/// The message type counts: it is emitted into the module beside the structs,
/// so a struct spelled like its own message is the same clash. `kafka-wire-schema`
/// rejects that pair outright, which is why this only has to consider the
/// emitter's own names.
pub(crate) fn declares(message: &Message, name: &str) -> bool {
    message.name.rust_type() == name
        || message
            .structs
            .declarations()
            .iter()
            .any(|declaration| declaration.name.rust_type() == name)
}

/// Spells one importable type as `message`'s module must write it.
///
/// The bare name almost always, because almost no module declares a struct that
/// clashes with one. A module that does gets the full path for that one name and
/// keeps the bare spelling for every other.
pub(crate) fn spell(message: &Message, symbol: ExternalSymbol) -> String {
    if let Some(absolute) = symbol.absolute() {
        return absolute.to_owned();
    }
    let name = symbol.name();
    let origin = symbol
        .origin()
        .unwrap_or_else(|| unreachable!("absolute symbols return before import rendering"));
    if declares(message, name) {
        return format!("{origin}::{name}");
    }
    name.to_owned()
}

/// Filters an import list down to the names this module can actually bind.
///
/// A name it declares is dropped rather than renamed. `use kafka_wire_core::X as _`
/// would not help — the uses are by name — and an alias would put a second
/// spelling of one type into generated source, which is worse to read than the
/// full path at the two or three places it appears.
pub(crate) fn importable(message: &Message, names: &[ExternalSymbol]) -> Vec<&'static str> {
    debug_assert!(
        names.iter().all(|symbol| IMPORTABLE.contains(symbol)),
        "only the exhaustive typed import vocabulary may reach a generated use declaration"
    );
    names
        .iter()
        .copied()
        .filter(|symbol| symbol.origin().is_some())
        .filter(|symbol| !declares(message, symbol.name()))
        .map(ExternalSymbol::name)
        .collect()
}
