//! What a message module imports, and how it spells a name it cannot import.
//!
//! the module-scoped naming rule puts each message and the structs it declares into one module under
//! upstream's own spellings. That opens a scope no measurement of the schemas
//! can find, because it depends on what the *emitter* imports rather than on
//! what upstream declares: a declared struct can collide with a name its own
//! module imports. `ApiVersionsResponse` declares a struct upstream spells
//! `ApiVersion`, and every module imports `kafka_wire_core::ApiVersion` for its
//! codec signatures — `error[E0255]`.
//!
//! the module-scoped naming rule names two resolutions and this file implements the second, the one
//! it calls the reference-grade answer: import every name that does not clash,
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

/// Every name a message module may import, and where it comes from.
///
/// This is the emitter's import list stated once, so that the clash check and
/// the import rendering cannot disagree. A name emitted as a literal but absent
/// here would be spelled bare in a module that declares a struct of that name,
/// which is the failure this table exists to prevent.
pub(super) const IMPORTABLE: &[(&str, &str)] = &[
    ("ApiKey", "kafka_wire_core"),
    ("ApiVersion", "kafka_wire_core"),
    ("Bytes", "kafka_wire_core"),
    ("BytesMut", "kafka_wire_core"),
    ("DecodeError", "kafka_wire_core"),
    ("Decoder", "kafka_wire_core"),
    ("EncodeError", "kafka_wire_core"),
    ("EncodeTarget", "kafka_wire_core"),
    ("Encoder", "kafka_wire_core"),
    ("KafkaDecode", "kafka_wire_core"),
    ("KafkaEncode", "kafka_wire_core"),
    ("KnownTags", "kafka_wire_core"),
    ("StrBytes", "kafka_wire_core"),
    ("TagOutcome", "kafka_wire_core"),
    ("TaggedFields", "kafka_wire_core"),
    ("Uuid", "kafka_wire_core"),
    ("VersionRange", "kafka_wire_core"),
    ("encode_into_with", "kafka_wire_core"),
    ("encoded_len_with", "kafka_wire_core"),
    ("KafkaMessage", "crate"),
    ("KafkaRequest", "crate"),
    ("KafkaResponse", "crate"),
    ("RequestResponsePair", "crate"),
];

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
pub(crate) fn spell(message: &Message, name: &str) -> String {
    if declares(message, name) {
        return format!("{}::{name}", origin(name));
    }
    name.to_owned()
}

/// The crate an importable name comes from.
///
/// Panic-free by construction is not available here — the name is a literal at
/// every call site — so an unlisted name falls back to `kafka_wire_core`, which is
/// where all but four of them live. A name that reached this fallback wrongly
/// would fail to compile in the one module that declares it, which is the same
/// signal the table exists to raise, at the same place.
fn origin(name: &str) -> &'static str {
    IMPORTABLE
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map_or("kafka_wire_core", |(_, path)| *path)
}

/// Filters an import list down to the names this module can actually bind.
///
/// A name it declares is dropped rather than renamed. `use kafka_wire_core::X as _`
/// would not help — the uses are by name — and an alias would put a second
/// spelling of one type into generated source, which is worse to read than the
/// full path at the two or three places it appears.
pub(crate) fn importable<'a>(message: &Message, names: &[&'a str]) -> Vec<&'a str> {
    names
        .iter()
        .copied()
        .filter(|name| !declares(message, name))
        .collect()
}
