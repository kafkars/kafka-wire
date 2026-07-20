//! How many modules have to spell a wire type in full, and which.
//!
//! Scenario: walk the pinned corpus and ask, of every message, whether it
//! declares a struct under a name its own module imports. the module-scoped naming rule calls this
//! the third scope, and the one no measurement of the schemas alone can find,
//! because it depends on the emitter's import list as much as on upstream's
//! spellings.
//!
//! The count is asserted, not printed. Adding a name to `IMPORTABLE` can create
//! a clash that no schema changed, and the failure mode is `error[E0255]` in one
//! generated file out of 194 — this is the cheaper place to hear about it.

use std::path::{Path, PathBuf};

use kafka_wire_schema::Message;

use super::imports::{IMPORTABLE, declares};
use crate::{lockfile::ProtocolLock, source::load_every_source};

/// Every (module, name) pair the pinned corpus forces to be written in full.
///
/// One. `ApiVersionsResponse` declares a struct upstream spells `ApiVersion`,
/// which is exactly the collision the module-scoped naming rule reports finding by attempting the
/// rename. Owner qualification hid it — the struct was
/// `ApiVersionsResponseApiVersion` — so nothing before this decision could have
/// surfaced it.
const QUALIFIED: &[(&str, &str)] = &[("api_versions_response", "ApiVersion")];

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn corpus() -> Vec<Message> {
    let workspace = repository_root();
    let lock = ProtocolLock::read(&workspace.join("spec/protocol.lock"))
        .unwrap_or_else(|error| panic!("read the repository lockfile: {error}"));
    load_every_source(&workspace, &lock)
        .unwrap_or_else(|error| panic!("load the pinned corpus: {error}"))
        .sources
        .into_iter()
        .map(|source| source.message)
        .collect()
}

#[test]
fn one_module_in_the_corpus_must_spell_a_wire_type_in_full() {
    let mut clashes = Vec::new();
    for message in corpus() {
        for (name, _) in IMPORTABLE {
            if declares(&message, name) {
                clashes.push((message.name.rust_module().to_owned(), (*name).to_owned()));
            }
        }
    }
    clashes.sort_unstable();

    let expected = QUALIFIED
        .iter()
        .map(|(module, name)| ((*module).to_owned(), (*name).to_owned()))
        .collect::<Vec<_>>();

    assert_eq!(
        clashes, expected,
        "the set of modules that cannot import a wire type moved; if a schema \
         changed, re-measure and update the module-scoped naming rule, and if `IMPORTABLE` changed, \
         check that the new name is actually emitted through `spell`",
    );
}

#[test]
fn a_module_that_declares_no_clashing_struct_writes_every_name_bare() {
    // The negative half. `spell` returning a path unconditionally would satisfy
    // the count above and make every generated signature longer, which is the
    // resolution the module-scoped naming rule considered and rejected.
    let corpus = corpus();
    let ordinary = corpus
        .iter()
        .find(|message| message.name.rust_module() == "produce_request")
        .unwrap_or_else(|| panic!("the corpus must contain ProduceRequest"));

    for (name, _) in IMPORTABLE {
        assert_eq!(
            super::imports::spell(ordinary, name),
            *name,
            "`produce_request` declares no struct named `{name}` and must import it",
        );
    }
}

#[test]
fn the_one_clashing_module_qualifies_that_name_and_only_that_name() {
    let corpus = corpus();
    let api_versions = corpus
        .iter()
        .find(|message| message.name.rust_module() == "api_versions_response")
        .unwrap_or_else(|| panic!("the corpus must contain ApiVersionsResponse"));

    assert_eq!(
        super::imports::spell(api_versions, "ApiVersion"),
        "kafka_wire_core::ApiVersion",
    );
    // Every other name it imports keeps the bare spelling, which is what makes
    // this resolution worth the extra machinery over qualifying everything.
    assert_eq!(super::imports::spell(api_versions, "Decoder"), "Decoder");
    assert_eq!(
        super::imports::spell(api_versions, "KafkaMessage"),
        "KafkaMessage",
    );
}

#[test]
fn a_crate_local_name_would_be_qualified_against_the_crate_not_the_wire() {
    // No pinned schema declares one, so the arm is exercised here rather than
    // left to be discovered wrong the first time upstream adds a struct named
    // `KafkaMessage`. A `kafka_wire_core::KafkaMessage` path would not compile.
    let mut message = corpus()
        .into_iter()
        .find(|message| message.name.rust_module() == "produce_request")
        .unwrap_or_else(|| panic!("the corpus must contain ProduceRequest"));
    message.name = kafka_wire_schema::MessageName::new("KafkaRequest");

    assert_eq!(
        super::imports::spell(&message, "KafkaRequest"),
        "crate::KafkaRequest",
    );
}
