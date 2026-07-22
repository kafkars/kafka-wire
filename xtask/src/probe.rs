//! A throwaway crate that asks whether generated Rust actually compiles.
//!
//! This module owns the scratch tree under `target/`: it renders every pinned
//! schema the backend can emit, wraps the result in the smallest crate that can
//! host it, and reports what did not make it. Compiling generated code is a
//! different question from generating it, and answering the first one long
//! before deciding to check anything in is the point of the whole command.
//!
//! It deliberately owns no judgement about schemas and spawns no process. The
//! renderer's answers come from `kafka-wire-codegen`, and running `cargo test` over
//! what is written here belongs to `commands.rs`, the declared owner of process
//! spawning.

use std::{
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_codegen::CorpusRender;

/// Where the scratch crate is written, relative to the repository root.
const PROBE_ROOT: &str = "target/protocol-probe";

/// One probe run: the crate that was written and what it left out.
#[derive(Debug)]
pub(crate) struct Probe {
    /// Directory holding the generated scratch crate.
    pub(crate) crate_root: PathBuf,
    /// Pinned files the backend rendered.
    pub(crate) rendered: usize,
    /// Pinned files the backend did not render.
    pub(crate) refused: usize,
    /// Generated Rust files written into the scratch crate.
    pub(crate) files: usize,
    /// Refusal reason to the pinned files it accounts for, largest first.
    pub(crate) taxonomy: Vec<(String, Vec<String>)>,
}

/// Renders the whole pinned corpus into a compilable scratch crate.
pub(crate) fn render(workspace: &Path) -> Result<Probe, String> {
    let corpus = kafka_wire_codegen::render_corpus(workspace)
        .map_err(|error| format!("render the pinned corpus: {error}"))?;

    let crate_root = workspace.join(PROBE_ROOT);
    write_crate(&crate_root, workspace, &corpus)?;

    let mut taxonomy = corpus.failure_taxonomy().into_iter().collect::<Vec<_>>();
    taxonomy.sort_by(|left, right| {
        right
            .1
            .len()
            .cmp(&left.1.len())
            .then_with(|| left.0.cmp(&right.0))
    });

    let rendered = corpus.rendered();
    Ok(Probe {
        crate_root,
        rendered,
        refused: corpus.outcomes.len() - rendered,
        files: corpus.files.len(),
        taxonomy,
    })
}

pub(crate) fn write_crate(
    root: &Path,
    workspace: &Path,
    corpus: &CorpusRender,
) -> Result<(), String> {
    // The tree is rebuilt from nothing every run. A leftover module from an
    // earlier probe would keep compiling and quietly inflate the answer.
    if root.exists() {
        fs::remove_dir_all(root).map_err(|error| format!("clear {}: {error}", root.display()))?;
    }

    write(&root.join("Cargo.toml"), &manifest(workspace)?)?;
    write(&root.join("rustfmt.toml"), "")?;
    write(&root.join("src/lib.rs"), LIB)?;
    write(&root.join("src/message.rs"), MESSAGE_SHIM)?;
    write(
        &root.join("src/adversarial_decode.rs"),
        &kafka_wire_codegen::render_adversarial_decode_fixture()
            .map_err(|error| format!("render adversarial decode fixture: {error}"))?,
    )?;
    write(
        &root.join("tests/adversarial_decode.rs"),
        ADVERSARIAL_DECODE_TEST,
    )?;
    for (name, source) in &corpus.files {
        write(&root.join("src/generated").join(name), source)?;
    }
    Ok(())
}

/// The scratch crate's manifest, standing outside the repository workspace.
///
/// `[workspace]` is what makes it its own root: without it cargo refuses to
/// build a package that sits inside another workspace's directory but is not
/// one of its members.
fn manifest(workspace: &Path) -> Result<String, String> {
    let wire = dependency_path(workspace, "crates/kafka-wire-core")?;
    let protocol = dependency_path(workspace, "crates/kafka-wire")?;
    Ok(format!(
        "[workspace]\n\n\
         [package]\n\
         name = \"protocol-probe\"\n\
         version = \"0.0.0\"\n\
         edition = \"2024\"\n\
         publish = false\n\n\
         [dependencies]\n\
         kafka-wire-core = {{ path = \"{wire}\" }}\n\
         kafka-wire = {{ path = \"{protocol}\" }}\n"
    ))
}

/// An absolute path to one workspace crate, as the scratch manifest spells it.
fn dependency_path(workspace: &Path, relative: &str) -> Result<String, String> {
    let path = workspace.join(relative);
    path.to_str()
        .map(|path| path.replace('\\', "/"))
        .ok_or_else(|| format!("{} is not valid UTF-8", path.display()))
}

fn write(path: &Path, source: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(path, source).map_err(|error| format!("write {}: {error}", path.display()))
}

/// The scratch crate root.
///
/// Generated modules say `use crate::{KafkaMessage, ..}`, so the traits are
/// re-exported at the root under the names the emitter writes.
pub(crate) const LIB: &str = "//! Compile probe for the whole pinned protocol corpus.\n\
     //!\n\
     //! Written by `cargo xtask generate-all --check-only`. Nothing here is\n\
     //! checked in. The corpus is typechecked and adversarial emitter fixtures\n\
     //! execute their behavioral assertions.\n\
     \n\
     #![allow(dead_code, unused_imports)]\n\
     \n\
     pub use kafka_wire::{\n\
     \x20   ApiDescriptor, KafkaMessage, KafkaRequest, KafkaResponse, MessageDescriptor,\n\
     \x20   MessageDirection, RequestResponsePair,\n\
     };\n\
     \n\
     pub mod adversarial_decode;\n\
     mod generated;\n\
     mod message;\n";

/// Runtime proof that positional decode locals preserve both sibling values.
pub(crate) const ADVERSARIAL_DECODE_TEST: &str = "//! Generated decode locals preserve field identity at runtime.\n\
     \n\
     use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode};\n\
     use protocol_probe::adversarial_decode::AdversarialDecodeRequest;\n\
     \n\
     #[test]\n\
     fn sibling_names_decode_from_their_own_wire_positions() {\n\
     \x20   let input = Bytes::from_static(&[\n\
     \x20       0, 0, 0, 11,\n\
     \x20       0, 0, 0, 22,\n\
     \x20   ]);\n\
     \x20   let mut decoder = Decoder::new(input, DecodeLimits::default()).unwrap();\n\
     \x20   let decoded = AdversarialDecodeRequest::decode(\n\
     \x20       &mut decoder,\n\
     \x20       ApiVersion::new(0),\n\
     \x20   )\n\
     \x20   .unwrap();\n\
     \n\
     \x20   assert_eq!(decoded.version, 11);\n\
     \x20   assert_eq!(decoded.version_value, 22);\n\
     \x20   decoder.finish().unwrap();\n\
     }\n";

/// The two version gates generated code calls on every encode and decode.
///
/// `kafka-wire` keeps its own versions private, so the probe supplies its
/// own. They answer `Ok` unconditionally on purpose: this crate is compiled and
/// never run, and a stand-in that tried to reproduce the real check would be a
/// second copy of protocol logic that nothing verifies.
pub(crate) const MESSAGE_SHIM: &str = "//! Version-gate stand-ins so generated bodies typecheck.\n\
     //!\n\
     //! This crate is compiled, never run. These functions exist to give\n\
     //! `crate::message::ensure_*_version` a definition with the right shape.\n\
     \n\
     use kafka_wire_core::{ApiVersion, DecodeError, EncodeError};\n\
     \n\
     use crate::KafkaMessage;\n\
     \n\
     pub(crate) fn ensure_decode_version<M: KafkaMessage>(\n\
     \x20   _version: ApiVersion,\n\
     ) -> Result<(), DecodeError> {\n\
     \x20   Ok(())\n\
     }\n\
     \n\
     pub(crate) fn ensure_encode_version<M: KafkaMessage>(\n\
     \x20   _version: ApiVersion,\n\
     ) -> Result<(), EncodeError> {\n\
     \x20   Ok(())\n\
     }\n";
