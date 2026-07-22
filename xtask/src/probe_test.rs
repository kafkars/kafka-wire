//! The probe crate can host whatever the emitter currently emits.
//!
//! Scenario: write the scratch crate's scaffolding, then read the checked-in
//! generated tree and assert the scaffolding supplies every name it reaches
//! for. The probe answers "does generated Rust compile", and a probe whose
//! host crate has fallen behind the emitter answers that question about code
//! nobody writes any more — while still exiting zero.
//!
//! The failure this closes is specific: the day a renderer adds an import from
//! `crate::`, every probe run would fail to compile for a reason that has
//! nothing to do with the schemas, and the natural reading of that failure is
//! "the generator is broken".

use std::{
    fs,
    path::{Path, PathBuf},
};

use kafka_wire_codegen::CorpusRender;

use crate::{
    probe::{LIB, MESSAGE_SHIM, write_crate},
    probe_fixtures::{ADVERSARIAL_DECODE_TEST, ADVERSARIAL_DEFAULTS_TEST},
};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// The generated Rust checked into `kafka-wire`, concatenated.
///
/// This is the only sample of real emitter output available without running a
/// generation, and it is exactly the shape the probe crate must be able to host.
fn checked_in_generated() -> String {
    let root = repository_root().join("crates/kafka-wire/src/generated");
    let entries =
        fs::read_dir(&root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()));

    let mut sources = String::new();
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry: {error}"))
            .path();
        if path.extension().is_some_and(|kind| kind == "rs") {
            sources.push_str(
                &fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
            );
            sources.push('\n');
        }
    }
    assert!(
        !sources.is_empty(),
        "no generated Rust was found, so this proof covers nothing"
    );
    sources
}

fn scratch(name: &str) -> PathBuf {
    repository_root()
        .join("target")
        .join("probe-scaffold")
        .join(name)
}

#[test]
fn the_scaffolding_stands_outside_the_repository_workspace() {
    let root = scratch("standalone");
    write_crate(&root, &repository_root(), &CorpusRender::default())
        .unwrap_or_else(|error| panic!("write the probe crate: {error}"));

    let manifest = fs::read_to_string(root.join("Cargo.toml"))
        .unwrap_or_else(|error| panic!("read the probe manifest: {error}"));

    assert!(
        manifest.starts_with("[workspace]"),
        "without its own `[workspace]` table, cargo refuses to build a package \
         sitting inside another workspace's directory: {manifest}"
    );
    for crate_name in ["kafka-wire-core", "kafka-wire"] {
        assert!(
            manifest.contains(crate_name),
            "the probe manifest does not depend on {crate_name}: {manifest}"
        );
    }
    assert!(
        !manifest.contains("kafka-wire-codegen"),
        "the probe must not put the generator in the graph it compiles: {manifest}"
    );
    assert!(
        root.join("src/lib.rs").is_file() && root.join("src/message.rs").is_file(),
        "the probe crate is missing its root or its version-gate shim"
    );
    assert!(
        root.join("src/adversarial_decode.rs").is_file()
            && root.join("tests/adversarial_decode.rs").is_file()
            && root.join("src/adversarial_defaults.rs").is_file()
            && root.join("tests/adversarial_defaults.rs").is_file()
            && ADVERSARIAL_DECODE_TEST.contains("decoded.version_value, 22"),
        "the behavioral decode fixture is absent from the scratch crate"
    );
    assert!(
        ADVERSARIAL_DEFAULTS_TEST.contains("changed_deep.deep.inner.value"),
        "the recursive default fixture is absent from the scratch crate"
    );
}

#[test]
fn each_run_rebuilds_the_scratch_tree_from_nothing() {
    // A module left over from an earlier probe keeps compiling, and the run
    // reports a corpus larger than the one it actually rendered.
    let root = scratch("rebuilt");
    write_crate(&root, &repository_root(), &CorpusRender::default())
        .unwrap_or_else(|error| panic!("write the probe crate: {error}"));

    let stale = root.join("src/generated/left_over.rs");
    fs::create_dir_all(
        stale
            .parent()
            .unwrap_or_else(|| panic!("the generated directory has no parent")),
    )
    .unwrap_or_else(|error| panic!("create the generated directory: {error}"));
    fs::write(&stale, "//! left over from an earlier run\n")
        .unwrap_or_else(|error| panic!("write the stale module: {error}"));

    write_crate(&root, &repository_root(), &CorpusRender::default())
        .unwrap_or_else(|error| panic!("rewrite the probe crate: {error}"));

    assert!(
        !stale.exists(),
        "a module from an earlier probe survived into the next run"
    );
}

#[test]
fn the_probe_root_re_exports_every_name_generated_code_imports_from_it() {
    let generated = checked_in_generated();
    let mut imported = Vec::new();
    for block in generated.split("use crate::{").skip(1) {
        let Some(names) = block.split_once("};") else {
            continue;
        };
        imported.extend(
            names
                .0
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
        );
    }
    imported.sort();
    imported.dedup();

    assert!(
        !imported.is_empty(),
        "no `use crate::{{..}}` was found in generated Rust, so this proof is vacuous"
    );
    for name in imported {
        assert!(
            LIB.contains(&name),
            "generated code imports `crate::{name}`, which the probe crate does not \
             re-export; every probe run would fail to compile for that reason alone"
        );
    }
}

#[test]
fn the_shim_defines_every_crate_function_generated_code_calls() {
    let generated = checked_in_generated();
    let mut called = Vec::new();
    for block in generated.split("crate::message::").skip(1) {
        let name = block
            .split(|character: char| !character.is_alphanumeric() && character != '_')
            .next()
            .unwrap_or_default();
        if !name.is_empty() {
            called.push(name.to_owned());
        }
    }
    called.sort();
    called.dedup();

    assert!(
        !called.is_empty(),
        "generated code calls nothing through `crate::message::`, so this proof is vacuous"
    );
    for name in called {
        assert!(
            MESSAGE_SHIM.contains(&format!("fn {name}<")),
            "generated code calls `crate::message::{name}`, which the probe shim \
             does not define"
        );
    }
}
