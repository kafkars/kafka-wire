//! Compiler phase orchestration from lockfile to generated tree.

use std::collections::BTreeMap;

use crate::{
    GenerationError, GenerationReport, GeneratorConfig,
    corpus_validation::validate_source_corpus,
    format::format_rendered_rust_with_identity,
    group::group_sources,
    lockfile::ProtocolLock,
    manifest::render_manifest,
    namespace::validate_generated_namespace,
    output::apply_tree,
    overrides::{HeaderOverrides, SchemaExceptionOverrides},
    provenance::semantic_inputs,
    render::{
        render_api, render_exports_file, render_fuzz_dispatch, render_header_version,
        render_module_file, render_registry, render_unkeyed,
    },
    source::load_sources_with,
};

/// Generates or checks the pinned protocol Rust tree.
pub fn generate(config: &GeneratorConfig) -> Result<GenerationReport, GenerationError> {
    let (lock, lock_bytes) = ProtocolLock::read_with_bytes(&config.lockfile_path())?;
    let schema_overrides = SchemaExceptionOverrides::read(config.workspace_root(), &lock)?;
    let exceptions = schema_overrides.exceptions();
    let sources = load_sources_with(config.workspace_root(), &lock, &exceptions)?;
    validate_source_corpus(&sources)?;
    let grouped = group_sources(sources)?;
    validate_generated_namespace(&grouped.api, &grouped.unkeyed)?;
    let overrides = HeaderOverrides::read(
        config.workspace_root(),
        &lock,
        &grouped.api,
        &grouped.unkeyed,
    )?;
    let groups = grouped.api;
    validate_output_paths(&groups)?;

    let mut rendered = BTreeMap::new();
    let mut producers = BTreeMap::new();
    if !grouped.unkeyed.is_empty() {
        insert_unique(
            &mut rendered,
            &mut producers,
            "framing.rs".to_owned(),
            render_unkeyed(&grouped.unkeyed, &lock.kafka.commit)?,
            "fixed framing output",
        )?;
    }
    for group in &groups {
        let producer = api_producer(group);
        insert_unique(
            &mut rendered,
            &mut producers,
            format!("{}.rs", group.module_name()),
            render_api(group, &lock.kafka.commit)?,
            &producer,
        )?;
    }
    insert_unique(
        &mut rendered,
        &mut producers,
        "mod.rs".to_owned(),
        render_module_file(&groups, &grouped.unkeyed, &lock.kafka.commit),
        "fixed module facade",
    )?;
    // The crate root's own export list. `lib.rs` includes it, which is what lets
    // the flat facade name every generated item without a wildcard re-export.
    insert_unique(
        &mut rendered,
        &mut producers,
        "exports.rsi".to_owned(),
        render_exports_file(&groups, &grouped.unkeyed, &lock.kafka.commit),
        "fixed crate export list",
    )?;
    insert_unique(
        &mut rendered,
        &mut producers,
        "registry.rs".to_owned(),
        render_registry(&groups, &lock.kafka.commit),
        "fixed API registry",
    )?;
    insert_unique(
        &mut rendered,
        &mut producers,
        "header_version.rs".to_owned(),
        render_header_version(&overrides, &lock.kafka.commit),
        "fixed header-version policy",
    )?;
    insert_unique(
        &mut rendered,
        &mut producers,
        "fuzz_roundtrip.rs".to_owned(),
        render_fuzz_dispatch(&groups, &lock.kafka.commit)?,
        "fixed fuzz dispatch",
    )?;

    // Layout belongs to rustfmt, so the manifest must hash formatted bytes.
    let formatted = format_rendered_rust_with_identity(rendered, config.workspace_root())?;
    let inputs = semantic_inputs(
        config.workspace_root(),
        &lock_bytes,
        overrides.input_bytes(),
        schema_overrides.input_bytes(),
        lock.generator.ir_version,
        formatted.formatter_identity,
    )?;
    let mut files = formatted.files;

    let manifest = render_manifest(
        &files,
        lock.generator.ir_version,
        &lock.kafka.repository,
        &lock.kafka.commit,
        inputs,
    )?;
    insert_unique(
        &mut files,
        &mut producers,
        "MANIFEST.json".to_owned(),
        manifest,
        "generated-tree manifest",
    )?;

    let output_root = lock.generator.output.join_to(config.workspace_root());
    apply_tree(&output_root, &files, config.mode())
}

fn validate_output_paths(groups: &[crate::group::ApiGroup]) -> Result<(), GenerationError> {
    let mut claimed = BTreeMap::new();
    for (path, producer) in [
        ("framing.rs", "fixed framing output"),
        ("mod.rs", "fixed module facade"),
        ("exports.rsi", "fixed crate export list"),
        ("registry.rs", "fixed API registry"),
        ("header_version.rs", "fixed header-version policy"),
        ("fuzz_roundtrip.rs", "fixed fuzz dispatch"),
        ("MANIFEST.json", "generated-tree manifest"),
    ] {
        claim_output_path(&mut claimed, path, producer)?;
    }
    for group in groups {
        claim_output_path(
            &mut claimed,
            &format!("{}.rs", group.module_name()),
            &api_producer(group),
        )?;
    }
    Ok(())
}

pub(crate) fn api_producer(group: &crate::group::ApiGroup) -> String {
    let names = group
        .messages()
        .map(|source| source.message.name.protocol())
        .collect::<Vec<_>>()
        .join(" and ");
    format!("API key {} ({names})", group.api_key)
}

fn insert_unique(
    files: &mut BTreeMap<String, String>,
    producers: &mut BTreeMap<String, String>,
    path: String,
    file: String,
    producer: &str,
) -> Result<(), GenerationError> {
    claim_output_path(producers, &path, producer)?;
    files.insert(path, file);
    Ok(())
}

pub(crate) fn claim_output_path(
    claimed: &mut BTreeMap<String, String>,
    path: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    if is_windows_device_path(path) {
        return Err(GenerationError::NonPortableGeneratedPath {
            path: path.to_owned(),
            producer: producer.to_owned(),
            reason: "the filename stem is a reserved Windows device name",
        });
    }
    if let Some(first) = claimed.get(path) {
        return Err(GenerationError::GeneratedPathCollision {
            path: path.to_owned(),
            first: first.clone(),
            second: producer.to_owned(),
        });
    }
    claimed.insert(path.to_owned(), producer.to_owned());
    Ok(())
}

fn is_windows_device_path(path: &str) -> bool {
    let filename = path.rsplit('/').next().unwrap_or(path);
    let stem = filename
        .split('.')
        .next()
        .unwrap_or(filename)
        .trim_end_matches([' ', '.'])
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|suffix| suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9'))
}
