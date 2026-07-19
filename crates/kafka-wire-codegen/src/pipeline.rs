//! Compiler phase orchestration from lockfile to generated tree.

use std::collections::BTreeMap;

use crate::{
    GenerationError, GenerationReport, GeneratorConfig,
    format::format_rendered_rust,
    group::group_sources,
    lockfile::ProtocolLock,
    manifest::render_manifest,
    output::apply_tree,
    render::{render_api, render_module_file, render_registry, render_unkeyed},
    source::load_sources,
};

/// Generates or checks the pinned protocol Rust tree.
pub fn generate(config: &GeneratorConfig) -> Result<GenerationReport, GenerationError> {
    let lock = ProtocolLock::read(&config.lockfile_path())?;
    let sources = load_sources(config.workspace_root(), &lock)?;
    let grouped = group_sources(sources)?;
    let groups = grouped.api;

    let mut rendered = BTreeMap::new();
    if !grouped.unkeyed.is_empty() {
        rendered.insert(
            "framing.rs".to_owned(),
            render_unkeyed(&grouped.unkeyed, &lock.kafka.commit)?,
        );
    }
    for group in &groups {
        rendered.insert(
            format!("{}.rs", group.module_name),
            render_api(group, &lock.kafka.commit)?,
        );
    }
    rendered.insert(
        "mod.rs".to_owned(),
        render_module_file(&groups, &lock.kafka.commit),
    );
    rendered.insert(
        "registry.rs".to_owned(),
        render_registry(&groups, &lock.kafka.commit),
    );

    // Layout belongs to rustfmt, so the manifest must hash formatted bytes.
    let mut files = format_rendered_rust(rendered, config.workspace_root())?;

    let manifest = render_manifest(
        &files,
        lock.generator.ir_version,
        &lock.kafka.repository,
        &lock.kafka.commit,
    )?;
    files.insert("MANIFEST.json".to_owned(), manifest);

    let output_root = config.workspace_root().join(&lock.generator.output);
    apply_tree(&output_root, &files, config.mode())
}
