//! Compiler phase orchestration from lockfile to generated tree.

use std::collections::BTreeMap;

use crate::{
    GenerationError, GenerationReport, GeneratorConfig,
    group::group_sources,
    lockfile::ProtocolLock,
    manifest::render_manifest,
    output::apply_tree,
    render::{render_api, render_module_file, render_registry},
    source::load_sources,
};

/// Generates or checks the pinned protocol Rust tree.
pub fn generate(config: &GeneratorConfig) -> Result<GenerationReport, GenerationError> {
    let lock = ProtocolLock::read(&config.lockfile_path())?;
    let sources = load_sources(config.workspace_root(), &lock)?;
    let groups = group_sources(sources)?;

    let mut files = BTreeMap::new();
    for group in &groups {
        files.insert(
            format!("{}.rs", group.module_name),
            render_api(group, &lock.kafka.commit)?,
        );
    }
    files.insert(
        "mod.rs".to_owned(),
        render_module_file(&groups, &lock.kafka.commit),
    );
    files.insert(
        "registry.rs".to_owned(),
        render_registry(&groups, &lock.kafka.commit),
    );
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
