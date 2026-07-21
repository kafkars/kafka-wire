//! Whole-corpus render probe: how much of the pinned protocol the backend emits.
//!
//! This module owns one measurement. It hands every pinned source file to the
//! schema front end and then to the renderer, records what happened to each
//! one, and returns the Rust it managed to produce. Nothing here is a decision:
//! it does not consult `status`, does not stop at the first refusal, and does
//! not write anything. `pipeline.rs` remains the only path that generates the
//! checked-in tree, and it stays all-or-nothing on purpose.
//!
//! The point of separating them is that "which messages can we compile" is a
//! question worth answering continuously, long before the answer is "all of
//! them" and long before any of it is checked in.

use std::{collections::BTreeMap, path::Path};

use crate::{
    GenerationError,
    corpus_output::{insert_probe_file, refusal_cause, render_group},
    format::format_rendered_rust,
    group::group_sources,
    lockfile::ProtocolLock,
    overrides::HeaderOverrides,
    render::{
        render_fuzz_dispatch, render_header_version, render_module_file, render_registry,
        render_unkeyed,
    },
    source::load_every_source,
};

/// What the backend could do with one pinned source file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorpusOutcome {
    /// Rendered into the probe tree, in the named module.
    Rendered {
        /// Generated module file the message landed in.
        module: String,
    },
    /// The schema front end rejected the file.
    NotLoaded {
        /// Front-end diagnostic.
        reason: String,
    },
    /// The file loaded, but the backend has no emission for it.
    NotRendered {
        /// Generator diagnostic.
        reason: String,
    },
}

/// One whole-corpus render probe.
#[derive(Clone, Debug, Default)]
pub struct CorpusRender {
    /// Rendered and formatted Rust, by generated file name.
    pub files: BTreeMap<String, String>,
    /// Every pinned source file and what happened to it, by upstream filename.
    pub outcomes: BTreeMap<String, CorpusOutcome>,
}

impl CorpusRender {
    /// Number of pinned files the backend emitted Rust for.
    pub fn rendered(&self) -> usize {
        self.outcomes
            .values()
            .filter(|outcome| matches!(outcome, CorpusOutcome::Rendered { .. }))
            .count()
    }

    /// Refusal causes and the pinned files each one accounts for.
    ///
    /// Grouped by cause rather than listed per file: the useful shape of this
    /// answer is a work queue, and a hundred files blocked on one missing
    /// construct is one task, not a hundred.
    pub fn failure_taxonomy(&self) -> BTreeMap<String, Vec<String>> {
        let mut taxonomy: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (filename, outcome) in &self.outcomes {
            let reason = match outcome {
                CorpusOutcome::Rendered { .. } => continue,
                CorpusOutcome::NotLoaded { reason } | CorpusOutcome::NotRendered { reason } => {
                    reason
                }
            };
            taxonomy
                .entry(reason.clone())
                .or_default()
                .push(filename.clone());
        }
        taxonomy
    }
}

/// Renders every pinned source file the backend can, and reports the rest.
pub fn render_corpus(workspace_root: impl AsRef<Path>) -> Result<CorpusRender, GenerationError> {
    let workspace = workspace_root.as_ref();
    let lock = ProtocolLock::read(&workspace.join("spec/protocol.lock"))?;
    let corpus = load_every_source(workspace, &lock)?;

    let mut probe = CorpusRender::default();
    let mut producers = BTreeMap::new();
    for (filename, reason) in corpus.rejected {
        probe
            .outcomes
            .insert(filename, CorpusOutcome::NotLoaded { reason });
    }

    let grouped = group_sources(corpus.sources)?;
    let overrides = HeaderOverrides::read(workspace, &lock, &grouped.api, &grouped.unkeyed)?;

    // Headers and data schemas answer to no API key and render into one module
    // of their own rather than joining a pair.
    if !grouped.unkeyed.is_empty() {
        match render_unkeyed(&grouped.unkeyed, &lock.kafka.commit) {
            Ok(source) => {
                insert_probe_file(
                    &mut probe.files,
                    &mut producers,
                    "framing.rs".to_owned(),
                    source,
                    "fixed framing output",
                )?;
                for unkeyed in &grouped.unkeyed {
                    probe.outcomes.insert(
                        unkeyed.filename.clone(),
                        CorpusOutcome::Rendered {
                            module: "framing.rs".to_owned(),
                        },
                    );
                }
            }
            Err(error) => {
                for unkeyed in &grouped.unkeyed {
                    probe.outcomes.insert(
                        unkeyed.filename.clone(),
                        CorpusOutcome::NotRendered {
                            reason: refusal_cause(&error),
                        },
                    );
                }
            }
        }
    }

    let mut emitted = Vec::new();
    for group in grouped.api {
        render_group(
            &group,
            &lock.kafka.commit,
            workspace,
            &mut probe,
            &mut emitted,
            &mut producers,
        )?;
    }

    // Every file `mod.rs` declares must be written, or the probe cannot compile
    // and so cannot answer the one question it exists to ask. `header_version`
    // is not keyed by a schema and has no outcome to record, but it is a module
    // the facade names, which is enough.
    let facades = format_rendered_rust(
        BTreeMap::from([
            (
                "mod.rs".to_owned(),
                render_module_file(&emitted, &grouped.unkeyed, &lock.kafka.commit),
            ),
            (
                "registry.rs".to_owned(),
                render_registry(&emitted, &lock.kafka.commit),
            ),
            (
                "header_version.rs".to_owned(),
                render_header_version(&overrides, &lock.kafka.commit),
            ),
            (
                "fuzz_roundtrip.rs".to_owned(),
                render_fuzz_dispatch(&emitted, &lock.kafka.commit)?,
            ),
        ]),
        workspace,
    )?;
    for (path, source) in facades {
        let producer = match path.as_str() {
            "mod.rs" => "fixed module facade",
            "registry.rs" => "fixed API registry",
            "header_version.rs" => "fixed header-version policy",
            "fuzz_roundtrip.rs" => "fixed fuzz dispatch",
            _ => "fixed corpus-probe output",
        };
        insert_probe_file(&mut probe.files, &mut producers, path, source, producer)?;
    }

    Ok(probe)
}
