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
    format::format_rendered_rust,
    group::{ApiGroup, group_sources},
    lockfile::ProtocolLock,
    overrides::HeaderOverrides,
    render::{
        render_api, render_header_version, render_module_file, render_registry, render_unkeyed,
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
                probe.files.insert("framing.rs".to_owned(), source);
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
                            reason: cause(&error),
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
        );
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
        ]),
        workspace,
    )?;
    probe.files.extend(facades);

    Ok(probe)
}

/// The construct a refusal is about, with the message and field stripped off.
///
/// Two messages blocked on inline structs are one piece of work. Keying the
/// taxonomy on the full diagnostic would report them as two, and the queue
/// would be as long as the corpus.
fn cause(error: &GenerationError) -> String {
    match error {
        GenerationError::UnsupportedSchema { reason, .. } => reason.clone(),
        other => other.to_string(),
    }
}

/// Renders one API pair, recording either its module or why it was refused.
fn render_group(
    group: &ApiGroup,
    commit: &str,
    workspace: &Path,
    probe: &mut CorpusRender,
    emitted: &mut Vec<ApiGroup>,
) {
    let filename = format!("{}.rs", group.module_name);
    // Formatting is part of rendering here: text this backend produced that
    // rustfmt cannot parse is not Rust, and counting it as rendered would
    // overstate the answer this probe exists to give.
    let rendered = render_api(group, commit).and_then(|source| {
        format_rendered_rust(BTreeMap::from([(filename.clone(), source)]), workspace)
    });

    match rendered {
        Ok(formatted) => {
            probe.files.extend(formatted);
            for source in group.messages() {
                probe.outcomes.insert(
                    source.filename.clone(),
                    CorpusOutcome::Rendered {
                        module: filename.clone(),
                    },
                );
            }
            emitted.push(group.clone());
        }
        Err(error) => {
            let reason = cause(&error);
            for source in group.messages() {
                probe.outcomes.insert(
                    source.filename.clone(),
                    CorpusOutcome::NotRendered {
                        reason: reason.clone(),
                    },
                );
            }
        }
    }
}
