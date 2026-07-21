//! Per-file rendering and collision-safe insertion for the corpus probe.
//!
//! Whole-corpus loading and accounting remain in `corpus.rs`; this module owns
//! only the transition from one grouped API to one claimed probe output.

use std::{collections::BTreeMap, path::Path};

use crate::{
    CorpusOutcome, CorpusRender, GenerationError,
    format::format_rendered_rust,
    group::ApiGroup,
    pipeline::{api_producer, claim_output_path},
    render::render_api,
};

pub(crate) fn render_group(
    group: &ApiGroup,
    commit: &str,
    workspace: &Path,
    probe: &mut CorpusRender,
    emitted: &mut Vec<ApiGroup>,
    producers: &mut BTreeMap<String, String>,
) -> Result<(), GenerationError> {
    let filename = format!("{}.rs", group.module_name);
    let rendered = render_api(group, commit).and_then(|source| {
        format_rendered_rust(BTreeMap::from([(filename.clone(), source)]), workspace)
    });

    match rendered {
        Ok(formatted) => {
            let producer = api_producer(group);
            for (path, source) in formatted {
                insert_probe_file(&mut probe.files, producers, path, source, &producer)?;
            }
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
            let reason = refusal_cause(&error);
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
    Ok(())
}

pub(crate) fn insert_probe_file(
    files: &mut BTreeMap<String, String>,
    producers: &mut BTreeMap<String, String>,
    path: String,
    source: String,
    producer: &str,
) -> Result<(), GenerationError> {
    claim_output_path(producers, &path, producer)?;
    files.insert(path, source);
    Ok(())
}

pub(crate) fn refusal_cause(error: &GenerationError) -> String {
    match error {
        GenerationError::UnsupportedSchema { reason, .. } => reason.clone(),
        other => other.to_string(),
    }
}
