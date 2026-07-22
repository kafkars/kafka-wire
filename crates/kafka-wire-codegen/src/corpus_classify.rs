//! Tolerant semantic classification for the whole-corpus compile probe.
//!
//! Production generation remains all-or-nothing. This file instead isolates a
//! global, pair, or namespace refusal to the source files that caused it so one
//! bad schema cannot erase the probe's answer for every other pinned source.

use std::collections::{BTreeMap, BTreeSet};

use kafka_wire_schema::MessageKind;

use crate::{
    CorpusOutcome, GenerationError,
    group::{Grouped, group_sources},
    namespace::validate_generated_namespace,
    source::MessageSource,
};

pub(crate) fn classify_semantics(
    sources: Vec<MessageSource>,
    outcomes: &mut BTreeMap<String, CorpusOutcome>,
) -> Result<Grouped, GenerationError> {
    let sources = retain_corpus_valid(sources, outcomes)?;
    let grouped = group_independently(sources, outcomes);
    Ok(retain_namespace_valid(grouped, outcomes))
}

fn retain_corpus_valid(
    mut sources: Vec<MessageSource>,
    outcomes: &mut BTreeMap<String, CorpusOutcome>,
) -> Result<Vec<MessageSource>, GenerationError> {
    let messages = sources
        .iter()
        .map(|source| source.message.clone())
        .collect::<Vec<_>>();
    let Err(errors) = kafka_wire_schema::validate_struct_names(&messages) else {
        return Ok(sources);
    };

    let rejected = errors
        .0
        .iter()
        .map(|error| error.path.clone())
        .collect::<BTreeSet<_>>();
    if !sources
        .iter()
        .any(|source| rejected.contains(&source.message.source))
    {
        return Err(GenerationError::CorpusValidation(errors));
    }
    for source in &sources {
        let diagnostics = errors
            .0
            .iter()
            .filter(|error| error.path == source.message.source)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !diagnostics.is_empty() {
            outcomes.insert(
                source.filename.clone(),
                CorpusOutcome::NotRendered {
                    reason: diagnostics.join("; "),
                },
            );
        }
    }
    sources.retain(|source| !rejected.contains(&source.message.source));
    Ok(sources)
}

fn group_independently(
    sources: Vec<MessageSource>,
    outcomes: &mut BTreeMap<String, CorpusOutcome>,
) -> Grouped {
    let mut by_key: BTreeMap<i16, Vec<MessageSource>> = BTreeMap::new();
    let mut unkeyed = Vec::new();
    for source in sources {
        match source.message.kind {
            MessageKind::Header | MessageKind::Data => unkeyed.push(source),
            MessageKind::Request | MessageKind::Response => {
                if let Some(api_key) = source.message.api_key {
                    by_key.entry(api_key).or_default().push(source);
                } else {
                    outcomes.insert(
                        source.filename,
                        CorpusOutcome::NotRendered {
                            reason: "directional message declares no apiKey".to_owned(),
                        },
                    );
                }
            }
        }
    }

    let mut api = Vec::new();
    for bucket in by_key.into_values() {
        match group_sources(bucket.clone()) {
            Ok(grouped) => api.extend(grouped.api),
            Err(error) => {
                let reason = error.to_string();
                for source in bucket {
                    outcomes.insert(
                        source.filename,
                        CorpusOutcome::NotRendered {
                            reason: reason.clone(),
                        },
                    );
                }
            }
        }
    }
    Grouped { api, unkeyed }
}

fn retain_namespace_valid(
    grouped: Grouped,
    outcomes: &mut BTreeMap<String, CorpusOutcome>,
) -> Grouped {
    let mut unkeyed = Vec::new();
    for source in grouped.unkeyed {
        let mut candidate = unkeyed.clone();
        candidate.push(source.clone());
        match validate_generated_namespace(&[], &candidate) {
            Ok(()) => unkeyed.push(source),
            Err(error) => {
                outcomes.insert(
                    source.filename,
                    CorpusOutcome::NotRendered {
                        reason: error.to_string(),
                    },
                );
            }
        }
    }

    let mut api = Vec::new();
    for group in grouped.api {
        let mut candidate = api.clone();
        candidate.push(group.clone());
        match validate_generated_namespace(&candidate, &unkeyed) {
            Ok(()) => api.push(group),
            Err(error) => {
                let reason = error.to_string();
                for source in group.messages() {
                    outcomes.insert(
                        source.filename.clone(),
                        CorpusOutcome::NotRendered {
                            reason: reason.clone(),
                        },
                    );
                }
            }
        }
    }
    Grouped { api, unkeyed }
}
