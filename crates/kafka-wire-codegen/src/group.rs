//! Validated request/response pairs grouped by Kafka API key.
//!
//! This phase owns the facts shared by both wire directions: checked API
//! identity, supported and flexible versions, and negotiation policy. It does
//! not render Rust or infer compatibility after the pair has crossed this seam.

use std::collections::BTreeMap;

use kafka_wire_schema::{ApiName, MessageKind, VersionSet};

use crate::{GenerationError, PairError, source::MessageSource};

/// One generated Rust module containing one validated Kafka API pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiGroup {
    pub(crate) api_key: i16,
    pub(crate) name: ApiName,
    pub(crate) request: MessageSource,
    pub(crate) response: MessageSource,
    pub(crate) supported_versions: VersionSet,
    pub(crate) flexible_versions: VersionSet,
    pub(crate) latest_version_unstable: bool,
}

impl ApiGroup {
    pub(crate) fn messages(&self) -> impl Iterator<Item = &MessageSource> {
        [&self.request, &self.response].into_iter()
    }

    pub(crate) fn module_name(&self) -> &str {
        self.name.rust_module()
    }
}

#[derive(Default)]
struct PendingGroup {
    request: Option<MessageSource>,
    response: Option<MessageSource>,
}

/// One grouping pass: validated API pairs and schemas that answer to no key.
#[derive(Debug)]
pub(crate) struct Grouped {
    pub(crate) api: Vec<ApiGroup>,
    /// Headers and data schemas, in protocol order.
    ///
    /// A header is not dispatched by API key and carries no descriptor: it is
    /// the frame around a message, not a message. It is kept out of `ApiGroup`
    /// so every later stage can assume a complete pair and key exist there.
    pub(crate) unkeyed: Vec<MessageSource>,
}

pub(crate) fn group_sources(sources: Vec<MessageSource>) -> Result<Grouped, GenerationError> {
    let mut pending: BTreeMap<i16, PendingGroup> = BTreeMap::new();
    let mut unkeyed = Vec::new();

    for source in sources {
        let (MessageKind::Request | MessageKind::Response) = source.message.kind else {
            unkeyed.push(source);
            continue;
        };
        let Some(api_key) = source.message.api_key else {
            return Err(GenerationError::UnsupportedSchema {
                message: source.message.name.protocol().to_owned(),
                field: "<message>".to_owned(),
                reason: "message declares no apiKey".to_owned(),
            });
        };
        ApiName::try_from_message(&source.message.name).map_err(|error| {
            PairError::InvalidApiName {
                message: source.message.name.protocol().to_owned(),
                reason: error.to_string(),
            }
        })?;

        let group = pending.entry(api_key).or_default();
        match source.message.kind {
            MessageKind::Request => {
                insert_direction(&mut group.request, source, api_key, "request")?;
            }
            MessageKind::Response => {
                insert_direction(&mut group.response, source, api_key, "response")?;
            }
            MessageKind::Header | MessageKind::Data => {
                return Err(GenerationError::InternalInvariant {
                    message: source.message.name.protocol().to_owned(),
                    invariant: "an unkeyed schema entered API-pair insertion".to_owned(),
                });
            }
        }
    }

    let api = pending
        .into_iter()
        .map(|(api_key, group)| finish_pair(api_key, group))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Grouped { api, unkeyed })
}

fn finish_pair(api_key: i16, group: PendingGroup) -> Result<ApiGroup, GenerationError> {
    let request = group.request.ok_or(PairError::MissingDirection {
        api_key,
        direction: "request",
    })?;
    let response = group.response.ok_or(PairError::MissingDirection {
        api_key,
        direction: "response",
    })?;

    let request_name = ApiName::try_from_message(&request.message.name).map_err(|error| {
        PairError::InvalidApiName {
            message: request.message.name.protocol().to_owned(),
            reason: error.to_string(),
        }
    })?;
    let response_name = ApiName::try_from_message(&response.message.name).map_err(|error| {
        PairError::InvalidApiName {
            message: response.message.name.protocol().to_owned(),
            reason: error.to_string(),
        }
    })?;
    if request_name.protocol_stem() != response_name.protocol_stem() {
        return Err(PairError::NameMismatch {
            api_key,
            request: request.message.name.protocol().to_owned(),
            response: response.message.name.protocol().to_owned(),
        }
        .into());
    }

    if request.message.valid_versions != response.message.valid_versions {
        return Err(PairError::SupportedVersions {
            api_key,
            request: request.message.valid_versions.to_string(),
            response: response.message.valid_versions.to_string(),
        }
        .into());
    }
    let request_flexible = request.message.effective_flexible_versions();
    let response_flexible = response.message.effective_flexible_versions();
    if request_flexible != response_flexible {
        return Err(PairError::FlexibleVersions {
            api_key,
            request: request_flexible.to_string(),
            response: response_flexible.to_string(),
        }
        .into());
    }
    if response.message.latest_version_unstable {
        return Err(PairError::UnstablePolicy {
            api_key,
            response: response.message.name.protocol().to_owned(),
        }
        .into());
    }

    Ok(ApiGroup {
        api_key,
        name: request_name,
        supported_versions: request.message.valid_versions.clone(),
        flexible_versions: request_flexible,
        latest_version_unstable: request.message.latest_version_unstable,
        request,
        response,
    })
}

fn insert_direction(
    slot: &mut Option<MessageSource>,
    source: MessageSource,
    api_key: i16,
    direction: &'static str,
) -> Result<(), GenerationError> {
    if let Some(existing) = slot {
        return Err(PairError::DuplicateDirection {
            api_key,
            direction,
            left: existing.message.name.protocol().to_owned(),
            right: source.message.name.protocol().to_owned(),
        }
        .into());
    }
    *slot = Some(source);
    Ok(())
}
