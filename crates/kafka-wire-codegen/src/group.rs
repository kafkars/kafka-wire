//! Stable request/response grouping by Kafka API key.

use std::collections::BTreeMap;

use kafka_wire_schema::MessageKind;

use crate::{GenerationError, source::MessageSource};

/// One generated Rust module containing one Kafka API pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApiGroup {
    pub(crate) api_key: i16,
    pub(crate) module_name: String,
    pub(crate) request: Option<MessageSource>,
    pub(crate) response: Option<MessageSource>,
}

impl ApiGroup {
    pub(crate) fn messages(&self) -> impl Iterator<Item = &MessageSource> {
        self.request.iter().chain(self.response.iter())
    }
}

pub(crate) fn group_sources(sources: Vec<MessageSource>) -> Result<Vec<ApiGroup>, GenerationError> {
    let mut groups: BTreeMap<i16, ApiGroup> = BTreeMap::new();
    for source in sources {
        // Headers and data schemas are not dispatched by API key, so they have
        // no group to join. Rejecting them here keeps every later stage able to
        // assume a key exists.
        let (MessageKind::Request | MessageKind::Response) = source.message.kind else {
            return Err(GenerationError::UnsupportedSchema {
                message: source.message.name.protocol().to_owned(),
                field: "<message>".to_owned(),
                reason: "only request and response schemas are grouped by API key".to_owned(),
            });
        };
        let Some(api_key) = source.message.api_key else {
            return Err(GenerationError::UnsupportedSchema {
                message: source.message.name.protocol().to_owned(),
                field: "<message>".to_owned(),
                reason: "message declares no apiKey".to_owned(),
            });
        };
        let stem = source.message.name.api_stem().to_owned();
        let module_name = module_name(&source);
        let group = groups.entry(api_key).or_insert_with(|| ApiGroup {
            api_key,
            module_name,
            request: None,
            response: None,
        });
        match source.message.kind {
            MessageKind::Request => {
                insert_direction(&mut group.request, source, api_key, "request")?;
            }
            MessageKind::Response => {
                insert_direction(&mut group.response, source, api_key, "response")?;
            }
            // Rejected above; the arm keeps the match total.
            MessageKind::Header | MessageKind::Data => {}
        }

        if let (Some(request), Some(response)) = (&group.request, &group.response) {
            if request.message.name.api_stem() != response.message.name.api_stem() {
                return Err(GenerationError::PairName {
                    api_key,
                    request: request.message.name.protocol().to_owned(),
                    response: response.message.name.protocol().to_owned(),
                });
            }
            group.module_name = module_name_from_stem(&stem);
        }
    }
    Ok(groups.into_values().collect())
}

fn insert_direction(
    slot: &mut Option<MessageSource>,
    source: MessageSource,
    api_key: i16,
    direction: &'static str,
) -> Result<(), GenerationError> {
    if let Some(existing) = slot {
        return Err(GenerationError::DuplicateDirection {
            api_key,
            direction,
            left: existing.message.name.protocol().to_owned(),
            right: source.message.name.protocol().to_owned(),
        });
    }
    *slot = Some(source);
    Ok(())
}

fn module_name(source: &MessageSource) -> String {
    source
        .message
        .name
        .rust_module()
        .strip_suffix("_request")
        .or_else(|| source.message.name.rust_module().strip_suffix("_response"))
        .unwrap_or(source.message.name.rust_module())
        .to_owned()
}

fn module_name_from_stem(stem: &str) -> String {
    let normalized = kafka_wire_schema::MessageName::new(stem.to_owned());
    normalized.rust_module().to_owned()
}
