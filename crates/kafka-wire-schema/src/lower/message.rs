//! Message-level source lowering.

use std::path::PathBuf;

use crate::{Message, MessageKind, MessageName, RawMessage, RawMessageKind};

use super::{LowerError, field::lower_field, field::parse_versions};

/// Lowers one raw Kafka message definition into backend-neutral semantics.
pub fn lower_message(raw: RawMessage, source: PathBuf) -> Result<Message, LowerError> {
    if !raw.extra.is_empty() {
        return Err(LowerError::MessageProperties {
            path: source,
            properties: raw.extra.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }

    let api_key = raw.api_key.ok_or_else(|| LowerError::MissingApiKey {
        path: source.clone(),
        message: raw.name.clone(),
    })?;
    let valid_versions = parse_versions(&source, "valid", &raw.name, &raw.valid_versions)?;
    let flexible_versions = parse_versions(&source, "flexible", &raw.name, &raw.flexible_versions)?;
    let fields = raw
        .fields
        .into_iter()
        .map(|field| lower_field(field, &source))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Message {
        source,
        api_key,
        kind: match raw.kind {
            RawMessageKind::Request => MessageKind::Request,
            RawMessageKind::Response => MessageKind::Response,
        },
        listeners: raw.listeners,
        name: MessageName::new(raw.name),
        valid_versions,
        flexible_versions,
        fields,
    })
}
