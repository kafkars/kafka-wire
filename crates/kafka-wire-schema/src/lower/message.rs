//! Message-level source lowering.

use std::path::{Path, PathBuf};

use crate::{
    CommonStruct, Message, MessageKind, MessageName, RawCommonStruct, RawMessage, RawMessageKind,
    StructRef, VersionSet,
};

use super::{LowerError, field::lower_field, field::parse_versions, structs::collect_struct_table};

/// Lowers one raw Kafka message definition into backend-neutral semantics.
///
/// The message name is normalized first because every nested struct is bound
/// relative to its owning message's module. The owner therefore has to exist
/// before a single field type can be parsed.
pub fn lower_message(raw: RawMessage, source: PathBuf) -> Result<Message, LowerError> {
    if !raw.extra.is_empty() {
        return Err(LowerError::MessageProperties {
            path: source,
            properties: raw.extra.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }

    let name = MessageName::try_new(raw.name).map_err(|error| LowerError::Identifier {
        path: source.clone(),
        kind: "message",
        name: error.input().to_owned(),
        reason: error.to_string(),
    })?;

    let valid_versions = parse_versions(&source, "valid", name.protocol(), &raw.valid_versions)?;
    // A schema that omits `flexibleVersions` predates the tagged-field
    // encoding, so it is flexible at no version rather than at all of them.
    let flexible_versions = parse_versions(
        &source,
        "flexible",
        name.protocol(),
        raw.flexible_versions.as_deref().unwrap_or("none"),
    )?;

    let common_structs = raw
        .common_structs
        .into_iter()
        .map(|declaration| lower_common_struct(declaration, &name, &valid_versions, &source))
        .collect::<Result<Vec<_>, _>>()?;
    let fields = raw
        .fields
        .into_iter()
        .map(|field| lower_field(field, &name, &valid_versions, &source))
        .collect::<Result<Vec<_>, _>>()?;

    let structs = collect_struct_table(&common_structs, &fields, &valid_versions);

    Ok(Message {
        source,
        api_key: raw.api_key,
        kind: match raw.kind {
            RawMessageKind::Request => MessageKind::Request,
            RawMessageKind::Response => MessageKind::Response,
            RawMessageKind::Header => MessageKind::Header,
            RawMessageKind::Data => MessageKind::Data,
        },
        listeners: raw.listeners,
        name,
        valid_versions,
        flexible_versions,
        latest_version_unstable: raw.latest_version_unstable,
        common_structs,
        fields,
        structs,
    })
}

fn lower_common_struct(
    raw: RawCommonStruct,
    owner: &MessageName,
    valid_versions: &VersionSet,
    source: &Path,
) -> Result<CommonStruct, LowerError> {
    if !raw.extra.is_empty() {
        return Err(LowerError::CommonStructProperties {
            path: source.to_path_buf(),
            declaration: raw.name,
            properties: raw.extra.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }

    let versions = parse_versions(source, "common struct", &raw.name, &raw.versions)?;
    let fields = raw
        .fields
        .into_iter()
        .map(|field| lower_field(field, owner, valid_versions, source))
        .collect::<Result<Vec<_>, _>>()?;

    let name = StructRef::try_qualify(owner, raw.name).map_err(|error| LowerError::Identifier {
        path: source.to_path_buf(),
        kind: "common struct",
        name: error.input().to_owned(),
        reason: error.to_string(),
    })?;

    Ok(CommonStruct {
        name,
        versions,
        fields,
    })
}
