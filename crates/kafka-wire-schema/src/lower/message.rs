//! Message-level source lowering.

use std::path::{Path, PathBuf};

use crate::{
    CommonStruct, Message, MessageKind, MessageName, RawCommonStruct, RawMessage, RawMessageKind,
    StructRef,
};

use super::{LowerError, field::lower_field, field::parse_versions, structs::collect_struct_table};

/// Lowers one raw Kafka message definition into backend-neutral semantics.
///
/// The message name is normalized first because everything below it is bound
/// relative to it: the module-scoped naming rule scopes every nested struct to its owning message's
/// module, so the owner has to exist before a single field type can be parsed.
pub fn lower_message(raw: RawMessage, source: PathBuf) -> Result<Message, LowerError> {
    if !raw.extra.is_empty() {
        return Err(LowerError::MessageProperties {
            path: source,
            properties: raw.extra.keys().cloned().collect::<Vec<_>>().join(", "),
        });
    }

    let name = MessageName::new(raw.name);

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
        .map(|declaration| lower_common_struct(declaration, &name, &source))
        .collect::<Result<Vec<_>, _>>()?;
    let fields = raw
        .fields
        .into_iter()
        .map(|field| lower_field(field, &name, &source))
        .collect::<Result<Vec<_>, _>>()?;

    let structs = collect_struct_table(&common_structs, &fields);

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
        .map(|field| lower_field(field, owner, source))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CommonStruct {
        name: StructRef::qualify(owner, raw.name),
        versions,
        fields,
    })
}
