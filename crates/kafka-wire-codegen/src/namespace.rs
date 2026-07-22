//! Complete generated and public Rust namespace claims: modules, re-exports,
//! descriptors, fixed vocabulary, and the handwritten crate facade.
//! Per-message struct collisions remain schema-front-end invariants.

use std::collections::BTreeMap;

use crate::{
    GenerationError,
    group::ApiGroup,
    namespace_members::validate_synthesized_members,
    render::{api_descriptor_name, descriptor_name},
    source::MessageSource,
};

const GENERATED_TYPE_NAMESPACE: &str = "the generated module type namespace";
const GENERATED_VALUE_NAMESPACE: &str = "the generated module value namespace";
const PUBLIC_TYPE_NAMESPACE: &str = "the kafka-wire crate-root type namespace";
const PUBLIC_VALUE_NAMESPACE: &str = "the kafka-wire crate-root value namespace";

/// Proves every emitted symbol has one producer in each scope it enters.
pub(crate) fn validate_generated_namespace(
    groups: &[ApiGroup],
    unkeyed: &[MessageSource],
) -> Result<(), GenerationError> {
    let mut generated_types = BTreeMap::new();
    let mut generated_values = BTreeMap::new();
    let mut public_types = handwritten_public_types();
    let mut public_values = handwritten_public_values();

    for (module, producer) in [
        ("registry", "fixed API registry module"),
        ("header_version", "fixed header-version policy module"),
    ] {
        claim(
            &mut generated_types,
            GENERATED_TYPE_NAMESPACE,
            module,
            producer,
        )?;
    }
    if !unkeyed.is_empty() {
        claim(
            &mut generated_types,
            GENERATED_TYPE_NAMESPACE,
            "framing",
            "fixed framing module",
        )?;
    }

    for group in groups {
        let producer = format!(
            "API key {} pair {}",
            group.api_key,
            group.name.protocol_stem()
        );
        claim(
            &mut generated_types,
            GENERATED_TYPE_NAMESPACE,
            group.module_name(),
            &producer,
        )?;
        for source in group.messages() {
            validate_message_module(source)?;
            claim_message_exports(
                source,
                &mut generated_types,
                &mut generated_values,
                &mut public_types,
                &mut public_values,
            )?;
        }
        claim_value_export(
            &mut generated_values,
            &mut public_values,
            &api_descriptor_name(group),
            &format!("pair descriptor for {}", group.name.protocol_stem()),
        )?;
    }

    for source in unkeyed {
        validate_message_module(source)?;
        claim_type_export(
            &mut generated_types,
            &mut public_types,
            source.message.name.rust_type(),
            &format!("message {}", source.message.name.protocol()),
        )?;
        claim_type_export(
            &mut generated_types,
            &mut public_types,
            source.message.name.rust_module(),
            &format!(
                "generated struct module for {}",
                source.message.name.protocol()
            ),
        )?;
    }

    for (symbol, producer) in [
        ("API_DESCRIPTORS", "fixed API-pair registry"),
        ("MESSAGE_DESCRIPTORS", "fixed directional-message registry"),
        (
            "request_header_version",
            "fixed request-header policy function",
        ),
        (
            "response_header_version",
            "fixed response-header policy function",
        ),
    ] {
        claim_value_export(&mut generated_values, &mut public_values, symbol, producer)?;
    }
    Ok(())
}

fn validate_message_module(source: &MessageSource) -> Result<(), GenerationError> {
    let message = &source.message;
    let namespace = format!(
        "the generated `{}` module type namespace",
        message.name.rust_module()
    );
    let mut claimed = BTreeMap::new();
    claim(
        &mut claimed,
        &namespace,
        message.name.rust_type(),
        &format!("message {}", message.name.protocol()),
    )?;
    for declaration in message.structs.declarations() {
        claim(
            &mut claimed,
            &namespace,
            declaration.name.rust_type(),
            &format!(
                "nested struct {} declared {} by {}",
                declaration.name.declared(),
                declaration.origin.describe(),
                declaration.name.owner()
            ),
        )?;
    }
    validate_synthesized_members(source)?;
    Ok(())
}

fn claim_message_exports(
    source: &MessageSource,
    generated_types: &mut BTreeMap<String, String>,
    generated_values: &mut BTreeMap<String, String>,
    public_types: &mut BTreeMap<String, String>,
    public_values: &mut BTreeMap<String, String>,
) -> Result<(), GenerationError> {
    let protocol = source.message.name.protocol();
    claim_type_export(
        generated_types,
        public_types,
        source.message.name.rust_type(),
        &format!("message {protocol}"),
    )?;
    claim_type_export(
        generated_types,
        public_types,
        source.message.name.rust_module(),
        &format!("generated struct module for {protocol}"),
    )?;
    claim_value_export(
        generated_values,
        public_values,
        &descriptor_name(&source.message),
        &format!("descriptor for message {protocol}"),
    )
}

fn claim_type_export(
    generated: &mut BTreeMap<String, String>,
    public: &mut BTreeMap<String, String>,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    claim(generated, GENERATED_TYPE_NAMESPACE, symbol, producer)?;
    claim(public, PUBLIC_TYPE_NAMESPACE, symbol, producer)
}

fn claim_value_export(
    generated: &mut BTreeMap<String, String>,
    public: &mut BTreeMap<String, String>,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    claim(generated, GENERATED_VALUE_NAMESPACE, symbol, producer)?;
    claim(public, PUBLIC_VALUE_NAMESPACE, symbol, producer)
}

fn claim(
    claimed: &mut BTreeMap<String, String>,
    namespace: &str,
    symbol: &str,
    producer: &str,
) -> Result<(), GenerationError> {
    if let Some(first) = claimed.get(symbol) {
        return Err(GenerationError::GeneratedSymbolCollision {
            namespace: namespace.to_owned(),
            symbol: symbol.to_owned(),
            first: first.clone(),
            second: producer.to_owned(),
        });
    }
    claimed.insert(symbol.to_owned(), producer.to_owned());
    Ok(())
}

fn handwritten_public_types() -> BTreeMap<String, String> {
    let mut claimed = [
        "ApiDescriptor",
        "KafkaMessage",
        "KafkaRequest",
        "KafkaResponse",
        "MessageDescriptor",
        "MessageDirection",
        "OutboundFrameLimits",
        "ProtocolEq",
        "RequestResponsePair",
    ]
    .into_iter()
    .map(|symbol| (symbol.to_owned(), "handwritten crate facade".to_owned()))
    .collect::<BTreeMap<_, _>>();
    for module in ["descriptor", "frame", "generated", "message"] {
        claimed.insert(
            module.to_owned(),
            "handwritten private crate-root module".to_owned(),
        );
    }
    claimed
}

fn handwritten_public_values() -> BTreeMap<String, String> {
    ["encode_request", "response_header_version_for"]
        .into_iter()
        .map(|symbol| (symbol.to_owned(), "handwritten crate facade".to_owned()))
        .collect()
}
