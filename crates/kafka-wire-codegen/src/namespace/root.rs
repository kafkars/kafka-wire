//! Handwritten crate-root claims reserved before generation.

use std::collections::BTreeMap;

const PRIVATE_ROOT_MODULE: &str = "handwritten private crate-root module";

pub(crate) fn handwritten_root_types() -> BTreeMap<String, String> {
    [
        ("ApiDescriptor", "handwritten crate facade"),
        ("KafkaMessage", "handwritten crate facade"),
        ("KafkaRequest", "handwritten crate facade"),
        ("KafkaResponse", "handwritten crate facade"),
        ("MessageDescriptor", "handwritten crate facade"),
        ("MessageDirection", "handwritten crate facade"),
        ("OutboundFrameLimits", "handwritten crate facade"),
        ("ProtocolEq", "handwritten crate facade"),
        ("RequestFrameMeasure", "handwritten crate facade"),
        ("RequestResponsePair", "handwritten crate facade"),
        ("RetainedFootprint", "handwritten crate facade"),
        ("RetainedSize", "handwritten crate facade"),
        ("consumer_protocol", PRIVATE_ROOT_MODULE),
        ("consumer_protocol_test", PRIVATE_ROOT_MODULE),
        ("descriptor", PRIVATE_ROOT_MODULE),
        ("frame", PRIVATE_ROOT_MODULE),
        ("generated", PRIVATE_ROOT_MODULE),
        ("message", PRIVATE_ROOT_MODULE),
        ("retained", PRIVATE_ROOT_MODULE),
        ("retained_test", PRIVATE_ROOT_MODULE),
        ("tagged_claims_test", PRIVATE_ROOT_MODULE),
    ]
    .into_iter()
    .map(|(symbol, producer)| (symbol.to_owned(), producer.to_owned()))
    .collect()
}

pub(crate) fn handwritten_root_values() -> BTreeMap<String, String> {
    [
        "decode_consumer_protocol_assignment",
        "decode_consumer_protocol_subscription",
        "encode_consumer_protocol_assignment",
        "encode_consumer_protocol_subscription",
        "encode_request",
        "measure_request",
        "response_header_version_for",
    ]
    .into_iter()
    .map(|symbol| (symbol.to_owned(), "handwritten crate facade".to_owned()))
    .collect()
}
