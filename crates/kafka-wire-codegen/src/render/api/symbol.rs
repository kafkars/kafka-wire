//! Closed vocabulary for every external symbol emitted into message Rust.
//!
//! This file owns origin and collision-proof spelling. It deliberately knows
//! no schema traversal or import-block layout.

/// One compiler-owned symbol emitted into a generated message module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExternalSymbol {
    ApiDescriptor,
    ApiKey,
    ApiVersion,
    Bytes,
    BytesMut,
    DecodeError,
    Decoder,
    EncodeError,
    EncodeTarget,
    Encoder,
    KafkaDecode,
    KafkaEncode,
    KnownTags,
    StrBytes,
    TagOutcome,
    TaggedFields,
    TaggedFieldsError,
    Uuid,
    VersionRange,
    EncodeIntoWith,
    EncodedLenWith,
    KafkaMessage,
    MessageDescriptor,
    MessageDirection,
    KafkaRequest,
    KafkaResponse,
    ProtocolEq,
    RequestResponsePair,
    Result,
    Option,
    Vec,
    Default,
    Ok,
    Err,
    Some,
    None,
}

impl ExternalSymbol {
    /// Every symbol introduced through a generated `use` declaration.
    pub(super) const IMPORTABLE: &'static [Self] = &[
        Self::ApiDescriptor,
        Self::ApiKey,
        Self::ApiVersion,
        Self::Bytes,
        Self::BytesMut,
        Self::DecodeError,
        Self::Decoder,
        Self::EncodeError,
        Self::EncodeTarget,
        Self::Encoder,
        Self::KafkaDecode,
        Self::KafkaEncode,
        Self::KnownTags,
        Self::StrBytes,
        Self::TagOutcome,
        Self::TaggedFields,
        Self::TaggedFieldsError,
        Self::Uuid,
        Self::VersionRange,
        Self::EncodeIntoWith,
        Self::EncodedLenWith,
        Self::KafkaMessage,
        Self::MessageDescriptor,
        Self::MessageDirection,
        Self::KafkaRequest,
        Self::KafkaResponse,
        Self::ProtocolEq,
        Self::RequestResponsePair,
    ];

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::ApiDescriptor => "ApiDescriptor",
            Self::ApiKey => "ApiKey",
            Self::ApiVersion => "ApiVersion",
            Self::Bytes => "Bytes",
            Self::BytesMut => "BytesMut",
            Self::DecodeError => "DecodeError",
            Self::Decoder => "Decoder",
            Self::EncodeError => "EncodeError",
            Self::EncodeTarget => "EncodeTarget",
            Self::Encoder => "Encoder",
            Self::KafkaDecode => "KafkaDecode",
            Self::KafkaEncode => "KafkaEncode",
            Self::KnownTags => "KnownTags",
            Self::StrBytes => "StrBytes",
            Self::TagOutcome => "TagOutcome",
            Self::TaggedFields => "TaggedFields",
            Self::TaggedFieldsError => "TaggedFieldsError",
            Self::Uuid => "Uuid",
            Self::VersionRange => "VersionRange",
            Self::EncodeIntoWith => "encode_into_with",
            Self::EncodedLenWith => "encoded_len_with",
            Self::KafkaMessage => "KafkaMessage",
            Self::MessageDescriptor => "MessageDescriptor",
            Self::MessageDirection => "MessageDirection",
            Self::KafkaRequest => "KafkaRequest",
            Self::KafkaResponse => "KafkaResponse",
            Self::ProtocolEq => "ProtocolEq",
            Self::RequestResponsePair => "RequestResponsePair",
            Self::Result => "Result",
            Self::Option => "Option",
            Self::Vec => "Vec",
            Self::Default => "Default",
            Self::Ok => "Ok",
            Self::Err => "Err",
            Self::Some => "Some",
            Self::None => "None",
        }
    }

    pub(super) const fn origin(self) -> Option<&'static str> {
        match self {
            Self::ApiDescriptor
            | Self::KafkaMessage
            | Self::MessageDescriptor
            | Self::MessageDirection
            | Self::KafkaRequest
            | Self::KafkaResponse
            | Self::ProtocolEq
            | Self::RequestResponsePair => Some("crate"),
            Self::ApiKey
            | Self::ApiVersion
            | Self::Bytes
            | Self::BytesMut
            | Self::DecodeError
            | Self::Decoder
            | Self::EncodeError
            | Self::EncodeTarget
            | Self::Encoder
            | Self::KafkaDecode
            | Self::KafkaEncode
            | Self::KnownTags
            | Self::StrBytes
            | Self::TagOutcome
            | Self::TaggedFields
            | Self::TaggedFieldsError
            | Self::Uuid
            | Self::VersionRange
            | Self::EncodeIntoWith
            | Self::EncodedLenWith => Some("kafka_wire_core"),
            Self::Result
            | Self::Option
            | Self::Vec
            | Self::Default
            | Self::Ok
            | Self::Err
            | Self::Some
            | Self::None => None,
        }
    }

    pub(super) const fn absolute(self) -> Option<&'static str> {
        match self {
            Self::Result => Some("::core::result::Result"),
            Self::Option => Some("::core::option::Option"),
            Self::Vec => Some("::std::vec::Vec"),
            Self::Default => Some("::core::default::Default"),
            Self::Ok => Some("::core::result::Result::Ok"),
            Self::Err => Some("::core::result::Result::Err"),
            Self::Some => Some("::core::option::Option::Some"),
            Self::None => Some("::core::option::Option::None"),
            _ => None,
        }
    }
}
