//! The generated Rust message one vector is about, and what this repository
//! believes about it.
//!
//! This module owns the dispatch from a vector's upstream message name onto the
//! concrete `kafka-wire` type, owns encoding and decoding that type at a
//! stated version, and owns reading the generated descriptors back out so a
//! vector's hand-transcribed identity can be checked against them.
//!
//! It deliberately owns no field mapping — `json_value` builds a message from
//! canonical JSON — and no file access and no assertions; `corpus` reads the
//! files and the tests under `tests/` state the protocol claims.

use bytes::Bytes;
use kafka_wire::{
    ApiVersionsRequest, KafkaMessage, KafkaRequest, KafkaResponse, MessageDirection,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaDecode, KafkaEncode, VersionRange};

use crate::corpus::Vector;
use crate::json_value::{
    self, Fields, api_versions_request, sasl_handshake_request, sasl_handshake_response,
};

/// One generated message, held as the concrete type the vector names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Subject {
    /// `ApiVersions` request body.
    ApiVersionsRequest(ApiVersionsRequest),
    /// `SaslHandshake` request body.
    SaslHandshakeRequest(SaslHandshakeRequest),
    /// `SaslHandshake` response body.
    SaslHandshakeResponse(SaslHandshakeResponse),
}

/// Static protocol facts this repository generated for one message.
#[derive(Clone, Copy, Debug)]
pub struct Facts {
    /// Numeric Kafka API key.
    pub api_key: i16,
    /// Request or response direction.
    pub direction: MessageDirection,
    /// Inclusive supported version range.
    pub supported_versions: VersionRange,
}

/// Report what this repository believes about `message`.
pub fn facts(message: &str) -> Result<Facts, String> {
    match message {
        "ApiVersionsRequest" => Ok(Facts {
            api_key: <ApiVersionsRequest as KafkaRequest>::API_KEY.value(),
            direction: MessageDirection::Request,
            supported_versions: ApiVersionsRequest::SUPPORTED_VERSIONS,
        }),
        "SaslHandshakeRequest" => Ok(Facts {
            api_key: <SaslHandshakeRequest as KafkaRequest>::API_KEY.value(),
            direction: MessageDirection::Request,
            supported_versions: SaslHandshakeRequest::SUPPORTED_VERSIONS,
        }),
        "SaslHandshakeResponse" => Ok(Facts {
            api_key: <SaslHandshakeResponse as KafkaResponse>::API_KEY.value(),
            direction: MessageDirection::Response,
            supported_versions: SaslHandshakeResponse::SUPPORTED_VERSIONS,
        }),
        _ => Err(unknown(message)),
    }
}

/// Report whether this repository encodes `message` flexibly at `version`.
pub fn is_flexible(message: &str, version: i16) -> Result<bool, String> {
    let version = ApiVersion::new(version);
    match message {
        "ApiVersionsRequest" => Ok(ApiVersionsRequest::is_flexible(version)),
        "SaslHandshakeRequest" => Ok(SaslHandshakeRequest::is_flexible(version)),
        "SaslHandshakeResponse" => Ok(SaslHandshakeResponse::is_flexible(version)),
        _ => Err(unknown(message)),
    }
}

impl Subject {
    /// Build the message a vector describes from its canonical JSON value.
    pub fn from_vector(vector: &Vector) -> Result<Self, String> {
        let mut fields = Fields::new(&vector.name, &vector.json_value)?;
        let subject = match vector.message.as_str() {
            "ApiVersionsRequest" => {
                Self::ApiVersionsRequest(api_versions_request(&mut fields, vector)?)
            }
            "SaslHandshakeRequest" => {
                Self::SaslHandshakeRequest(sasl_handshake_request(&mut fields)?)
            }
            "SaslHandshakeResponse" => {
                Self::SaslHandshakeResponse(sasl_handshake_response(&mut fields)?)
            }
            other => return Err(unknown(other)),
        };

        json_value::Fields::finish(fields)?;
        Ok(subject)
    }

    /// Decode one message body, rejecting trailing bytes.
    pub fn decode(message: &str, version: i16, bytes: Bytes) -> Result<Self, String> {
        let version = ApiVersion::new(version);
        let limits = DecodeLimits::default();
        match message {
            "ApiVersionsRequest" => ApiVersionsRequest::decode_from_bytes(bytes, version, limits)
                .map(Self::ApiVersionsRequest),
            "SaslHandshakeRequest" => {
                SaslHandshakeRequest::decode_from_bytes(bytes, version, limits)
                    .map(Self::SaslHandshakeRequest)
            }
            "SaslHandshakeResponse" => {
                SaslHandshakeResponse::decode_from_bytes(bytes, version, limits)
                    .map(Self::SaslHandshakeResponse)
            }
            other => return Err(unknown(other)),
        }
        .map_err(|error| error.to_string())
    }

    /// Encode this message at `version`.
    pub fn encode(&self, version: i16) -> Result<Bytes, String> {
        let version = ApiVersion::new(version);
        match self {
            Self::ApiVersionsRequest(message) => message.encode_to_bytes(version),
            Self::SaslHandshakeRequest(message) => message.encode_to_bytes(version),
            Self::SaslHandshakeResponse(message) => message.encode_to_bytes(version),
        }
        .map_err(|error| error.to_string())
    }
}

fn unknown(message: &str) -> String {
    format!("no generated Rust type for message `{message}`")
}
