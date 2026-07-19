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
    AddRaftVoterRequest, AddRaftVoterResponse, ApiVersionsRequest, DeleteGroupsRequest,
    DeleteGroupsResponse, KafkaMessage, KafkaRequest, KafkaResponse, MessageDirection,
    OffsetDeleteRequest, OffsetDeleteResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaDecode, KafkaEncode, VersionRange};

use crate::corpus::Vector;
use crate::json_value::{
    self, Fields, api_versions_request, sasl_handshake_request, sasl_handshake_response,
};

/// Every message the corpus can judge, as one arm per generated type.
///
/// The arms are declared once here and expanded into the enum and into the
/// decode, encode, facts, and flexibility dispatches below. Adding an enabled
/// message is therefore a single line rather than five parallel edits that can
/// disagree with one another.
macro_rules! subjects {
    ($mac:ident) => {
        $mac! {
            ApiVersionsRequest => Request,
            SaslHandshakeRequest => Request,
            SaslHandshakeResponse => Response,
            OffsetDeleteRequest => Request,
            OffsetDeleteResponse => Response,
            DeleteGroupsRequest => Request,
            DeleteGroupsResponse => Response,
            AddRaftVoterRequest => Request,
            AddRaftVoterResponse => Response,
        }
    };
}

macro_rules! declare_subject {
    ($($name:ident => $direction:ident,)*) => {
        /// One generated message, held as the concrete type the vector names.
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub enum Subject {
            $(
                #[doc = concat!("`", stringify!($name), "` body.")]
                $name($name),
            )*
        }
    };
}
subjects!(declare_subject);

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
macro_rules! declare_facts {
    ($($name:ident => $direction:ident,)*) => {
        /// Report what this repository believes about `message`.
        pub fn facts(message: &str) -> Result<Facts, String> {
            match message {
                $(
                    stringify!($name) => Ok(Facts {
                        api_key: direction_api_key!($name, $direction),
                        direction: MessageDirection::$direction,
                        supported_versions: $name::SUPPORTED_VERSIONS,
                    }),
                )*
                _ => Err(unknown(message)),
            }
        }
    };
}

/// The API key constant lives on a different trait per direction.
macro_rules! direction_api_key {
    ($name:ident, Request) => {
        <$name as KafkaRequest>::API_KEY.value()
    };
    ($name:ident, Response) => {
        <$name as KafkaResponse>::API_KEY.value()
    };
}

subjects!(declare_facts);

/// Report whether this repository encodes `message` flexibly at `version`.
macro_rules! declare_is_flexible {
    ($($name:ident => $direction:ident,)*) => {
        /// Report whether this repository encodes `message` flexibly at `version`.
        pub fn is_flexible(message: &str, version: i16) -> Result<bool, String> {
            let version = ApiVersion::new(version);
            match message {
                $(stringify!($name) => Ok($name::is_flexible(version)),)*
                _ => Err(unknown(message)),
            }
        }
    };
}

subjects!(declare_is_flexible);

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
        macro_rules! declare_decode {
            ($($name:ident => $direction:ident,)*) => {
                match message {
                    $(
                        stringify!($name) => {
                            $name::decode_from_bytes(bytes, version, limits).map(Self::$name)
                        }
                    )*
                    other => return Err(unknown(other)),
                }
            };
        }
        subjects!(declare_decode).map_err(|error| error.to_string())
    }

    /// Encode this message at `version`.
    pub fn encode(&self, version: i16) -> Result<Bytes, String> {
        let version = ApiVersion::new(version);
        macro_rules! declare_encode {
            ($($name:ident => $direction:ident,)*) => {
                match self {
                    $(Self::$name(message) => message.encode_to_bytes(version),)*
                }
            };
        }
        subjects!(declare_encode).map_err(|error| error.to_string())
    }
}

fn unknown(message: &str) -> String {
    format!("no generated Rust type for message `{message}`")
}
