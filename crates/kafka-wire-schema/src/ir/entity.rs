//! The domain entity a protocol field's value names.
//!
//! This file owns the closed `entityType` vocabulary and the diagnostic raised
//! for a spelling it does not know. It deliberately does not own what a
//! consumer does with the answer: request routing, authorization checks, and
//! client-side validation policy all live above the schema front end.

use std::{fmt, str::FromStr};

use thiserror::Error;

/// The domain entity named by a field's value.
///
/// Upstream annotates fields with `entityType` so a reader can tell a bare
/// `int32` that happens to be a broker id from one that is a partition index,
/// and a `string` that names a topic from one that carries an error message.
/// That distinction is the difference between a client that can route a request
/// to the right broker and one that guesses from field names.
///
/// The vocabulary is closed on purpose. A new upstream spelling is a new
/// protocol concept that downstream code may need to act on, so it fails loudly
/// during lowering rather than arriving as an opaque string every consumer has
/// to re-parse and re-decide about.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EntityType {
    /// The value names a topic.
    TopicName,
    /// The value identifies a broker.
    BrokerId,
    /// The value names a consumer, share, or streams group.
    GroupId,
    /// The value identifies a producer session.
    ProducerId,
    /// The value names a transaction.
    TransactionalId,
}

impl EntityType {
    /// Returns the upstream spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopicName => "topicName",
            Self::BrokerId => "brokerId",
            Self::GroupId => "groupId",
            Self::ProducerId => "producerId",
            Self::TransactionalId => "transactionalId",
        }
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for EntityType {
    type Err = EntityTypeParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "topicName" => Ok(Self::TopicName),
            "brokerId" => Ok(Self::BrokerId),
            "groupId" => Ok(Self::GroupId),
            "producerId" => Ok(Self::ProducerId),
            "transactionalId" => Ok(Self::TransactionalId),
            other => Err(EntityTypeParseError {
                spelling: other.to_owned(),
            }),
        }
    }
}

/// An `entityType` spelling this adapter does not model.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error(
    "unknown entityType `{spelling}`: upstream introduced a domain entity this \
     adapter does not model yet"
)]
pub struct EntityTypeParseError {
    /// The unmodeled upstream spelling.
    pub spelling: String,
}
