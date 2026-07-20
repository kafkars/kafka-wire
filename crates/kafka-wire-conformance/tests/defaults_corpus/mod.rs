//! Facade for the defaults-corpus readers.
//!
//! `broker_authored_defaults` reaches its two inputs through this one name. The
//! reading itself — the transcript file format and the front-end walk over the
//! vendored corpus — lives in `corpus`, so this file stays a declaration and a
//! curated re-export, as every `mod.rs` in this repository does.

mod corpus;

pub(crate) use corpus::{
    DefaultKind, FIELDS, MESSAGES, MessageDefaults, STRUCTS, StructDefaults, load_transcript,
    lower_every_message,
};
