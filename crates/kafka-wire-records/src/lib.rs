//! Kafka `RecordBatch` v2: the container a `records` field carries.
//!
//! A `records` field in `Produce` or `Fetch` is a length-prefixed blob, and
//! `kafka-wire` carries it as exactly that. What sits inside it is this
//! crate's concern: a batch header, a CRC over everything after it, and a run of
//! varint-framed records that may be compressed as one unit.
//!
//! This crate sits BESIDE `kafka-wire` rather than beneath it. A batch
//! references no API message — messages reference batches — so putting it
//! underneath would drag compression codecs into the graph of a sans-I/O wire
//! kernel, and putting it above would force all 94 generated modules on a caller
//! that only wants to read a log segment. It depends on `kafka-wire-core` for the
//! zigzag varints and bounded reads it already owns, and on nothing else of this
//! repository's.

mod attributes;
mod batch;
mod error;
mod record;

pub use attributes::{Compression, TimestampType};
pub use batch::{MAGIC_V2, RecordBatch};
pub use error::RecordError;
pub use record::{Record, RecordHeader};
