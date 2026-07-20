//! Shared access to the two files under `spec/records/`.
//!
//! `vectors.json` holds batches Apache Kafka wrote; `verified.json` holds what
//! Kafka read back from batches this repository wrote. Three test files now
//! reach for one or both, so the mechanics of finding and parsing them live
//! here once.
//!
//! It deliberately owns no assertion. What a test concludes from a batch belongs
//! in the file that names the conclusion.

// Each test binary compiles this module separately, so whatever one of them does
// not reach is dead to that build alone.
#![allow(dead_code, unused_imports)]

mod batches;
mod verified;

pub(crate) use batches::{Batch, batches};
pub(crate) use verified::{ReadBatch, ReadHeader, ReadRecord, Reading, Verified, verified};
