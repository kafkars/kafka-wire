//! Shared access to the broker-authored record-batch corpus.
//!
//! This module owns reading `spec/records/vectors.json`, which two test files
//! now need: one holds `kafka-wire-records` to those bytes, the other carries them
//! through a `records` field to prove the two crates compose.
//!
//! It deliberately owns no assertion. What a test concludes from a batch belongs
//! in the file that names the conclusion.

// Each test binary compiles this module separately, so whatever one of them does
// not reach is dead to that build alone.
#![allow(dead_code, unused_imports)]

mod batches;

pub(crate) use batches::{Batch, batches};
