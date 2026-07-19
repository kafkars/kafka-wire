//! Shared access to the pinned upstream corpus and its reviewed exceptions.
//!
//! This module owns the mechanics every corpus-wide test repeats: finding the
//! one vendored commit tree, listing its schema files, and loading the accepted
//! upstream defects that parameterize a strict front-end run.
//!
//! It deliberately owns no assertion. What a test concludes from the corpus —
//! coverage, census, name uniqueness — belongs in the file that names that
//! conclusion, so a reader never has to open this module to learn what is being
//! proven.

#![allow(dead_code, unused_imports)]

mod corpus;

pub(crate) use corpus::{corpus_root, exceptions, schema_files};
