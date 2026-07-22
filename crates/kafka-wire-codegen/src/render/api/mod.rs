//! API-module rendering facade.

mod codec;
mod declarations;
mod descriptor;
mod file;
mod header;
mod imports;
mod message;
mod prose;
mod protocol_eq;
mod structs;
mod symbol;
mod tagged;
mod tagged_payload;
mod tagged_proof;
mod tagged_validation;
mod validation;

#[cfg(test)]
mod codec_test;
#[cfg(test)]
mod imports_test;
#[cfg(test)]
mod metadata_test;
#[cfg(test)]
mod structs_test;
#[cfg(test)]
mod tagged_proof_test;

pub(crate) use declarations::declared_structs;
pub(crate) use descriptor::{api_descriptor_name, descriptor_name};
pub(crate) use file::render_api;
pub(crate) use header::render_unkeyed;
pub(crate) use imports::{ExternalSymbol, spell};
