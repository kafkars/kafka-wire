//! Deterministic IR-to-Rust file rendering.

mod api;
mod exports;
mod field;
mod fuzz_dispatch;
mod header_version;
mod invariant;
mod module_file;
mod registry;
mod tag_boundaries;
mod tag_claims;
mod text;
mod verification;

#[cfg(test)]
mod fuzz_dispatch_test;
#[cfg(test)]
mod tag_boundaries_test;
#[cfg(test)]
mod tag_claims_test;
#[cfg(test)]
mod text_test;

pub(crate) use api::{
    api_descriptor_name, declared_structs, descriptor_name, render_api, render_unkeyed,
};
pub(crate) use exports::render_exports_file;
pub(crate) use fuzz_dispatch::render_fuzz_dispatch;
pub(crate) use header_version::render_header_version;
pub(crate) use module_file::render_module_file;
pub(crate) use registry::render_registry;
pub(crate) use tag_boundaries::render_tag_boundaries;
pub(crate) use tag_claims::render_tag_claims;
pub(crate) use verification::render_verification_files;
