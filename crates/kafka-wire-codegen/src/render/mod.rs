//! Deterministic IR-to-Rust file rendering.

mod api;
mod exports;
mod field;
mod header_version;
mod invariant;
mod module_file;
mod registry;
mod text;

pub(crate) use api::{render_api, render_unkeyed};
pub(crate) use exports::render_exports_file;
pub(crate) use header_version::render_header_version;
pub(crate) use module_file::render_module_file;
pub(crate) use registry::render_registry;
