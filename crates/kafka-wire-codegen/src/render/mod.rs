//! Deterministic IR-to-Rust file rendering.

mod api;
mod field;
mod module_file;
mod registry;
mod text;

pub(crate) use api::render_api;
pub(crate) use module_file::render_module_file;
pub(crate) use registry::render_registry;
