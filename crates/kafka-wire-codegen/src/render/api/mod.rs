//! API-module rendering facade.

mod codec;
mod descriptor;
mod file;
mod header;
mod imports;
mod message;
mod prose;
mod structs;
mod tagged;

#[cfg(test)]
mod imports_test;

pub(crate) use descriptor::descriptor_name;
pub(crate) use file::render_api;
pub(crate) use header::render_unkeyed;
pub(crate) use imports::spell;
