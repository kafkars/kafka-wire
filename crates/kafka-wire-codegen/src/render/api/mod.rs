//! API-module rendering facade.

mod codec;
mod descriptor;
mod file;
mod message;
mod prose;
mod structs;

pub(crate) use descriptor::descriptor_name;
pub(crate) use file::render_api;
