//! API-module rendering facade.

mod codec;
mod descriptor;
mod file;
mod message;

pub(crate) use descriptor::descriptor_name;
pub(crate) use file::render_api;
