//! Field-rendering facade: validation, types, version gates, and codec expressions.

mod codec;
mod types;
mod validate;
mod version;

#[cfg(test)]
mod probe;

#[cfg(test)]
mod codec_test;
#[cfg(test)]
mod types_test;
#[cfg(test)]
mod validate_test;
#[cfg(test)]
mod version_test;

pub(crate) use codec::{
    array_length_codec, element_codec, is_nullable, read_expression, write_statement,
};
pub(crate) use types::{
    default_expression, non_default_condition, rust_type, uses_rust_default, uses_type,
};
pub(crate) use validate::validate_supported;
pub(crate) use version::presence_condition;
