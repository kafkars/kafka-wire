//! Decoder facade: cursor ownership with focused primitive, string, array, and tag domains.

mod array;
mod core;
mod primitive;
mod string;
mod tagged;

pub use core::Decoder;
