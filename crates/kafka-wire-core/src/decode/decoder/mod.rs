//! Decoder facade: cursor ownership with focused primitive, string, array, and tag domains.

mod array;
mod bytes;
mod core;
mod primitive;
mod string;
mod tagged;
mod uuid;
mod varint;

pub use core::Decoder;
