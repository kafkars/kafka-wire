//! The consumer half of the re-export laundering case.
//!
//! Deliberately NOT expected to be rejected on its own: nothing in this file
//! names `std`. Resolving `crate::reexport_origin::TcpStream` would require
//! reading a second file, which the path resolver does not do. The laundering
//! is still closed, because the re-export must live inside the ruled root to be
//! reachable, and `reexport_origin.rs` is rejected there.

use crate::reexport_origin::TcpStream;

pub fn connect(address: &str) -> std::io::Result<TcpStream> {
    TcpStream::connect(address)
}
