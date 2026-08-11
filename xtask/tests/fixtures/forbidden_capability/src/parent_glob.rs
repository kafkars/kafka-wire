//! Networking reached through a parent glob, leaving the child segment bare.
//!
//! `use std::*;` records only `std` as glob evidence, which does not match the
//! forbidden `std::net`. The capability is then written as a bare
//! `net::TcpStream`, whose head `net` matches no import. Resolving that head
//! against the recorded glob prefix reconstructs `std::net::TcpStream` and ties
//! it back to the forbidden capability. This file is permanent.

use std::*;

pub fn connect(address: &str) -> io::Result<net::TcpStream> {
    net::TcpStream::connect(address)
}
