//! A file the parser cannot read, which must fail closed rather than pass.
//!
//! Substring matching "worked" on any byte sequence, so a file that does not
//! parse looked clean. Path resolution can prove nothing about it, and a test
//! that cannot read a file must not vouch for it.

pub fn truncated(address: &str) -> {
    std::net::TcpStream::connect(
