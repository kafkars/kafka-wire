//! Networking imported wholesale, so no imported name is written down.

use std::net::*;

pub fn connect(address: &str) -> std::io::Result<TcpStream> {
    TcpStream::connect(address)
}
