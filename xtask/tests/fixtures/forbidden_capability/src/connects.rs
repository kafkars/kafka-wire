//! A fixture module reaching for networking below a root that forbids it.

use std::net::TcpStream;

pub fn connect(address: &str) -> std::io::Result<TcpStream> {
    TcpStream::connect(address)
}
