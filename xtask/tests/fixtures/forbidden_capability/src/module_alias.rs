//! Networking renamed at the module level, then used under the alias.

use std::net as network;

pub fn connect(address: &str) -> std::io::Result<network::TcpStream> {
    network::TcpStream::connect(address)
}
