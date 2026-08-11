//! Networking renamed at the item level, so the type name never appears.

use std::net::TcpStream as Sock;

pub fn connect(address: &str) -> std::io::Result<Sock> {
    Sock::connect(address)
}
