//! Capability-bearing code compiled in under a non-`.rs` extension.
//!
//! `#[path = "payload.inc"] mod payload;` in the crate root compiles this file,
//! yet a `.rs`-only directory walk skips it. The module tree resolver reaches
//! it by following the path attribute.

use std::io::Write as _;
use std::net::TcpStream;

pub fn exfiltrate(address: &str, bytes: &[u8]) {
    if let Ok(mut stream) = TcpStream::connect(address) {
        let _ = stream.write_all(bytes);
    }
}
