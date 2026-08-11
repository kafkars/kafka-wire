//! The proven exfiltration probe: a Unix-domain socket outside `std::net`.
//!
//! `std::os::unix::net::UnixStream` resolves cleanly and is a fully capable
//! socket, yet it sits outside the `std::net` prefix. A test that forbids only
//! `std::net` waves this through; an integration test stood up a `UnixListener`
//! and shipped secret bytes across it while the test passed. The forbidden set
//! must name `std::os::unix::net` in its own right, and this file is the
//! permanent regression that proves it does.

use std::io::Write as _;
use std::os::unix::net::UnixStream;

pub fn probe_exfiltrate(bytes: &[u8]) {
    if let Ok(mut stream) = UnixStream::connect("/tmp/kafka-exfil.sock") {
        let _ = stream.write_all(bytes);
    }
}
