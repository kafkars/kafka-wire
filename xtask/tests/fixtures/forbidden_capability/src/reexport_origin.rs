//! Networking laundered through a re-export inside the ruled root.
//!
//! The re-export itself is the capability acquisition, and it must be rejected
//! here regardless of which sibling module later consumes the name.

pub use std::net::TcpStream;
