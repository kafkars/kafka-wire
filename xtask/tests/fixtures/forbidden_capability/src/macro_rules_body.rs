//! Networking hidden in a `macro_rules!` body, which is not parseable as Rust.
//!
//! The token stream here never parses as an expression or a statement, so the
//! resolver falls back to a conservative scan of the rendered tokens.

macro_rules! dial {
    ($address:expr) => {
        std::net::TcpStream::connect($address)
    };
}

pub fn connect(address: &str) -> std::io::Result<()> {
    dial!(address).map(drop)
}
