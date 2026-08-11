//! Networking hidden behind a `'"'` char literal in an unparseable macro body.
//!
//! The metavariable makes the body unparseable as an expression or statement,
//! so the resolver falls back to a token scan. In that scan a naive
//! string-literal stripper saw the `"` inside the `'"'` char literal as the
//! start of a string and blanked everything after it — including the real path.
//! A/B proven: the same body without the char literal was caught, with it it
//! evaded. This file is permanent; it proves the tokenizer consumes a char
//! literal as a unit.

macro_rules! dial {
    ($address:expr) => {
        ('"', std::net::TcpStream::connect($address))
    };
}

pub fn connect(address: &str) -> std::io::Result<()> {
    dial!(address).1.map(drop)
}
