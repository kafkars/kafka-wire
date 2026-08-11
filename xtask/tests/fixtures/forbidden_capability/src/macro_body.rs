//! Networking reached from inside a macro invocation's token stream.

pub fn connect(address: &str) {
    let _ = std::hint::black_box(std::net::TcpStream::connect(address).is_ok());
    println!("{:?}", std::net::TcpStream::connect(address).is_ok());
}
