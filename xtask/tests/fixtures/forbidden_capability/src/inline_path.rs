//! Networking reached by fully-qualified path with no `use` statement at all.

pub fn connect(address: &str) -> std::io::Result<()> {
    let stream = std::net::TcpStream::connect(address)?;
    drop(stream);
    Ok(())
}
