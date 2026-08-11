//! Decode limits under test in this fixture.

#[derive(Debug)]
pub struct DecodeLimits {
    pub max_bytes: usize,
}

impl DecodeLimits {
    pub fn permits(&self, length: usize) -> bool {
        length <= self.max_bytes
    }
}
