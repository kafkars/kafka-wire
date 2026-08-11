//! A fixture module that touches no capability at all.

pub fn encoded_len(value: u32) -> usize {
    value.to_be_bytes().len()
}
