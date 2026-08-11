//! A fixture module using an owned capability from outside its owner file.

use std::fs;

pub fn read_anything(path: &str) -> std::io::Result<String> {
    fs::read_to_string(path)
}
