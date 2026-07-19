//! Exit handling for the explicit repository command surface.

mod cli;
mod commands;
mod workspace;

use std::process::ExitCode;

fn main() -> ExitCode {
    match cli::Command::parse(std::env::args().skip(1)).and_then(commands::run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}
