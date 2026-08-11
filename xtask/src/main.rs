//! Exit handling for the explicit repository command surface.

mod cli;
mod commands;
mod defaults;
mod fetch;
mod probe;
mod probe_fixtures;
mod protocol_lock;
mod records;
mod upstream_name;
mod vectors;
mod vendor;
mod vendor_cleanup;
mod vendor_transaction;
mod vendor_verification;
mod workspace;

#[cfg(test)]
mod cli_test;
#[cfg(test)]
mod probe_test;
#[cfg(test)]
mod protocol_lock_test;
#[cfg(test)]
mod upstream_name_test;
#[cfg(test)]
mod vectors_check_test;
#[cfg(test)]
mod vendor_test;
#[cfg(test)]
mod vendor_transaction_test;

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
