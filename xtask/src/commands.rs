//! Command orchestration across generator and executable architecture check crates.

use std::process::Command as Process;

use kafka_wire_codegen::{GenerationMode, GeneratorConfig};

use crate::{cli::Command, workspace};

pub(crate) fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Generate => generate(GenerationMode::Write),
        Command::GeneratedCheck => generate(GenerationMode::Check),
        Command::Verify => {
            generate(GenerationMode::Check)?;
            cargo(&["test", "-p", "xtask"])
        }
        Command::Doctor => doctor(),
    }
}

fn generate(mode: GenerationMode) -> Result<(), String> {
    let config = GeneratorConfig::new(workspace::root(), mode);
    let report = kafka_wire_codegen::generate(&config).map_err(|error| error.to_string())?;
    match mode {
        GenerationMode::Write => println!(
            "generated protocol: {} written, {} unchanged, {} removed",
            report.written, report.unchanged, report.removed
        ),
        GenerationMode::Check => println!(
            "generated protocol is current: {} files verified",
            report.unchanged
        ),
    }
    Ok(())
}

fn cargo(arguments: &[&str]) -> Result<(), String> {
    let status = Process::new("cargo")
        .args(arguments)
        .current_dir(workspace::root())
        .status()
        .map_err(|error| format!("could not launch cargo: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "`cargo {}` exited with {status}",
            arguments.join(" ")
        ))
    }
}

fn doctor() -> Result<(), String> {
    let workspace = workspace::root();
    let identity = kafka_wire_codegen::protocol_identity(&workspace)
        .map_err(|error| format!("read pinned protocol identity: {error}"))?;

    println!("workspace: {}", workspace.display());
    println!("upstream:  {}", identity.repository);
    println!("commit:    {}", identity.commit);
    println!("IR:        {}", identity.ir_version);
    println!("sources:   {}", identity.source_files);
    println!("generate:  cargo xtask generate");
    println!("verify:    cargo xtask verify");
    println!("full CI:   just check");
    Ok(())
}
