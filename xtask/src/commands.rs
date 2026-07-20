//! Command orchestration across generator and executable architecture check crates.

use std::process::Command as Process;

use kafka_wire_codegen::{GenerationMode, GeneratorConfig};

use crate::{
    cli::{Command, CorpusMode},
    probe, vectors, vendor, workspace,
};

pub(crate) fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Vendor => vendor(),
        Command::Generate => generate(GenerationMode::Write),
        Command::GeneratedCheck => generate(GenerationMode::Check),
        Command::GenerateAll(CorpusMode::CheckOnly) => generate_all(),
        Command::Verify => {
            generate(GenerationMode::Check)?;
            cargo(&["test", "-p", "xtask"])
        }
        Command::Vectors(mode) => vectors::run(mode),
        Command::Records(mode) => crate::records::run(mode),
        Command::Doctor => doctor(),
    }
}

fn vendor() -> Result<(), String> {
    let report = vendor::vendor(&workspace::root())?;
    println!(
        "vendored upstream corpus: {} files ({} enabled, {} pending)",
        report.vendored, report.enabled, report.pending
    );
    for removed in &report.removed {
        println!("removed no-longer-upstream schema: {removed}");
    }
    Ok(())
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

/// Renders the whole pinned corpus under `target/` and compiles it.
///
/// The report is printed before `cargo check` runs, because the two answers are
/// independent: a schema the backend cannot render never reaches the compiler,
/// and a schema it renders may still emit Rust that does not build. Both are
/// work queues, and a failure in the second must not hide the first.
fn generate_all() -> Result<(), String> {
    let probe = probe::render(&workspace::root())?;

    println!(
        "rendered {} of {} pinned schemas into {} file(s), \
         counting the module facade and registry",
        probe.rendered,
        probe.rendered + probe.refused,
        probe.files
    );
    if probe.refused > 0 {
        println!("{} schema(s) were not rendered:", probe.refused);
        for (reason, files) in &probe.taxonomy {
            println!("  {:>3}  {reason}", files.len());
        }
    }

    println!(
        "compiling the probe crate at {}",
        probe.crate_root.display()
    );
    cargo_in(&probe.crate_root, &["check", "--quiet"])
}

fn cargo(arguments: &[&str]) -> Result<(), String> {
    cargo_in(&workspace::root(), arguments)
}

fn cargo_in(directory: &std::path::Path, arguments: &[&str]) -> Result<(), String> {
    let status = Process::new("cargo")
        .args(arguments)
        .current_dir(directory)
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
    println!(
        "pinned:    {} vendored message files",
        identity.source_files
    );
    println!("enabled:   {} compiled into Rust", identity.enabled_files);
    println!("vendor:    cargo xtask vendor");
    println!("generate:  cargo xtask generate");
    println!("verify:    cargo xtask verify");
    println!("full CI:   just check");
    Ok(())
}
