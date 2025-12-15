//! Component benchmarks entry point (Phase 1: memtable insert via ingest).

use std::path::PathBuf;

use clap::Parser;

mod components;
mod harness;

use harness::{BenchConfig, Workload};

#[derive(Debug, Parser)]
struct Args {
    /// Path to YAML config.
    #[arg(long)]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = parse_args();
    let config = BenchConfig::from_yaml_file(&args.config)?;
    let workload = Workload::from_config(&config)
        .ok_or_else(|| anyhow::anyhow!("unsupported workload: {}", config.workload.name))?;

    if config.runtime.concurrency != 1 {
        anyhow::bail!("concurrency > 1 not implemented in Phase 1");
    }

    components::memtable::run_memtable_bench(&config, &workload)?;
    Ok(())
}

fn parse_args() -> Args {
    let filtered: Vec<String> = std::env::args().filter(|arg| arg != "--bench").collect();
    Args::parse_from(filtered)
}
