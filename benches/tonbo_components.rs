//! Component benchmarks entry point (Phase 1: memtable insert via ingest).

use std::path::PathBuf;

use clap::Parser;

mod components;
mod harness;

use harness::{BackendRun, BenchConfig, Workload};

#[derive(Debug, Parser)]
struct Args {
    /// Path to YAML config.
    #[arg(long)]
    config: PathBuf,
    /// Component bench selector: memtable | wal | sst | iterator | all
    #[arg(long, default_value = "all")]
    component: String,
}

fn bench_target_from_path(path: &PathBuf) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "component".to_string())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    let config = BenchConfig::from_yaml_file(&args.config)?;
    let bench_target = bench_target_from_path(&args.config);
    let workload = Workload::from_config(&config)
        .ok_or_else(|| anyhow::anyhow!("unsupported workload: {}", config.workload.name))?;

    if config.runtime.concurrency != 1 {
        anyhow::bail!("concurrency > 1 not implemented in Phase 1");
    }

    let backend = BackendRun::new(&config).await?;
    let selection = ComponentSelection::from_str(&args.component);
    if selection.memtable {
        components::memtable::run_memtable_bench(&backend, &config, &workload, &bench_target)
            .await?;
    }
    if selection.wal {
        components::wal::run_wal_bench(&backend, &config, &workload, &bench_target).await?;
    }
    if selection.sst {
        components::sst_encode::run_sst_encode_bench(&backend, &config, &workload, &bench_target)
            .await?;
    }
    if selection.iterator {
        components::iterator::run_iterator_bench(&backend, &config, &workload, &bench_target)
            .await?;
    }
    if let Err(err) = backend.cleanup().await {
        eprintln!("component backend cleanup failed: {err}");
    }
    Ok(())
}

fn parse_args() -> Args {
    let filtered: Vec<String> = std::env::args().filter(|arg| arg != "--bench").collect();
    Args::parse_from(filtered)
}

struct ComponentSelection {
    memtable: bool,
    wal: bool,
    sst: bool,
    iterator: bool,
}

impl ComponentSelection {
    fn from_str(s: &str) -> Self {
        match s {
            "memtable" => Self {
                memtable: true,
                wal: false,
                sst: false,
                iterator: false,
            },
            "wal" => Self {
                memtable: false,
                wal: true,
                sst: false,
                iterator: false,
            },
            "sst" => Self {
                memtable: false,
                wal: false,
                sst: true,
                iterator: false,
            },
            "iterator" => Self {
                memtable: false,
                wal: false,
                sst: false,
                iterator: true,
            },
            _ => Self {
                memtable: true,
                wal: true,
                sst: true,
                iterator: true,
            },
        }
    }
}
