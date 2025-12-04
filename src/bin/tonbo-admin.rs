//! Minimal admin CLI entrypoint for stateless compaction triggers.
//! This is intentionally single-threaded and uses the existing manifest-backed idempotency
//! runtime to avoid duplicate work when `--idempotency-key` is supplied.

use std::{sync::Arc, time::Duration};

use clap::Parser;
use fusio_manifest::NoopExecutor;
use tonbo::{
    compaction::{
        executor::LocalCompactionExecutor,
        planner::{CompactionStrategy, LeveledPlannerConfig},
        trigger::TriggerRequest,
    },
    db::DB,
    mode::DynMode,
    ondisk::sstable::SsTableConfig,
    schema::SchemaBuilder,
};

#[derive(Parser, Debug)]
#[command(author, version, about = "Tonbo admin CLI")]
enum Command {
    /// Trigger a one-shot compaction using the configured planner/executor.
    Trigger {
        /// Optional owner label for observability.
        #[arg(long, default_value = "cli")]
        owner: String,
        /// Idempotency key to prevent duplicate work; when supplied, a manifest-backed CAS will
        /// no-op if a live record exists.
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Lease TTL in milliseconds (applies to idempotency record).
        #[arg(long, default_value = "30000")]
        lease_ms: u64,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cmd = Command::parse();

    match cmd {
        Command::Trigger {
            owner,
            idempotency_key,
            lease_ms,
        } => {
            // Minimal in-memory DB for demonstration; replace with real builder wiring.
            let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("v", arrow_schema::DataType::Int32, false),
            ]));
            let schema_cfg = SchemaBuilder::from_schema(arrow_schema)
                .primary_key("id")
                .build()?;
            let planner = CompactionStrategy::Leveled(LeveledPlannerConfig::default())
                .build()
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let fs = Arc::new(fusio::mem::fs::InMemoryFs::new());
            let sst_cfg = Arc::new(SsTableConfig::new(
                schema_cfg.schema().clone(),
                fs,
                fusio::path::Path::from("admin-trigger"),
            ));
            let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 1);
            let db: DB<DynMode, NoopExecutor> = DB::<DynMode, NoopExecutor>::builder(schema_cfg)
                .in_memory("admin-trigger")
                .build_with_executor(Arc::new(NoopExecutor))
                .await?;
            let request = TriggerRequest {
                owner,
                idempotency_key,
                lease_ttl: Duration::from_millis(lease_ms),
            };
            let ran = db
                .trigger_compaction_once(&planner, &executor, request, None)
                .await?
                .is_some();
            if ran {
                println!("compaction executed");
            } else {
                println!("no compaction scheduled");
            }
        }
    }

    Ok(())
}
