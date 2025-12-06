#![cfg(feature = "tokio")]

use std::{sync::Arc, time::Duration};

use arrow_schema::{DataType, Field, Schema};
use fusio::{executor::tokio::TokioExecutor, mem::fs::InMemoryFs, path::Path};
use tokio::{task::LocalSet, time::sleep};
use tonbo::{
    compaction::planner::CompactionStrategy, db::DB, ondisk::sstable::SsTableConfig,
    schema::SchemaBuilder,
};

#[tokio::test(flavor = "current_thread")]
async fn compaction_loop_is_spawned_when_configured() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
            let cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
                .primary_key("id")
                .build()
                .expect("schema");

            let fs = Arc::new(InMemoryFs::default());
            let root = Path::parse("compaction").expect("path");
            let sst_cfg = Arc::new(SsTableConfig::new(Arc::clone(&schema), fs, root));

            let db = DB::<InMemoryFs, TokioExecutor>::builder(cfg)
                .in_memory("compaction-loop")
                .expect("in_memory config")
                .with_compaction_strategy(CompactionStrategy::default())
                .with_compaction_loop(Duration::from_millis(5), Arc::clone(&sst_cfg), 1)
                .build()
                .await
                .expect("build");

            assert!(
                db.has_compaction_worker(),
                "compaction loop should be spawned when requested"
            );

            // Let the loop tick at least once before dropping.
            sleep(Duration::from_millis(10)).await;
        })
        .await;
}
