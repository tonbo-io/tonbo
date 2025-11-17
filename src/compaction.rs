//! Lightweight compaction orchestrators and planners.
//!
//! These helpers sit on top of the in-memory staging surfaces and decide when
//! to drain immutable runs into on-disk SSTables.

/// Leveled compaction planning helpers.
pub mod planner;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow_array::RecordBatch;
use fusio::executor::{Executor, Timer};

use crate::{
    db::DB,
    key::KeyOwned,
    mode::Mode,
    ondisk::sstable::{SsTable, SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
};

/// Naïve minor-compaction driver that flushes once a segment threshold is hit.
pub struct MinorCompactor {
    segment_threshold: usize,
    target_level: usize,
    next_id: AtomicU64,
}

impl MinorCompactor {
    /// Build a compactor that flushes after `segment_threshold` immutable runs.
    pub fn new(segment_threshold: usize, target_level: usize, start_id: u64) -> Self {
        Self {
            segment_threshold: segment_threshold.max(1),
            target_level,
            next_id: AtomicU64::new(start_id),
        }
    }

    /// Threshold configured for flushing.
    pub fn segment_threshold(&self) -> usize {
        self.segment_threshold
    }

    /// Target level applied to generated descriptors.
    pub fn target_level(&self) -> usize {
        self.target_level
    }

    fn next_descriptor(&self) -> SsTableDescriptor {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        SsTableDescriptor::new(SsTableId::new(id), self.target_level)
    }

    /// Flush immutables when the threshold is met, returning the new SST on success.
    pub async fn maybe_compact<M, E>(
        &self,
        db: &mut DB<M, E>,
        config: Arc<SsTableConfig>,
    ) -> Result<Option<SsTable<M>>, SsTableError>
    where
        M: Mode<ImmLayout = RecordBatch, Key = KeyOwned> + Sized,
        E: Executor + Timer + Send + Sync,
    {
        if db.num_immutable_segments() < self.segment_threshold {
            return Ok(None);
        }
        let descriptor = self.next_descriptor();
        db.flush_immutables_with_descriptor(config, descriptor)
            .await
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{disk::LocalFs, dynamic::DynFs, executor::BlockingExecutor, path::Path};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::MinorCompactor;
    use crate::{db::DB, mode::DynMode, ondisk::sstable::SsTableConfig, test_util::build_batch};

    fn build_db() -> (Arc<SsTableConfig>, DB<DynMode, BlockingExecutor>) {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = crate::schema::SchemaBuilder::from_schema(schema)
            .primary_key("id")
            .with_metadata()
            .build()
            .expect("key field");
        let schema = Arc::clone(&config.schema);
        let executor = Arc::new(BlockingExecutor);
        let db = DB::<DynMode, BlockingExecutor>::builder(config)
            .in_memory("compaction-test")
            .build_with_executor(Arc::clone(&executor))
            .expect("db init");

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            fs,
            Path::from("/tmp/tonbo-compaction-test"),
        ));
        (cfg, db)
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future)
    }

    #[test]
    fn below_threshold_noop() {
        let (cfg, mut db) = build_db();
        let compactor = MinorCompactor::new(2, 0, 7);
        let result = block_on(compactor.maybe_compact(&mut db, cfg));
        assert!(matches!(result, Ok(None)));
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn threshold_met_invokes_flush() {
        let (cfg, mut db) = build_db();
        db.set_seal_policy(Box::new(crate::inmem::policy::BatchesThreshold {
            batches: 1,
        }));
        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch = build_batch(cfg.schema().clone(), rows).expect("batch");
        block_on(db.ingest(batch)).expect("ingest");
        assert_eq!(db.num_immutable_segments(), 1);

        let compactor = MinorCompactor::new(1, 0, 9);
        let table = block_on(compactor.maybe_compact(&mut db, cfg))
            .expect("flush result")
            .expect("sstable");
        assert_eq!(db.num_immutable_segments(), 0);
        let descriptor = table.descriptor();
        assert_eq!(descriptor.id().raw(), 9);
        assert_eq!(descriptor.level(), 0);
        assert_eq!(descriptor.stats().map(|s| s.rows), Some(1));
        let stats = descriptor.stats().expect("descriptor stats");
        assert_eq!(stats.rows, 1);
        assert!(stats.bytes > 0);
    }
}
