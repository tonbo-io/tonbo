//! Naïve minor-compaction driver for flushing immutable memtables.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use fusio::executor::{Executor, Timer};

use crate::{
    db::DbInner,
    manifest::ManifestFs,
    ondisk::sstable::{SsTable, SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
};

/// Naïve minor-compaction driver that flushes once a segment threshold is hit.
pub struct MinorCompactor {
    segment_threshold: usize,
    target_level: usize,
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    fn next_descriptor(&self) -> SsTableDescriptor {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        SsTableDescriptor::new(SsTableId::new(id), self.target_level)
    }

    /// Flush immutables when the threshold is met, returning the new SST on success.
    ///
    /// This method is exposed publicly via `test-helpers` feature for testing.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn maybe_compact<FS, E>(
        &self,
        db: &mut DbInner<FS, E>,
        config: Arc<SsTableConfig>,
    ) -> Result<Option<SsTable>, SsTableError>
    where
        FS: ManifestFs<E>,
        E: Executor + Timer + Clone,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        self.maybe_compact_inner(db, config).await
    }

    /// Internal implementation of maybe_compact.
    #[allow(dead_code)]
    pub(crate) async fn maybe_compact_inner<FS, E>(
        &self,
        db: &mut DbInner<FS, E>,
        config: Arc<SsTableConfig>,
    ) -> Result<Option<SsTable>, SsTableError>
    where
        FS: ManifestFs<E>,
        E: Executor + Timer + Clone,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        disk::LocalFs, dynamic::DynFs, executor::NoopExecutor, mem::fs::InMemoryFs, path::Path,
    };
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::MinorCompactor;
    use crate::{
        db::{DB, DbInner},
        ondisk::sstable::SsTableConfig,
        test::build_batch,
    };

    async fn build_db() -> (Arc<SsTableConfig>, DbInner<InMemoryFs, NoopExecutor>) {
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
        let executor = Arc::new(NoopExecutor);
        let db = DB::<InMemoryFs, NoopExecutor>::builder(config)
            .in_memory("compaction-test")
            .expect("in_memory config")
            .open_with_executor(Arc::clone(&executor))
            .await
            .expect("db init")
            .into_inner();

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            fs,
            Path::from("/tmp/tonbo-compaction-test"),
        ));
        (cfg, db)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn below_threshold_noop() {
        let (cfg, mut db) = build_db().await;
        let compactor = MinorCompactor::new(2, 0, 7);
        let result = compactor.maybe_compact(&mut db, cfg).await;
        assert!(matches!(result, Ok(None)));
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn threshold_met_invokes_flush() {
        let (cfg, mut db) = build_db().await;
        db.set_seal_policy(Arc::new(crate::inmem::policy::BatchesThreshold {
            batches: 1,
        }));
        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch = build_batch(cfg.schema().clone(), rows).expect("batch");
        db.ingest(batch).await.expect("ingest");
        assert_eq!(db.num_immutable_segments(), 1);

        let compactor = MinorCompactor::new(1, 0, 9);
        let table = compactor
            .maybe_compact(&mut db, cfg)
            .await
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
