//! Lightweight compaction orchestrators and planners.
//!
//! These helpers sit on top of the in-memory staging surfaces and decide when
//! to drain immutable runs into on-disk SSTables.

/// Compaction executor interfaces.
pub mod executor;
/// Leveled compaction planning helpers.
pub mod planner;
/// Scheduler scaffolding for background/remote compaction.
pub mod scheduler;

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_array::RecordBatch;
use fusio::executor::{Executor, Timer};
use tokio::{task::spawn_local, time::Duration};

use crate::{
    compaction::{
        executor::{CompactionError, CompactionExecutor, CompactionOutcome},
        planner::CompactionPlanner,
    },
    db::DB,
    key::KeyOwned,
    mode::Mode,
    ondisk::sstable::{SsTable, SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
};

/// Minimal capability required by the background compaction loop.
pub(crate) trait CompactionHost<M: Mode, E: Executor + Timer> {
    fn compact_once<'a, CE, P>(
        &'a self,
        planner: &'a P,
        executor: &'a CE,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>>
    where
        CE: CompactionExecutor + 'a,
        P: CompactionPlanner + 'a;
}

/// Naïve minor-compaction driver that flushes once a segment threshold is hit.
pub struct MinorCompactor {
    segment_threshold: usize,
    target_level: usize,
    next_id: AtomicU64,
}

/// Drive compaction in a simple loop, waiting on a Tokio `interval` between attempts.
/// Callers should run this in their runtime/task and handle cancellation externally.
#[allow(dead_code)]
pub(crate) async fn compaction_loop<M, E, CE, P>(
    db: Weak<impl CompactionHost<M, E> + 'static>,
    planner: P,
    executor: CE,
    _gc_sst_config: Option<Arc<SsTableConfig>>,
    interval: Duration,
    budget: usize,
) where
    M: Mode,
    E: Executor + Timer,
    CE: CompactionExecutor,
    P: CompactionPlanner,
{
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let Some(db_strong) = db.upgrade() else {
            break;
        };
        for _ in 0..budget.max(1) {
            match db_strong.compact_once(&planner, &executor).await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(err) => {
                    eprintln!("compaction loop iteration failed: {err}");
                    break;
                }
            }
        }
        // GC consumption deferred to follow-up; skip draining GC plans in this loop.
    }
}

/// Spawn a non-Send compaction loop on the current-thread Tokio runtime.
/// This is a stopgap to keep compaction running until a proper scheduler/lease lands.
/// Callers are responsible for choosing an interval and handling cancellation via the returned
/// handle.
#[allow(dead_code, private_bounds)]
pub(crate) fn spawn_compaction_loop_local<M, E, CE, P>(
    db: Weak<impl CompactionHost<M, E> + 'static>,
    planner: P,
    executor: CE,
    gc_sst_config: Option<Arc<SsTableConfig>>,
    interval: Duration,
    budget: usize,
) -> tokio::task::JoinHandle<()>
where
    M: Mode + 'static,
    E: Executor + Timer + 'static,
    CE: CompactionExecutor + 'static,
    P: CompactionPlanner + 'static,
{
    spawn_local(async move {
        compaction_loop(db, planner, executor, gc_sst_config, interval, budget).await;
    })
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
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        disk::LocalFs,
        dynamic::DynFs,
        executor::{BlockingExecutor, tokio::TokioExecutor},
        path::Path,
    };
    use tokio::{
        task::LocalSet,
        time::{Duration, sleep},
    };
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::MinorCompactor;
    use crate::{
        compaction::{
            CompactionHost,
            executor::{CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome},
            planner::{CompactionPlanner, CompactionSnapshot, CompactionTask},
            spawn_compaction_loop_local,
        },
        db::DB,
        mode::DynMode,
        ondisk::sstable::{SsTableConfig, SsTableDescriptor},
        test_util::build_batch,
    };

    async fn build_db() -> (Arc<SsTableConfig>, DB<DynMode, BlockingExecutor>) {
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
            .await
            .expect("db init");

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
        db.set_seal_policy(Box::new(crate::inmem::policy::BatchesThreshold {
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

    #[derive(Clone)]
    struct CountingHost {
        hits: Arc<AtomicUsize>,
    }

    impl CountingHost {
        fn new() -> Self {
            Self {
                hits: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    impl CompactionHost<DynMode, TokioExecutor> for CountingHost {
        fn compact_once<'a, CE, P>(
            &'a self,
            _planner: &'a P,
            _executor: &'a CE,
        ) -> Pin<Box<dyn Future<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>>
        where
            CE: CompactionExecutor + 'a,
            P: CompactionPlanner + 'a,
        {
            let hits = Arc::clone(&self.hits);
            Box::pin(async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            })
        }
    }

    #[derive(Clone)]
    struct NoopPlanner;

    impl CompactionPlanner for NoopPlanner {
        fn plan(&self, _snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
            None
        }
    }

    #[derive(Clone)]
    struct NoopExecutor;

    impl CompactionExecutor for NoopExecutor {
        fn execute(
            &self,
            _job: CompactionJob,
        ) -> Pin<Box<dyn Future<Output = Result<CompactionOutcome, CompactionError>> + Send + '_>>
        {
            Box::pin(async { Err(CompactionError::Unimplemented) })
        }

        fn cleanup_outputs<'a>(
            &'a self,
            _outputs: &'a [SsTableDescriptor],
        ) -> Pin<Box<dyn Future<Output = Result<(), CompactionError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compaction_loop_retains_driver_and_upgrades_weak() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let host = Arc::new(CountingHost::new());
                let weak = Arc::downgrade(&host);
                let planner = NoopPlanner;
                let executor = NoopExecutor;
                let handle = spawn_compaction_loop_local::<
                    DynMode,
                    TokioExecutor,
                    NoopExecutor,
                    NoopPlanner,
                >(
                    weak, planner, executor, None, Duration::from_millis(10), 1
                );

                struct Guard {
                    _host: Arc<CountingHost>,
                    handle: tokio::task::JoinHandle<()>,
                }

                let guard = Guard {
                    _host: Arc::clone(&host),
                    handle,
                };

                drop(host);
                sleep(Duration::from_millis(30)).await;
                guard.handle.abort();
                assert!(
                    guard._host.hits() > 0,
                    "background compaction loop should upgrade the weak ref and run at least once"
                );
            })
            .await;
    }
}
