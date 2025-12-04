//! Lightweight compaction orchestrators and planners.
//!
//! These helpers sit on top of the in-memory staging surfaces and decide when
//! to drain immutable runs into on-disk SSTables.

/// Compaction executor interfaces.
pub mod executor;
/// Leveled compaction planning helpers.
pub mod planner;
/// Scheduler scaffolding for background/remote compaction (native builds only for now).
#[cfg(feature = "tokio-runtime")]
pub mod scheduler;
/// Stateless trigger helpers intended for cron/HTTP surfaces.
pub mod trigger;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[cfg(feature = "tokio-runtime")]
use std::time::Duration;
#[cfg(feature = "tokio-runtime")]
use std::{future::Future, pin::Pin, sync::Weak};

use arrow_array::RecordBatch;
use fusio::executor::{Executor, Timer};

#[cfg(feature = "tokio-runtime")]
use crate::compaction::{
    executor::{CompactionError, CompactionExecutor, CompactionOutcome},
    planner::CompactionPlanner,
};
use crate::{
    db::DB,
    key::KeyOwned,
    mode::Mode,
    ondisk::sstable::{SsTable, SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
};

/// Minimal capability required by the background compaction loop.
#[cfg(feature = "tokio-runtime")]
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

/// Drive compaction in a simple loop, waiting on a Tokio interval between attempts.
/// Callers should run this in their runtime/task and handle cancellation externally.
#[cfg(feature = "tokio-runtime")]
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
#[cfg(feature = "tokio-runtime")]
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
    tokio::task::spawn_local(async move {
        compaction_loop::<M, E, CE, P>(db, planner, executor, gc_sst_config, interval, budget)
            .await;
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
        E: Executor + Timer,
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

#[cfg(all(test, feature = "tokio-runtime"))]
mod tests {
    use std::{
        fs,
        future::Future,
        path::PathBuf,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        disk::LocalFs,
        dynamic::MaybeSendFuture,
        executor::{NoopExecutor, tokio::TokioExecutor},
        path::Path,
    };
    use tokio::{
        task::LocalSet,
        time::{Duration, sleep},
    };

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
        ondisk::sstable::SsTableConfig,
    };

    #[derive(Default)]
    struct NeverCompact {
        attempts: AtomicUsize,
    }

    impl CompactionPlanner for NeverCompact {
        fn plan(&self, _snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    #[derive(Clone, Default)]
    struct NoopCompactionExecutor;

    impl CompactionExecutor for NoopCompactionExecutor {
        fn execute(
            &self,
            _job: CompactionJob,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
        {
            Box::pin(async { Err(CompactionError::Unimplemented) })
        }

        fn cleanup_outputs<'a>(
            &'a self,
            _outputs: &'a [crate::ondisk::sstable::SsTableDescriptor],
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone)]
    struct CountExecutor {
        count: Arc<AtomicUsize>,
    }

    impl Default for CountExecutor {
        fn default() -> Self {
            Self {
                count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl CountExecutor {
        fn count(&self) -> usize {
            self.count.load(Ordering::Relaxed)
        }
    }

    impl CompactionExecutor for CountExecutor {
        fn execute(
            &self,
            _job: CompactionJob,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
        {
            self.count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Err(CompactionError::Unimplemented) })
        }

        fn cleanup_outputs<'a>(
            &'a self,
            _outputs: &'a [crate::ondisk::sstable::SsTableDescriptor],
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minor_compactor_skips_when_below_threshold() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let mode_config = crate::mode::DynModeConfig::new(schema.clone(), extractor).unwrap();
        let executor = Arc::new(TokioExecutor::default());
        let mut db: DB<DynMode, TokioExecutor> =
            DB::new(mode_config, executor).await.expect("db init");
        let compactor = MinorCompactor::new(2, 0, 0);

        let tmp_root: PathBuf = std::env::temp_dir().join("tonbo-compaction-test");
        let _ = fs::create_dir_all(&tmp_root);
        let fusio_path = Path::from_filesystem_path(&tmp_root).expect("tmp path");
        let cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            Arc::new(LocalFs {}),
            fusio_path,
        ));
        let res = compactor.maybe_compact(&mut db, cfg).await.unwrap();
        assert!(res.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compaction_loop_stops_when_host_dropped() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let planner = NeverCompact::default();
                let executor = NoopCompactionExecutor::default();
                let (_tx, rx) = tokio::sync::watch::channel(());
                let db = Arc::new(TestHost::default());
                let weak = Arc::downgrade(&db);
                let handle = spawn_compaction_loop_local(
                    weak,
                    planner,
                    executor,
                    None,
                    Duration::from_millis(10),
                    1,
                );
                drop(db);
                // Allow loop to observe drop
                sleep(Duration::from_millis(30)).await;
                handle.abort();
                let _ = rx; // silence unused warning in cfg gates
            })
            .await;
    }

    #[derive(Default)]
    struct TestHost {
        called: AtomicUsize,
    }

    impl CompactionHost<DynMode, NoopExecutor> for TestHost {
        fn compact_once<'a, CE, P>(
            &'a self,
            _planner: &'a P,
            executor: &'a CE,
        ) -> Pin<Box<dyn Future<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>>
        where
            CE: CompactionExecutor + 'a,
            P: CompactionPlanner + 'a,
        {
            self.called.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                // Kick the executor once to exercise scheduling paths; ignore outcome.
                let job = CompactionJob {
                    task: CompactionTask {
                        source_level: 0,
                        target_level: 0,
                        input: Vec::new(),
                        key_range: None,
                    },
                    inputs: Vec::new(),
                    lease: None,
                };
                let _ = executor.execute(job).await;
                Ok(None)
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_loop_triggers_compaction_attempts() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let planner = NeverCompact::default();
                let executor = CountExecutor::default();
                let host = Arc::new(TestHost::default());
                let weak = Arc::downgrade(&host);
                let handle = spawn_compaction_loop_local(
                    weak,
                    planner,
                    executor.clone(),
                    None,
                    Duration::from_millis(5),
                    2,
                );
                sleep(Duration::from_millis(20)).await;
                handle.abort();
                assert!(executor.count() > 0);
            })
            .await;
    }
}
