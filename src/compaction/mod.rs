//! Lightweight compaction orchestrators and planners.
//!
//! These helpers sit on top of the in-memory staging surfaces and decide when
//! to drain immutable runs into on-disk SSTables.

/// Compaction executor interfaces.
pub mod executor;
/// Leveled compaction planning helpers.
pub mod planner;
/// Scheduler scaffolding for background/remote compaction (native builds only for now).
pub mod scheduler;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[cfg(test)]
use std::{pin::Pin, sync::Weak, time::Duration};

use arrow_array::RecordBatch;
use fusio::executor::{Executor, Timer};
#[cfg(test)]
use fusio::{
    dynamic::{MaybeSend, MaybeSendFuture, MaybeSync},
    executor::JoinHandle,
};
#[cfg(test)]
use futures::future::{AbortHandle, AbortRegistration, Abortable};

#[cfg(test)]
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
#[cfg(test)]
pub(crate) trait CompactionHost<M: Mode, E: Executor + Timer + Clone> {
    fn compact_once<'a, CE, P>(
        &'a self,
        planner: &'a P,
        executor: &'a CE,
    ) -> Pin<
        Box<dyn MaybeSendFuture<Output = Result<Option<CompactionOutcome>, CompactionError>> + 'a>,
    >
    where
        CE: CompactionExecutor + MaybeSend + MaybeSync + 'a,
        P: CompactionPlanner + MaybeSend + MaybeSync + 'a;
}

/// Naïve minor-compaction driver that flushes once a segment threshold is hit.
pub struct MinorCompactor {
    segment_threshold: usize,
    target_level: usize,
    next_id: AtomicU64,
}

#[cfg(test)]
pub(crate) struct CompactionLoopHandle<E: Executor> {
    abort: AbortHandle,
    handle: Option<E::JoinHandle<()>>,
}

#[cfg(test)]
impl<E: Executor> CompactionLoopHandle<E> {
    // Currently only used by tests/manual cleanup; runtime cancellation relies on Drop.
    pub async fn abort(mut self) {
        self.abort.abort();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join().await;
        }
    }
}

#[cfg(test)]
impl<E: Executor> Drop for CompactionLoopHandle<E> {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Drive compaction in a simple loop, waiting on the provided timer between attempts.
/// Callers should run this in their runtime/task and handle cancellation externally.
#[cfg(test)]
pub(crate) async fn compaction_loop<M, E, CE, P>(
    db: Weak<impl CompactionHost<M, E> + MaybeSend + MaybeSync + 'static>,
    planner: P,
    executor: CE,
    timer: Arc<E>,
    _gc_sst_config: Option<Arc<SsTableConfig>>,
    interval: Duration,
    budget: usize,
) where
    M: Mode + MaybeSend + MaybeSync,
    E: Executor + Timer + Clone,
    CE: CompactionExecutor + MaybeSend + MaybeSync,
    P: CompactionPlanner + MaybeSend + MaybeSync,
{
    loop {
        timer.sleep(interval).await;
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

/// Spawn a non-Send compaction loop on the provided executor/timer.
/// This is a stopgap to keep compaction running until a proper scheduler/lease lands.
/// Callers are responsible for choosing an interval and handling cancellation via the returned
/// handle.
#[cfg(test)]
pub(crate) fn spawn_compaction_loop_local<M, E, CE, P>(
    db: Weak<impl CompactionHost<M, E> + MaybeSend + MaybeSync + 'static>,
    planner: P,
    executor: CE,
    timer: Arc<E>,
    gc_sst_config: Option<Arc<SsTableConfig>>,
    interval: Duration,
    budget: usize,
) -> CompactionLoopHandle<E>
where
    M: Mode + MaybeSend + MaybeSync + 'static,
    E: Executor + Timer + Clone + 'static,
    CE: CompactionExecutor + MaybeSend + MaybeSync + 'static,
    P: CompactionPlanner + MaybeSend + MaybeSync + 'static,
{
    let (abort, reg): (AbortHandle, AbortRegistration) = AbortHandle::new_pair();
    let loop_future = compaction_loop::<M, E, CE, P>(
        db,
        planner,
        executor,
        Arc::clone(&timer),
        gc_sst_config,
        interval,
        budget,
    );
    let abortable = Abortable::new(loop_future, reg);
    let handle = timer.spawn(async move {
        let _ = abortable.await;
    });
    CompactionLoopHandle {
        abort,
        handle: Some(handle),
    }
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
    pub async fn maybe_compact<M, FS, E>(
        &self,
        db: &mut DB<M, FS, E>,
        config: Arc<SsTableConfig>,
    ) -> Result<Option<SsTable<M>>, SsTableError>
    where
        M: Mode<ImmLayout = RecordBatch, Key = KeyOwned> + Sized,
        FS: crate::manifest::ManifestFs<E>,
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
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        disk::LocalFs,
        dynamic::{DynFs, MaybeSend, MaybeSendFuture, MaybeSync},
        executor::{NoopExecutor, tokio::TokioExecutor},
        mem::fs::InMemoryFs,
        path::Path,
    };
    use tokio::time::sleep;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::{CompactionLoopHandle, MinorCompactor};
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
        test::build_batch,
    };

    async fn build_db() -> (Arc<SsTableConfig>, DB<DynMode, InMemoryFs, NoopExecutor>) {
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
        let db = DB::<DynMode, InMemoryFs, NoopExecutor>::builder(config)
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
        ) -> Pin<
            Box<
                dyn MaybeSendFuture<Output = Result<Option<CompactionOutcome>, CompactionError>>
                    + 'a,
            >,
        >
        where
            CE: CompactionExecutor + MaybeSend + MaybeSync + 'a,
            P: CompactionPlanner + MaybeSend + MaybeSync + 'a,
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
            _outputs: &'a [SsTableDescriptor],
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compaction_loop_retains_driver_and_upgrades_weak() {
        let host = Arc::new(CountingHost::new());
        let weak = Arc::downgrade(&host);
        let planner = NoopPlanner;
        let executor = NoopCompactionExecutor;
        let runtime = Arc::new(TokioExecutor::default());
        let handle = spawn_compaction_loop_local::<
            DynMode,
            TokioExecutor,
            NoopCompactionExecutor,
            NoopPlanner,
        >(
            weak,
            planner,
            executor,
            Arc::clone(&runtime),
            None,
            Duration::from_millis(10),
            1,
        );

        struct Guard {
            _host: Arc<CountingHost>,
            handle: CompactionLoopHandle<TokioExecutor>,
        }

        let guard = Guard {
            _host: Arc::clone(&host),
            handle,
        };

        drop(host);
        sleep(Duration::from_millis(30)).await;
        guard.handle.abort().await;
        assert!(
            guard._host.hits() > 0,
            "background compaction loop should upgrade the weak ref and run at least once"
        );
    }
}

#[cfg(all(test, target_arch = "wasm32", feature = "web"))]
mod wasm_tests {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use fusio::{
        dynamic::{MaybeSend, MaybeSendFuture, MaybeSync},
        executor::web::WebExecutor,
    };
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use super::CompactionLoopHandle;
    use crate::{
        compaction::{
            CompactionHost,
            executor::{CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome},
            planner::{CompactionPlanner, CompactionSnapshot, CompactionTask},
            spawn_compaction_loop_local,
        },
        mode::DynMode,
        ondisk::sstable::SsTableDescriptor,
    };

    wasm_bindgen_test_configure!(run_in_browser);

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

    impl CompactionHost<DynMode, WebExecutor> for CountingHost {
        fn compact_once<'a, CE, P>(
            &'a self,
            _planner: &'a P,
            _executor: &'a CE,
        ) -> Pin<
            Box<
                dyn MaybeSendFuture<Output = Result<Option<CompactionOutcome>, CompactionError>>
                    + 'a,
            >,
        >
        where
            CE: CompactionExecutor + MaybeSend + MaybeSync + 'a,
            P: CompactionPlanner + MaybeSend + MaybeSync + 'a,
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
            _outputs: &'a [SsTableDescriptor],
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[wasm_bindgen_test]
    async fn compaction_loop_runs_on_web_executor() {
        let host = Arc::new(CountingHost::new());
        let weak = Arc::downgrade(&host);
        let planner = NoopPlanner;
        let executor = NoopCompactionExecutor;
        let runtime = Arc::new(WebExecutor::new());
        let handle = spawn_compaction_loop_local::<DynMode, WebExecutor, _, _>(
            weak,
            planner,
            executor,
            Arc::clone(&runtime),
            None,
            Duration::from_millis(10),
            1,
        );

        struct Guard {
            _host: Arc<CountingHost>,
            handle: CompactionLoopHandle<WebExecutor>,
        }

        let guard = Guard {
            _host: Arc::clone(&host),
            handle,
        };

        runtime.sleep(Duration::from_millis(30)).await;
        guard.handle.abort().await;
        assert!(
            guard._host.hits() > 0,
            "web executor compaction loop should upgrade the weak ref and tick"
        );
    }
}
