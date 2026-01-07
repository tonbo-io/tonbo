use std::{
    collections::HashSet,
    fs,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::{
    DynFs,
    disk::LocalFs,
    dynamic::{MaybeSend, MaybeSendFuture, MaybeSync},
    executor::{Executor, Instant, NoopExecutor, Timer, tokio::TokioExecutor},
    mem::fs::InMemoryFs,
    path::Path,
};
use futures::{StreamExt, future::AbortHandle};
use tokio::sync::Mutex;
use typed_arrow_dyn::{DynCell, DynRow};

use super::common::workspace_temp_dir;
use crate::{
    compaction::{
        CompactionHandle, CompactionWorkerConfig,
        executor::{
            CompactionError, CompactionExecutor, CompactionJob, CompactionOutcome,
            LocalCompactionExecutor,
        },
        metrics::{CompactionMetrics, CompactionQueueDropContext, CompactionQueueDropReason},
        orchestrator,
        planner::{
            CompactionInput, CompactionPlanner, CompactionSnapshot, CompactionTask,
            LeveledCompactionPlanner, LeveledPlannerConfig,
        },
    },
    db::{
        BackpressureDecision, CasBackoffConfig, CascadeConfig, DB, DbInner, L0BackpressureConfig,
        L0Stats,
    },
    extractor::projection_for_columns,
    id::FileIdGenerator,
    inmem::{
        immutable::memtable::{
            DeleteSidecar, ImmutableIndexEntry, ImmutableMemTable, bundle_mvcc_sidecar,
        },
        policy::BatchesThreshold,
    },
    key::KeyTsViewRaw,
    manifest::{
        ManifestError, SstEntry, TableId, TonboManifest, VersionEdit, VersionState, WalSegmentRef,
    },
    mvcc::Timestamp,
    ondisk::sstable::{SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableId, SsTableReader},
    schema::SchemaBuilder,
    test::build_batch,
};

type SleepHook = Arc<dyn Fn(Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> + Send + Sync>;

#[derive(Clone)]
struct RecordingExecutor {
    inner: TokioExecutor,
    sleep_calls: Arc<StdMutex<Vec<Duration>>>,
    sleep_hook: Arc<StdMutex<Option<SleepHook>>>,
}

impl RecordingExecutor {
    fn new() -> Self {
        Self {
            inner: TokioExecutor::default(),
            sleep_calls: Arc::new(StdMutex::new(Vec::new())),
            sleep_hook: Arc::new(StdMutex::new(None)),
        }
    }

    fn set_hook(&self, hook: SleepHook) {
        let mut guard = self.sleep_hook.lock().expect("sleep hook lock");
        *guard = Some(hook);
    }

    fn sleep_calls(&self) -> Vec<Duration> {
        self.sleep_calls.lock().expect("sleep calls lock").clone()
    }
}

impl Executor for RecordingExecutor {
    type JoinHandle<R>
        = <TokioExecutor as Executor>::JoinHandle<R>
    where
        R: MaybeSend;

    type Mutex<T>
        = <TokioExecutor as Executor>::Mutex<T>
    where
        T: MaybeSend + MaybeSync;

    type RwLock<T>
        = <TokioExecutor as Executor>::RwLock<T>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        self.inner.spawn(future)
    }

    fn mutex<T>(value: T) -> Self::Mutex<T>
    where
        T: MaybeSend + MaybeSync,
    {
        <TokioExecutor as Executor>::mutex(value)
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        <TokioExecutor as Executor>::rw_lock(value)
    }
}

impl Timer for RecordingExecutor {
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        let calls = Arc::clone(&self.sleep_calls);
        let hook = self.sleep_hook.lock().expect("sleep hook lock").clone();
        Box::pin(async move {
            calls.lock().expect("sleep calls lock").push(dur);
            if let Some(hook) = hook {
                hook(dur).await;
            }
        })
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn system_time(&self) -> SystemTime {
        SystemTime::now()
    }
}

fn dummy_compaction_handle<E: Executor>() -> CompactionHandle<E> {
    let (abort, _reg) = AbortHandle::new_pair();
    CompactionHandle::new(abort, None, None)
}

#[test]
fn l0_backpressure_thresholds_select_actions() {
    let config = L0BackpressureConfig::new(2, 4);
    let stats = L0Stats {
        file_count: 1,
        total_bytes: Some(0),
    };
    assert!(matches!(
        config.decision(stats),
        BackpressureDecision::Proceed
    ));

    let stats = L0Stats {
        file_count: 2,
        total_bytes: Some(0),
    };
    assert!(matches!(
        config.decision(stats),
        BackpressureDecision::Slowdown(_)
    ));

    let stats = L0Stats {
        file_count: 4,
        total_bytes: Some(0),
    };
    assert!(matches!(
        config.decision(stats),
        BackpressureDecision::Stall(_)
    ));
}

#[test]
fn l0_backpressure_bytes_can_trigger() {
    let config = L0BackpressureConfig::new(10, 20)
        .slowdown_bytes(100)
        .stop_bytes(200);
    let stats = L0Stats {
        file_count: 1,
        total_bytes: Some(150),
    };
    assert!(matches!(
        config.decision(stats),
        BackpressureDecision::Slowdown(_)
    ));

    let stats = L0Stats {
        file_count: 1,
        total_bytes: Some(250),
    };
    assert!(matches!(
        config.decision(stats),
        BackpressureDecision::Stall(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn l0_backpressure_applies_slowdown_delay() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = RecordingExecutor::new();
    let mut db: DbInner<InMemoryFs, RecordingExecutor> =
        DB::new(mode_cfg, Arc::new(executor.clone()))
            .await?
            .into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    db.compaction_metrics = Some(Arc::clone(&metrics));

    db.l0_backpressure = Some(
        L0BackpressureConfig::new(1, 3)
            .slowdown_delay(Duration::from_millis(7))
            .stop_delay(Duration::from_millis(11)),
    );
    db.compaction_worker = Some(dummy_compaction_handle());

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/1.parquet"),
        None,
    );
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    db.apply_l0_backpressure().await?;

    let sleeps = executor.sleep_calls();
    assert_eq!(sleeps, vec![Duration::from_millis(7)]);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.backpressure_slowdown, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn l0_backpressure_stall_rechecks_until_reduced() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = RecordingExecutor::new();
    let mut db: DbInner<InMemoryFs, RecordingExecutor> =
        DB::new(mode_cfg, Arc::new(executor.clone()))
            .await?
            .into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    db.compaction_metrics = Some(Arc::clone(&metrics));

    db.l0_backpressure = Some(
        L0BackpressureConfig::new(1, 2)
            .slowdown_delay(Duration::from_millis(3))
            .stop_delay(Duration::from_millis(9)),
    );
    db.compaction_worker = Some(dummy_compaction_handle());

    let sst_id_a = SsTableId::new(1);
    let sst_id_b = SsTableId::new(2);
    let entry_a = SstEntry::new(
        sst_id_a.clone(),
        None,
        None,
        Path::from("L0/1.parquet"),
        None,
    );
    let entry_b = SstEntry::new(
        sst_id_b.clone(),
        None,
        None,
        Path::from("L0/2.parquet"),
        None,
    );
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry_a, entry_b],
            }],
        )
        .await?;

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let manifest = db.manifest.clone();
    let table = db.manifest_table;
    let remove_id = sst_id_b.clone();
    let hook_calls_clone = Arc::clone(&hook_calls);
    executor.set_hook(Arc::new(move |_delay| {
        let manifest = manifest.clone();
        let hook_calls = Arc::clone(&hook_calls_clone);
        let remove_id = remove_id.clone();
        Box::pin(async move {
            if hook_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                manifest
                    .apply_version_edits(
                        table,
                        &[VersionEdit::RemoveSsts {
                            level: 0,
                            sst_ids: vec![remove_id],
                        }],
                    )
                    .await
                    .expect("remove sst");
            }
        })
    }));

    db.apply_l0_backpressure().await?;

    let sleeps = executor.sleep_calls();
    assert_eq!(
        sleeps,
        vec![Duration::from_millis(9), Duration::from_millis(3)]
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.backpressure_stall, 1);
    assert_eq!(snapshot.backpressure_slowdown, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_compaction_returns_task() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = SchemaBuilder::from_schema(schema)
        .primary_key("id")
        .with_metadata()
        .build()
        .expect("schema builder");
    let schema = Arc::clone(&config.schema);
    let executor = Arc::new(TokioExecutor::default());
    let policy = Arc::new(BatchesThreshold { batches: 1 });
    let db: DbInner<InMemoryFs, TokioExecutor> =
        DB::new_with_policy(config, Arc::clone(&executor), policy)
            .await?
            .into_inner();

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(SsTableConfig::new(
        Arc::clone(&schema),
        fs,
        Path::from("/tmp/plan-compaction"),
    ));

    for pass in 0..2 {
        let rows = vec![vec![
            Some(DynCell::Str(format!("comp-{pass}").into())),
            Some(DynCell::I32(pass as i32)),
        ]];
        let batch = build_batch(Arc::clone(&schema), rows).expect("batch");
        db.ingest(batch).await.expect("ingest");
        let descriptor = SsTableDescriptor::new(SsTableId::new(pass as u64 + 1), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
            .await
            .expect("flush");
    }

    let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
        l0_trigger: 1,
        l0_max_inputs: 2,
        l0_max_bytes: None,
        level_thresholds: vec![usize::MAX],
        level_max_bytes: Vec::new(),
        max_inputs_per_task: 2,
        max_task_bytes: None,
    });
    let task = db
        .plan_compaction_task(&planner)
        .await?
        .expect("compaction task");
    assert_eq!(task.source_level, 0);
    assert_eq!(task.target_level, 1);
    assert_eq!(task.input.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_compaction_empty_manifest_is_none() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = SchemaBuilder::from_schema(schema.clone())
        .primary_key("id")
        .with_metadata()
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let db: DB<InMemoryFs, TokioExecutor> = DB::new(config, Arc::clone(&executor)).await?;
    let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig::default());
    let plan = db.inner().plan_compaction_task(&planner).await?;
    assert!(plan.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minor_compaction_flushes_after_seal() -> Result<(), Box<dyn std::error::Error>> {
    let db_root = workspace_temp_dir("minor-compaction-flush");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let config = SchemaBuilder::from_schema(schema.clone())
        .primary_key("id")
        .with_metadata()
        .build()
        .expect("schema builder");
    let batch_schema = Arc::clone(&config.schema);

    let mut db: DbInner<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(config)
        .on_disk(&db_root)?
        .with_minor_compaction(1, 0)
        .build()
        .await?
        .into_inner();
    db.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));

    let rows = vec![DynRow(vec![
        Some(DynCell::Str("k".into())),
        Some(DynCell::I32(1)),
    ])];
    let batch = build_batch(batch_schema, rows).expect("batch");
    db.ingest(batch)
        .await
        .expect("ingest triggers seal + flush");

    assert_eq!(db.num_immutable_segments(), 0);
    let sst_path = db_root
        .join("sst")
        .join("L0")
        .join("00000000000000000001.parquet");
    assert!(
        sst_path.exists(),
        "expected SST at {:?} to be created",
        sst_path
    );
    Ok(())
}

#[test]
fn wal_segments_after_compaction_preserves_manifest_when_metadata_missing() {
    let generator = FileIdGenerator::default();
    let table_id = TableId::new(&generator);
    let mut version = VersionState::empty(table_id);

    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

    let entry_missing_wal = SstEntry::new(SsTableId::new(1), None, None, Path::default(), None);
    let entry_with_wal = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_b.file_id().clone()]),
        Path::default(),
        None,
    );

    version
        .apply_edits(&[
            VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry_missing_wal, entry_with_wal],
            },
            VersionEdit::SetWalSegments {
                segments: vec![wal_a.clone(), wal_b.clone()],
            },
        ])
        .expect("apply edits");

    let wal_ids = orchestrator::wal_ids_for_remaining_ssts(&version, &HashSet::new(), &[]);
    assert!(
        wal_ids.is_none(),
        "missing wal metadata on a remaining SST should preserve the manifest wal set"
    );

    let filtered = orchestrator::wal_segments_after_compaction(&version, &[], &[]);
    assert!(
        filtered.is_none(),
        "compaction should not rewrite wal segments when metadata is absent"
    );
}

#[test]
fn wal_segments_after_compaction_filters_and_tracks_obsolete() {
    let generator = FileIdGenerator::default();
    let table_id = TableId::new(&generator);
    let mut version = VersionState::empty(table_id);

    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);
    let wal_c = WalSegmentRef::new(3, generator.generate(), 0, 0);

    let entry_a = SstEntry::new(
        SsTableId::new(1),
        None,
        Some(vec![wal_a.file_id().clone(), wal_b.file_id().clone()]),
        Path::default(),
        None,
    );
    let entry_b = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_c.file_id().clone()]),
        Path::default(),
        None,
    );

    version
        .apply_edits(&[
            VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry_a, entry_b],
            },
            VersionEdit::SetWalSegments {
                segments: vec![wal_a.clone(), wal_b.clone(), wal_c.clone()],
            },
        ])
        .expect("apply edits");

    let removed = vec![
        SsTableDescriptor::new(SsTableId::new(1), 0)
            .with_storage_paths(Path::from("L0/1.parquet"), None),
    ];
    let added = vec![SsTableDescriptor::new(SsTableId::new(3), 0)];

    assert_eq!(version.wal_segments().len(), 3);
    assert!(
        version.ssts()[0][0].wal_segments().is_some(),
        "entry should carry wal segments"
    );
    assert!(
        version.ssts()[0][1].wal_segments().is_some(),
        "second entry should carry wal segments"
    );
    let removed_ids: HashSet<SsTableId> = removed.iter().map(|d| d.id().clone()).collect();
    let wal_ids = orchestrator::wal_ids_for_remaining_ssts(&version, &removed_ids, &added);
    assert!(
        wal_ids.is_none(),
        "wal ids should be None when any SST lacks wal metadata"
    );
    let filtered = orchestrator::wal_segments_after_compaction(&version, &removed, &added);
    assert!(
        filtered.is_none(),
        "filtered wal segments should be None when any SST lacks wal metadata"
    );

    let outcome = CompactionOutcome {
        add_ssts: Vec::new(),
        remove_ssts: removed,
        target_level: 0,
        wal_segments: None,
        tombstone_watermark: None,
        outputs: added.clone(),
        obsolete_sst_ids: Vec::new(),
        wal_floor: None,
        obsolete_wal_segments: vec![wal_a.clone(), wal_b.clone()],
    };
    let plan = orchestrator::gc_plan_from_outcome(&outcome)
        .expect("gc plan")
        .expect("plan present");
    assert_eq!(plan.obsolete_wal_segments.len(), 2);
    assert!(plan.obsolete_wal_segments.contains(&wal_a));
    assert!(plan.obsolete_wal_segments.contains(&wal_b));
}

#[derive(Clone)]
struct StaticPlanner {
    task: CompactionTask,
}

impl CompactionPlanner for StaticPlanner {
    fn plan(&self, _snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        Some(self.task.clone())
    }
}

#[derive(Clone)]
struct StaticExecutor {
    outputs: Vec<SsTableDescriptor>,
    wal_segments: Vec<WalSegmentRef>,
    target_level: u32,
}

impl CompactionExecutor for StaticExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let outputs = self.outputs.clone();
        let wal_segments = self.wal_segments.clone();
        let target_level = self.target_level;
        Box::pin(async move {
            let mut outcome = CompactionOutcome::from_outputs(
                outputs.clone(),
                job.inputs.clone(),
                target_level,
                Some(wal_segments),
            )?;
            outcome.outputs = outputs;
            Ok(outcome)
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        _outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
struct NoStatsExecutor {
    output: SsTableDescriptor,
}

impl CompactionExecutor for NoStatsExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let output = self.output.clone();
        Box::pin(async move {
            CompactionOutcome::from_outputs(
                vec![output],
                job.inputs.clone(),
                job.task.target_level as u32,
                None,
            )
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        _outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
struct FailingExecutor;

impl CompactionExecutor for FailingExecutor {
    fn execute(
        &self,
        _job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        Box::pin(async { Err(CompactionError::NoInputs) })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        _outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_updates_manifest_wal_and_records_gc_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let generator = FileIdGenerator::default();
    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

    let entry_a = SstEntry::new(
        SsTableId::new(1),
        None,
        Some(vec![wal_a.file_id().clone(), wal_b.file_id().clone()]),
        Path::from("L0/1.parquet"),
        None,
    );
    let entry_b = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_b.file_id().clone()]),
        Path::from("L0/2.parquet"),
        None,
    );

    let db: DB<InMemoryFs, NoopExecutor> = DB::new(
        SchemaBuilder::from_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ])))
        .primary_key("id")
        .build()
        .expect("schema builder"),
        Arc::new(NoopExecutor),
    )
    .await
    .expect("db init");

    db.inner()
        .manifest
        .apply_version_edits(
            db.inner().manifest_table,
            &[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_a, entry_b],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ],
        )
        .await
        .expect("apply edits");

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(1),
            },
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(2),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };

    let output_desc = SsTableDescriptor::new(SsTableId::new(3), 1)
        .with_wal_ids(Some(vec![wal_b.file_id().clone()]))
        .with_storage_paths(Path::from("L1/3.parquet"), None);
    let executor = StaticExecutor {
        outputs: vec![output_desc],
        wal_segments: vec![wal_b.clone()],
        target_level: 1,
    };

    let outcome = db
        .inner()
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");

    let snapshot = db
        .inner()
        .manifest
        .snapshot_latest(db.inner().manifest_table)
        .await?;
    let latest = snapshot.latest_version.expect("latest version");
    assert_eq!(latest.wal_segments().len(), 1);
    assert_eq!(latest.wal_segments()[0].file_id(), wal_b.file_id());
    assert_eq!(
        outcome.obsolete_wal_segments,
        vec![wal_a.clone()],
        "should surface obsolete wal segments"
    );

    let plan = db
        .inner()
        .manifest
        .take_gc_plan(db.inner().manifest_table)
        .await?
        .expect("gc plan recorded");
    assert_eq!(plan.obsolete_wal_segments.len(), 1);
    assert_eq!(plan.obsolete_wal_segments[0].file_id(), wal_a.file_id());
    assert_eq!(plan.obsolete_ssts.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_preserves_manifest_wal_when_metadata_missing()
-> Result<(), Box<dyn std::error::Error>> {
    let generator = FileIdGenerator::default();
    let wal_a = WalSegmentRef::new(1, generator.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(2, generator.generate(), 0, 0);

    let entry_missing_wal = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/1.parquet"),
        None,
    );
    let entry_with_wal = SstEntry::new(
        SsTableId::new(2),
        None,
        Some(vec![wal_b.file_id().clone()]),
        Path::from("L0/2.parquet"),
        None,
    );

    let db: DB<InMemoryFs, NoopExecutor> = DB::new(
        SchemaBuilder::from_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ])))
        .primary_key("id")
        .build()
        .expect("schema builder"),
        Arc::new(NoopExecutor),
    )
    .await
    .expect("db init");

    db.inner()
        .manifest
        .apply_version_edits(
            db.inner().manifest_table,
            &[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_missing_wal, entry_with_wal],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ],
        )
        .await
        .expect("apply edits");

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(2),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };

    let output_desc = SsTableDescriptor::new(SsTableId::new(3), 1)
        .with_wal_ids(Some(vec![wal_b.file_id().clone()]))
        .with_storage_paths(Path::from("L1/3.parquet"), None);
    let executor = StaticExecutor {
        outputs: vec![output_desc],
        wal_segments: vec![wal_b.clone()],
        target_level: 1,
    };

    let outcome = db
        .inner()
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");

    let snapshot = db
        .inner()
        .manifest
        .snapshot_latest(db.inner().manifest_table)
        .await?;
    let latest = snapshot.latest_version.expect("latest version");
    assert_eq!(
        latest.wal_segments(),
        &[wal_a.clone(), wal_b.clone()],
        "manifest wal set should remain unchanged when wal metadata is missing",
    );
    assert!(
        outcome.obsolete_wal_segments.is_empty(),
        "should not surface obsolete wal segments when manifest set is preserved"
    );

    let plan = db
        .inner()
        .manifest
        .take_gc_plan(db.inner().manifest_table)
        .await?
        .expect("gc plan recorded");
    assert!(
        plan.obsolete_wal_segments.is_empty(),
        "gc plan should not mark wal segments obsolete"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_e2e_merges_and_advances_wal_floor() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-e2e-wal");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor =
        crate::extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), fs, sst_root).with_key_extractor(extractor.into()),
    );

    // Build two SSTs with WAL ids.
    let wal_gen = FileIdGenerator::default();
    let wal_a = WalSegmentRef::new(10, wal_gen.generate(), 0, 0);
    let wal_b = WalSegmentRef::new(11, wal_gen.generate(), 0, 0);

    let batch_a = build_batch(
        Arc::clone(&schema),
        vec![DynRow(vec![
            Some(DynCell::Str("a".into())),
            Some(DynCell::I32(1)),
        ])],
    )?;
    let imm_a = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_a, "id")?;
    let mut builder_a = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(1), 0),
    );
    builder_a.add_immutable(&imm_a)?;
    let sst_a = builder_a.finish(NoopExecutor).await?;
    let desc_a = sst_a
        .descriptor()
        .clone()
        .with_wal_ids(Some(vec![wal_a.file_id().clone()]));

    let batch_b = build_batch(
        Arc::clone(&schema),
        vec![DynRow(vec![
            Some(DynCell::Str("b".into())),
            Some(DynCell::I32(2)),
        ])],
    )?;
    let imm_b = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_b, "id")?;
    let mut builder_b = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(2), 0),
    );
    builder_b.add_immutable(&imm_b)?;
    let sst_b = builder_b.finish(NoopExecutor).await?;
    let desc_b = sst_b
        .descriptor()
        .clone()
        .with_wal_ids(Some(vec![wal_b.file_id().clone()]));

    // Seed manifest with the two inputs and WAL set.
    let db: DB<InMemoryFs, NoopExecutor> = DB::new(mode_cfg, Arc::new(NoopExecutor)).await?;
    let entry_a = SstEntry::new(
        desc_a.id().clone(),
        desc_a.stats().cloned(),
        desc_a.wal_ids().map(|ids| ids.to_vec()),
        desc_a
            .data_path()
            .expect("input descriptor missing data path")
            .clone(),
        desc_a.delete_path().cloned(),
    );
    let entry_b = SstEntry::new(
        desc_b.id().clone(),
        desc_b.stats().cloned(),
        desc_b.wal_ids().map(|ids| ids.to_vec()),
        desc_b
            .data_path()
            .expect("input descriptor missing data path")
            .clone(),
        desc_b.delete_path().cloned(),
    );
    db.inner()
        .manifest
        .apply_version_edits(
            db.inner().manifest_table,
            &[
                VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry_a, entry_b],
                },
                VersionEdit::SetWalSegments {
                    segments: vec![wal_a.clone(), wal_b.clone()],
                },
            ],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: desc_a.id().clone(),
            },
            CompactionInput {
                level: 0,
                sst_id: desc_b.id().clone(),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100)
        .with_max_output_bytes(8 * 1024 * 1024);

    let outcome = db
        .inner()
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");
    assert_eq!(outcome.remove_ssts.len(), 2);
    assert_eq!(outcome.add_ssts.len(), 1);
    assert_eq!(
        outcome
            .add_ssts
            .first()
            .and_then(|e| e.wal_segments())
            .map(|ids| ids.len()),
        Some(2),
        "output should aggregate wal ids"
    );
    assert_eq!(
        outcome.wal_segments.as_ref().map(|segments| segments.len()),
        Some(2),
        "manifest wal set should retain gap when inputs have discontinuity"
    );
    assert_eq!(
        outcome.wal_floor.as_ref().map(|w| w.seq()),
        Some(wal_a.seq()),
        "wal floor should reflect first retained segment when gaps exist"
    );
    assert_eq!(
        outcome.obsolete_wal_segments.len(),
        0,
        "no wal segments should be obsolete when gaps are retained"
    );

    let snapshot = db
        .inner()
        .manifest
        .snapshot_latest(db.inner().manifest_table)
        .await?;
    let latest = snapshot.latest_version.expect("latest version");
    assert_eq!(latest.wal_segments().len(), 2);
    let wal_ids: HashSet<_> = latest
        .wal_segments()
        .iter()
        .map(|seg| seg.file_id().clone())
        .collect();
    assert!(wal_ids.contains(wal_a.file_id()));
    assert!(wal_ids.contains(wal_b.file_id()));
    assert_eq!(
        latest.wal_floor().as_ref().map(|w| w.file_id()),
        Some(wal_a.file_id()),
        "wal floor should align to first retained segment"
    );

    let plan = db
        .inner()
        .manifest
        .take_gc_plan(db.inner().manifest_table)
        .await?
        .expect("gc plan recorded");
    assert_eq!(plan.obsolete_wal_segments.len(), 0);
    assert!(
        plan.obsolete_ssts.len() >= 1,
        "gc plan should record obsolete ssts"
    );

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_merges_overlap_heavy_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-overlap-heavy");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor =
        crate::extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), fs, sst_root).with_key_extractor(extractor.into()),
    );

    let seg_a = segment_with_commits(Arc::clone(&schema), vec![("k".to_string(), 1, 10, false)]);
    let seg_b = segment_with_commits(Arc::clone(&schema), vec![("k".to_string(), 2, 20, false)]);
    let seg_c = segment_with_commits(
        Arc::clone(&schema),
        vec![
            ("k".to_string(), 0, 30, true),
            ("z".to_string(), 9, 25, false),
        ],
    );

    let mut builder_a = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(1), 0),
    );
    builder_a.add_immutable(&seg_a)?;
    let sst_a = builder_a.finish(NoopExecutor).await?;

    let mut builder_b = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(2), 0),
    );
    builder_b.add_immutable(&seg_b)?;
    let sst_b = builder_b.finish(NoopExecutor).await?;

    let mut builder_c = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(3), 0),
    );
    builder_c.add_immutable(&seg_c)?;
    let sst_c = builder_c.finish(NoopExecutor).await?;

    let db: DB<InMemoryFs, NoopExecutor> = DB::new(mode_cfg, Arc::new(NoopExecutor)).await?;
    for sst in [&sst_a, &sst_b, &sst_c] {
        let desc = sst.descriptor();
        let entry = SstEntry::new(
            desc.id().clone(),
            desc.stats().cloned(),
            desc.wal_ids().map(|ids| ids.to_vec()),
            desc.data_path()
                .expect("input descriptor missing data path")
                .clone(),
            desc.delete_path().cloned(),
        );
        db.inner()
            .manifest
            .apply_version_edits(
                db.inner().manifest_table,
                &[VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry],
                }],
            )
            .await?;
    }

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(1),
            },
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(2),
            },
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(3),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100);

    let outcome = db
        .inner()
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");

    let mut data_keys = Vec::new();
    let mut data_vals = Vec::new();
    let mut delete_keys = Vec::new();
    for desc in &outcome.outputs {
        let reader = SsTableReader::open(Arc::clone(&sst_cfg), desc.clone()).await?;
        let mut stream = reader
            .into_stream(Timestamp::MAX, None, NoopExecutor)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.data.num_rows() > 0 {
                let ids = batch
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("string ids");
                let vals = batch
                    .data
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .expect("int vals");
                for i in 0..batch.data.num_rows() {
                    data_keys.push(ids.value(i).to_string());
                    data_vals.push(vals.value(i));
                }
            }
            if let Some(delete) = batch.delete.as_ref() {
                if delete.num_rows() > 0 {
                    let ids = delete
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .expect("string delete ids");
                    for i in 0..delete.num_rows() {
                        delete_keys.push(ids.value(i).to_string());
                    }
                }
            }
        }
    }

    assert_eq!(data_keys, vec!["z"]);
    assert_eq!(data_vals, vec![9]);
    assert!(delete_keys.contains(&"k".to_string()));

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_splits_outputs_by_row_cap() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-split-cap");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor =
        crate::extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), fs, sst_root).with_key_extractor(extractor.into()),
    );

    let batch_a = build_batch(
        Arc::clone(&schema),
        vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ],
    )?;
    let imm_a = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_a, "id")?;
    let mut builder_a = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(1), 0),
    );
    builder_a.add_immutable(&imm_a)?;
    let sst_a = builder_a.finish(NoopExecutor).await?;

    let batch_b = build_batch(
        Arc::clone(&schema),
        vec![DynRow(vec![
            Some(DynCell::Str("c".into())),
            Some(DynCell::I32(3)),
        ])],
    )?;
    let imm_b = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_b, "id")?;
    let mut builder_b = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(2), 0),
    );
    builder_b.add_immutable(&imm_b)?;
    let sst_b = builder_b.finish(NoopExecutor).await?;

    let db: DB<InMemoryFs, NoopExecutor> = DB::new(mode_cfg, Arc::new(NoopExecutor)).await?;
    for sst in [&sst_a, &sst_b] {
        let desc = sst.descriptor();
        let entry = SstEntry::new(
            desc.id().clone(),
            desc.stats().cloned(),
            desc.wal_ids().map(|ids| ids.to_vec()),
            desc.data_path()
                .expect("input descriptor missing data path")
                .clone(),
            desc.delete_path().cloned(),
        );
        db.inner()
            .manifest
            .apply_version_edits(
                db.inner().manifest_table,
                &[VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry],
                }],
            )
            .await?;
    }

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(1),
            },
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(2),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100).with_max_output_rows(1);

    let outcome = db
        .inner()
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");
    assert!(
        outcome.outputs.len() > 1,
        "expected output splitting with row cap"
    );
    for output in &outcome.outputs {
        let stats = output.stats().expect("output stats");
        assert!(stats.rows <= 1);
    }

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_splits_outputs_by_byte_cap() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-split-bytes");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let extractor =
        crate::extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), fs, sst_root).with_key_extractor(extractor.into()),
    );

    let batch_a = build_batch(
        Arc::clone(&schema),
        vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ],
    )?;
    let imm_a = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_a, "id")?;
    let mut builder_a = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(1), 0),
    );
    builder_a.add_immutable(&imm_a)?;
    let sst_a = builder_a.finish(NoopExecutor).await?;

    let batch_b = build_batch(
        Arc::clone(&schema),
        vec![DynRow(vec![
            Some(DynCell::Str("c".into())),
            Some(DynCell::I32(3)),
        ])],
    )?;
    let imm_b = crate::inmem::immutable::memtable::segment_from_batch_with_key_name(batch_b, "id")?;
    let mut builder_b = SsTableBuilder::new(
        Arc::clone(&sst_cfg),
        SsTableDescriptor::new(SsTableId::new(2), 0),
    );
    builder_b.add_immutable(&imm_b)?;
    let sst_b = builder_b.finish(NoopExecutor).await?;

    let db: DB<InMemoryFs, NoopExecutor> = DB::new(mode_cfg, Arc::new(NoopExecutor)).await?;
    for sst in [&sst_a, &sst_b] {
        let desc = sst.descriptor();
        let entry = SstEntry::new(
            desc.id().clone(),
            desc.stats().cloned(),
            desc.wal_ids().map(|ids| ids.to_vec()),
            desc.data_path()
                .expect("input descriptor missing data path")
                .clone(),
            desc.delete_path().cloned(),
        );
        db.inner()
            .manifest
            .apply_version_edits(
                db.inner().manifest_table,
                &[VersionEdit::AddSsts {
                    level: 0,
                    entries: vec![entry],
                }],
            )
            .await?;
    }

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(1),
            },
            CompactionInput {
                level: 0,
                sst_id: SsTableId::new(2),
            },
        ],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100).with_max_output_bytes(1);

    let outcome = db
        .inner()
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");
    assert!(
        outcome.outputs.len() > 1,
        "expected output splitting with byte cap"
    );

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[derive(Clone)]
struct ConflictingExecutor {
    inner: LocalCompactionExecutor,
    manifest: TonboManifest<InMemoryFs, TokioExecutor>,
    table: TableId,
    outputs: Arc<Mutex<Vec<SsTableDescriptor>>>,
    execute_calls: Arc<AtomicUsize>,
    cleanup_calls: Arc<AtomicUsize>,
}

impl ConflictingExecutor {
    fn new(
        inner: LocalCompactionExecutor,
        manifest: TonboManifest<InMemoryFs, TokioExecutor>,
        table: TableId,
        outputs: Arc<Mutex<Vec<SsTableDescriptor>>>,
        execute_calls: Arc<AtomicUsize>,
        cleanup_calls: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            manifest,
            table,
            outputs,
            execute_calls,
            cleanup_calls,
        }
    }
}

impl CompactionExecutor for ConflictingExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let inner = self.inner.clone();
        let manifest = self.manifest.clone();
        let table = self.table;
        let outputs = Arc::clone(&self.outputs);
        let execute_calls = Arc::clone(&self.execute_calls);
        Box::pin(async move {
            execute_calls.fetch_add(1, Ordering::SeqCst);
            let outcome = inner.execute(job).await?;
            {
                let mut guard = outputs.lock().await;
                guard.clear();
                guard.extend(outcome.outputs.iter().cloned());
            }
            manifest
                .apply_version_edits(
                    table,
                    &[VersionEdit::SetWalSegments {
                        segments: Vec::new(),
                    }],
                )
                .await
                .map_err(CompactionError::Manifest)?;
            Ok(outcome)
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        let inner = self.inner.clone();
        let cleanup_calls = Arc::clone(&self.cleanup_calls);
        Box::pin(async move {
            let result = inner.cleanup_outputs(outputs).await;
            cleanup_calls.fetch_add(1, Ordering::SeqCst);
            result
        })
    }
}

#[derive(Clone)]
struct CasConflictExecutor {
    manifest: TonboManifest<InMemoryFs, RecordingExecutor>,
    table: TableId,
    outputs: Vec<SsTableDescriptor>,
    conflict_once: Arc<AtomicUsize>,
}

impl CasConflictExecutor {
    fn new(
        manifest: TonboManifest<InMemoryFs, RecordingExecutor>,
        table: TableId,
        outputs: Vec<SsTableDescriptor>,
    ) -> Self {
        Self {
            manifest,
            table,
            outputs,
            conflict_once: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl CompactionExecutor for CasConflictExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let manifest = self.manifest.clone();
        let table = self.table;
        let outputs = self.outputs.clone();
        let conflict_once = Arc::clone(&self.conflict_once);
        Box::pin(async move {
            if conflict_once.fetch_add(1, Ordering::SeqCst) == 0 {
                manifest
                    .apply_version_edits(
                        table,
                        &[VersionEdit::SetWalSegments {
                            segments: Vec::new(),
                        }],
                    )
                    .await
                    .map_err(CompactionError::Manifest)?;
            }
            CompactionOutcome::from_outputs(
                outputs,
                job.inputs.clone(),
                job.task.target_level as u32,
                None,
            )
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        _outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_cas_conflict_cleans_outputs() -> Result<(), Box<dyn std::error::Error>> {
    let temp_root = workspace_temp_dir("compaction-cas-cleanup");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let extractor = Arc::clone(&mode_cfg.extractor);
    let schema = Arc::clone(&mode_cfg.schema);
    let executor = Arc::new(TokioExecutor::default());
    let policy = Arc::new(BatchesThreshold { batches: 1 });
    let mut db: DbInner<InMemoryFs, TokioExecutor> =
        DB::new_with_policy(mode_cfg, Arc::clone(&executor), policy)
            .await?
            .into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    db.compaction_metrics = Some(Arc::clone(&metrics));

    let sst_root = temp_root.join("sst");
    fs::create_dir_all(&sst_root)?;
    let sst_root = Path::from_filesystem_path(&sst_root)?;
    let sst_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let sst_cfg = Arc::new(
        SsTableConfig::new(Arc::clone(&schema), sst_fs.clone(), sst_root)
            .with_key_extractor(extractor),
    );

    for idx in 0..2 {
        let rows = vec![vec![
            Some(DynCell::Str(format!("ck-{idx}").into())),
            Some(DynCell::I32(idx as i32)),
        ]];
        let batch = build_batch(Arc::clone(&schema), rows)?;
        db.ingest(batch).await?;
        let descriptor = SsTableDescriptor::new(SsTableId::new(idx as u64 + 1), 0);
        db.flush_immutables_with_descriptor(Arc::clone(&sst_cfg), descriptor)
            .await?;
    }

    let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
        l0_trigger: 1,
        l0_max_inputs: 2,
        l0_max_bytes: None,
        level_thresholds: vec![usize::MAX],
        level_max_bytes: Vec::new(),
        max_inputs_per_task: 2,
        max_task_bytes: None,
    });
    let recorded_outputs = Arc::new(Mutex::new(Vec::new()));
    let execute_calls = Arc::new(AtomicUsize::new(0));
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let conflicting_executor = ConflictingExecutor::new(
        LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 100),
        db.manifest.clone(),
        db.manifest_table,
        Arc::clone(&recorded_outputs),
        Arc::clone(&execute_calls),
        Arc::clone(&cleanup_calls),
    );

    let result = db
        .run_compaction_task(&planner, &conflicting_executor)
        .await;
    match result {
        Err(CompactionError::Manifest(ManifestError::CasConflict(_)))
        | Err(CompactionError::CasConflict) => {}
        other => panic!("expected CAS conflict, got {other:?}"),
    }

    let outputs = recorded_outputs.lock().await.clone();
    assert!(
        !outputs.is_empty(),
        "compaction should have produced outputs before CAS conflict"
    );
    let execute_count = execute_calls.load(Ordering::SeqCst);
    let cleanup_count = cleanup_calls.load(Ordering::SeqCst);
    assert!(
        execute_count >= 2,
        "expected CAS retry attempts, got {execute_count}"
    );
    assert_eq!(
        cleanup_count, execute_count,
        "cleanup should run per failed attempt"
    );
    for desc in outputs {
        if let Some(path) = desc.data_path() {
            assert!(
                sst_fs.open(path).await.is_err(),
                "data file should be cleaned up"
            );
        }
        if let Some(path) = desc.delete_path() {
            assert!(
                sst_fs.open(path).await.is_err(),
                "delete file should be cleaned up"
            );
        }
    }

    let snapshot = metrics.snapshot();
    assert!(snapshot.cas_retries >= 1);
    assert_eq!(snapshot.cas_aborts, 1);
    assert_eq!(snapshot.job_failures, 1);

    fs::remove_dir_all(&temp_root)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn compaction_cas_backoff_sleeps_on_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = RecordingExecutor::new();
    let mut db: DbInner<InMemoryFs, RecordingExecutor> =
        DB::new(mode_cfg, Arc::new(executor.clone()))
            .await?
            .into_inner();

    db.cas_backoff = CasBackoffConfig::new(Duration::from_millis(5), Duration::from_millis(20));

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(1),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let output_desc = SsTableDescriptor::new(SsTableId::new(100), 1)
        .with_storage_paths(Path::from("L1/100.parquet"), None);
    let executor_with_conflict =
        CasConflictExecutor::new(db.manifest.clone(), db.manifest_table, vec![output_desc]);

    let outcome = db
        .run_compaction_task(&planner, &executor_with_conflict)
        .await?
        .expect("compaction outcome");
    assert_eq!(outcome.add_ssts.len(), 1);

    let sleeps = executor.sleep_calls();
    assert_eq!(sleeps, vec![Duration::from_millis(5)]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn compaction_executor_failure_records_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let mut db: DbInner<InMemoryFs, TokioExecutor> =
        DB::new(mode_cfg, Arc::clone(&executor)).await?.into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    db.compaction_metrics = Some(Arc::clone(&metrics));

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(1),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let err = db
        .run_compaction_task(&planner, &FailingExecutor)
        .await
        .expect_err("expected compaction failure");
    assert!(matches!(err, CompactionError::NoInputs));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.job_count, 0);
    assert_eq!(snapshot.job_failures, 1);
    assert_eq!(snapshot.cas_aborts, 0);
    assert!(snapshot.last_job.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn compaction_metrics_ignore_missing_stats() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let mut db: DbInner<InMemoryFs, TokioExecutor> =
        DB::new(mode_cfg, Arc::clone(&executor)).await?.into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    db.compaction_metrics = Some(Arc::clone(&metrics));

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    db.manifest
        .apply_version_edits(
            db.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(1),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let output_desc = SsTableDescriptor::new(SsTableId::new(99), 1)
        .with_storage_paths(Path::from("L1/099.parquet"), None);
    let executor = NoStatsExecutor {
        output: output_desc,
    };

    let outcome = db
        .run_compaction_task(&planner, &executor)
        .await?
        .expect("compaction outcome");
    assert_eq!(outcome.add_ssts.len(), 1);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.job_count, 1);
    assert_eq!(snapshot.bytes_in, 0);
    assert_eq!(snapshot.bytes_out, 0);
    assert_eq!(snapshot.rows_in, 0);
    assert_eq!(snapshot.rows_out, 0);
    assert_eq!(snapshot.tombstones_in, 0);
    assert_eq!(snapshot.tombstones_out, 0);
    let last_job = snapshot.last_job.expect("last job");
    assert!(!last_job.input.complete);
    assert!(!last_job.output.complete);
    Ok(())
}

#[derive(Clone)]
struct CascadePlanner;

impl CompactionPlanner for CascadePlanner {
    fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        let level0 = snapshot.level(0)?;
        let file = level0.files().first()?;
        Some(CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![CompactionInput {
                level: 0,
                sst_id: file.sst_id.clone(),
            }],
            key_range: None,
        })
    }

    fn plan_with_min_level(
        &self,
        snapshot: &CompactionSnapshot,
        min_level: usize,
    ) -> Option<CompactionTask> {
        if min_level > 1 {
            return None;
        }
        let level1 = snapshot.level(1)?;
        let file = level1.files().first()?;
        Some(CompactionTask {
            source_level: 1,
            target_level: 2,
            input: vec![CompactionInput {
                level: 1,
                sst_id: file.sst_id.clone(),
            }],
            key_range: None,
        })
    }
}

#[derive(Clone)]
struct CountingExecutor {
    executed: Arc<StdMutex<Vec<(usize, usize)>>>,
}

impl CountingExecutor {
    fn new(executed: Arc<StdMutex<Vec<(usize, usize)>>>) -> Self {
        Self { executed }
    }
}

impl CompactionExecutor for CountingExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<CompactionOutcome, CompactionError>> + '_>>
    {
        let executed = Arc::clone(&self.executed);
        Box::pin(async move {
            let source_level = job.task.source_level;
            let target_level = job.task.target_level;
            executed
                .lock()
                .expect("executed lock")
                .push((source_level, target_level));
            let input_id = job.inputs.first().map(|desc| desc.id().raw()).unwrap_or(0);
            let output_id = SsTableId::new(input_id + (target_level as u64 * 100));
            let output_path = Path::from(format!(
                "L{target_level}/{id}.parquet",
                id = output_id.raw()
            ));
            let output_desc = SsTableDescriptor::new(output_id, target_level)
                .with_storage_paths(output_path, None);
            CompactionOutcome::from_outputs(
                vec![output_desc],
                job.inputs.clone(),
                target_level as u32,
                None,
            )
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        _outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), CompactionError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

async fn wait_for_executions(
    executed: &Arc<StdMutex<Vec<(usize, usize)>>>,
    expected: usize,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if executed.lock().expect("executed lock").len() >= expected {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_metrics<F>(
    metrics: &Arc<CompactionMetrics>,
    timeout: Duration,
    mut predicate: F,
) -> bool
where
    F: FnMut(&crate::compaction::metrics::CompactionMetricsSnapshot) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = metrics.snapshot();
        if predicate(&snapshot) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn segment_with_commits(
    schema: SchemaRef,
    rows: Vec<(String, i32, u64, bool)>,
) -> ImmutableMemTable {
    let mut data_rows = Vec::new();
    let mut data_commits = Vec::new();
    let mut delete_rows = Vec::new();
    let mut delete_commits = Vec::new();

    for (key, value, commit, tombstone) in rows {
        let ts = Timestamp::new(commit);
        if tombstone {
            delete_rows.push(DynRow(vec![Some(DynCell::Str(key.into()))]));
            delete_commits.push(ts);
        } else {
            data_rows.push(DynRow(vec![
                Some(DynCell::Str(key.into())),
                Some(DynCell::I32(value)),
            ]));
            data_commits.push(ts);
        }
    }

    let batch = if data_rows.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        build_batch(schema.clone(), data_rows).expect("data batch")
    };
    let tombstone_flags = vec![false; batch.num_rows()];
    let (batch, mvcc) =
        bundle_mvcc_sidecar(batch, data_commits.clone(), tombstone_flags).expect("mvcc");

    let delete_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    let delete_empty = delete_rows.is_empty();
    let delete_batch = if delete_empty {
        RecordBatch::new_empty(delete_schema.clone())
    } else {
        build_batch(delete_schema.clone(), delete_rows).expect("delete batch")
    };
    let delete_sidecar = if delete_empty {
        DeleteSidecar::empty(&delete_schema)
    } else {
        DeleteSidecar::new(delete_batch, delete_commits.clone())
    };

    let extractor = crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
    let mut composite = std::collections::BTreeMap::new();
    let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
    let key_rows = extractor
        .project_view(&batch, &row_indices)
        .expect("project view");
    for (row, key_row) in key_rows.into_iter().enumerate() {
        composite.insert(
            KeyTsViewRaw::new(key_row, mvcc.commit_ts[row]),
            ImmutableIndexEntry::Row(row as u32),
        );
    }

    if !delete_sidecar.is_empty() {
        let delete_schema = delete_sidecar.key_batch().schema().clone();
        let indices: Vec<usize> = (0..delete_schema.fields().len()).collect();
        let projection =
            projection_for_columns(delete_schema.clone(), indices).expect("identity projection");
        let delete_row_indices: Vec<usize> = (0..delete_sidecar.key_batch().num_rows()).collect();
        let delete_key_rows = projection
            .project_view(delete_sidecar.key_batch(), &delete_row_indices)
            .expect("delete keys");
        for (row, key_row) in delete_key_rows.into_iter().enumerate() {
            let ts = delete_commits[row];
            composite.insert(KeyTsViewRaw::new(key_row, ts), ImmutableIndexEntry::Delete);
        }
    }

    ImmutableMemTable::new(batch, composite, mvcc, delete_sidecar)
}

#[tokio::test(flavor = "current_thread")]
async fn compaction_periodic_trigger_records_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let mut inner: DbInner<InMemoryFs, TokioExecutor> =
        DB::new(mode_cfg, Arc::clone(&executor)).await?.into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    inner.compaction_metrics = Some(Arc::clone(&metrics));

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    inner
        .manifest
        .apply_version_edits(
            inner.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(1),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executed = Arc::new(StdMutex::new(Vec::new()));
    let executor = CountingExecutor::new(Arc::clone(&executed));
    let driver = Arc::new(inner.compaction_driver());
    let worker_config = CompactionWorkerConfig::new(
        Some(Duration::from_millis(5)),
        2,
        1,
        CascadeConfig::new(0, Duration::from_millis(0)),
    );
    let handle = driver.spawn_worker(
        Arc::clone(&inner.executor),
        planner,
        executor,
        worker_config,
    );

    assert!(
        wait_for_metrics(&metrics, Duration::from_secs(1), |snapshot| {
            snapshot.trigger_periodic >= 1
        })
        .await,
        "expected periodic trigger metrics"
    );
    assert!(
        wait_for_executions(&executed, 1).await,
        "expected compaction execution"
    );
    drop(handle);

    let snapshot = metrics.snapshot();
    assert!(snapshot.trigger_periodic >= 1);
    assert_eq!(snapshot.trigger_kick, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn compaction_planner_queue_drop_records_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let metrics = CompactionMetrics::new();
    metrics.record_queue_drop(
        CompactionQueueDropContext::Planner,
        CompactionQueueDropReason::Full,
    );
    metrics.record_queue_drop(
        CompactionQueueDropContext::Planner,
        CompactionQueueDropReason::Closed,
    );
    let snapshot = metrics.snapshot();
    assert!(snapshot.queue_drops_planner_full >= 1);
    assert!(snapshot.queue_drops_planner_closed >= 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn compaction_kick_triggers_without_periodic_tick() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let mut inner: DbInner<InMemoryFs, TokioExecutor> =
        DB::new(mode_cfg, Arc::clone(&executor)).await?.into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    inner.compaction_metrics = Some(Arc::clone(&metrics));

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    inner
        .manifest
        .apply_version_edits(
            inner.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![CompactionInput {
            level: 0,
            sst_id: SsTableId::new(1),
        }],
        key_range: None,
    };
    let planner = StaticPlanner { task };
    let executed = Arc::new(StdMutex::new(Vec::new()));
    let executor = CountingExecutor::new(Arc::clone(&executed));
    let driver = Arc::new(inner.compaction_driver());
    let worker_config =
        CompactionWorkerConfig::new(None, 2, 1, CascadeConfig::new(0, Duration::from_millis(0)));
    let handle = driver.spawn_worker(
        Arc::clone(&inner.executor),
        planner,
        executor,
        worker_config,
    );

    handle.kick();
    assert!(
        wait_for_executions(&executed, 1).await,
        "expected compaction work after kick"
    );
    drop(handle);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.trigger_kick, 1);
    assert_eq!(snapshot.trigger_periodic, 0);
    assert!(snapshot.job_count >= 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cascade_scheduling_respects_budget() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let mut inner: DbInner<InMemoryFs, TokioExecutor> =
        DB::new(mode_cfg, Arc::clone(&executor)).await?.into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    inner.compaction_metrics = Some(Arc::clone(&metrics));

    let entry = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    inner
        .manifest
        .apply_version_edits(
            inner.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry],
            }],
        )
        .await?;

    let executed = Arc::new(StdMutex::new(Vec::new()));
    let planner = CascadePlanner;
    let executor = CountingExecutor::new(Arc::clone(&executed));
    let driver = Arc::new(inner.compaction_driver());
    let worker_config =
        CompactionWorkerConfig::new(None, 2, 1, CascadeConfig::new(0, Duration::from_millis(0)));
    let handle = driver.spawn_worker(
        Arc::clone(&inner.executor),
        planner,
        executor,
        worker_config,
    );

    handle.kick();
    assert!(
        wait_for_executions(&executed, 1).await,
        "expected initial compaction execution"
    );
    drop(handle);

    let tasks = executed.lock().expect("executed lock").clone();
    let l0_to_l1 = tasks
        .iter()
        .filter(|(source, target)| *source == 0 && *target == 1)
        .count();
    let l1_to_l2 = tasks
        .iter()
        .filter(|(source, target)| *source == 1 && *target == 2)
        .count();
    assert_eq!(l0_to_l1, 1);
    assert_eq!(l1_to_l2, 0);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cascades_scheduled, 0);
    assert!(snapshot.cascades_blocked_budget >= 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cascade_scheduling_respects_cooldown() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
        .primary_key("id")
        .build()
        .expect("schema builder");
    let executor = Arc::new(TokioExecutor::default());
    let mut inner: DbInner<InMemoryFs, TokioExecutor> =
        DB::new(mode_cfg, Arc::clone(&executor)).await?.into_inner();
    let metrics = Arc::new(CompactionMetrics::new());
    inner.compaction_metrics = Some(Arc::clone(&metrics));

    let entry_a = SstEntry::new(
        SsTableId::new(1),
        None,
        None,
        Path::from("L0/001.parquet"),
        None,
    );
    let entry_b = SstEntry::new(
        SsTableId::new(2),
        None,
        None,
        Path::from("L0/002.parquet"),
        None,
    );
    inner
        .manifest
        .apply_version_edits(
            inner.manifest_table,
            &[VersionEdit::AddSsts {
                level: 0,
                entries: vec![entry_a, entry_b],
            }],
        )
        .await?;

    let executed = Arc::new(StdMutex::new(Vec::new()));
    let planner = CascadePlanner;
    let executor = CountingExecutor::new(Arc::clone(&executed));
    let driver = Arc::new(inner.compaction_driver());
    let worker_config = CompactionWorkerConfig::new(
        Some(Duration::from_millis(5)),
        4,
        1,
        CascadeConfig::new(1, Duration::from_secs(60)),
    );
    let handle = driver.spawn_worker(
        Arc::clone(&inner.executor),
        planner,
        executor,
        worker_config,
    );

    handle.kick();
    assert!(
        wait_for_executions(&executed, 3).await,
        "expected two L0->L1 executions and one cascade"
    );

    drop(handle);

    let tasks = executed.lock().expect("executed lock").clone();
    let l0_to_l1 = tasks
        .iter()
        .filter(|(source, target)| *source == 0 && *target == 1)
        .count();
    let l1_to_l2 = tasks
        .iter()
        .filter(|(source, target)| *source == 1 && *target == 2)
        .count();
    assert_eq!(l0_to_l1, 2);
    assert_eq!(l1_to_l2, 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cascades_scheduled, 1);
    assert!(snapshot.cascades_blocked_cooldown >= 1);
    Ok(())
}

#[test]
fn resolve_compaction_inputs_keeps_levels() {
    let file_ids = FileIdGenerator::default();
    let table_id = TableId::new(&file_ids);
    let mut version = VersionState::empty(table_id);
    let l0_id = SsTableId::new(1);
    let l1_id = SsTableId::new(2);
    let edits = vec![
        VersionEdit::AddSsts {
            level: 0,
            entries: vec![SstEntry::new(
                l0_id.clone(),
                None,
                None,
                Path::from("L0/000.parquet"),
                None,
            )],
        },
        VersionEdit::AddSsts {
            level: 1,
            entries: vec![SstEntry::new(
                l1_id.clone(),
                None,
                None,
                Path::from("L1/001.parquet"),
                None,
            )],
        },
    ];
    version.apply_edits(&edits).expect("apply edits");

    let task = CompactionTask {
        source_level: 0,
        target_level: 1,
        input: vec![
            CompactionInput {
                level: 0,
                sst_id: l0_id,
            },
            CompactionInput {
                level: 1,
                sst_id: l1_id,
            },
        ],
        key_range: None,
    };

    let resolved = orchestrator::resolve_inputs(&version, &task).expect("resolve");
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].level(), 0);
    assert_eq!(resolved[1].level(), 1);
}
