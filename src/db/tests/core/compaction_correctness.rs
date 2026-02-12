use std::{
    collections::{BTreeMap, VecDeque},
    env,
    fmt::{self, Write},
    path::PathBuf,
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::{disk::LocalFs, dynamic::DynFs, executor::tokio::TokioExecutor, path::Path};
use typed_arrow_dyn::{DynCell, DynRow};

use super::common::workspace_temp_dir;
use crate::{
    db::{DB, Expr, ScalarValue},
    extractor,
    inmem::policy::BatchesThreshold,
    mvcc::Timestamp,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
    schema::SchemaBuilder,
    test::{build_batch, compact_merge_l0},
    transaction::Snapshot as TxSnapshot,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Version {
    commit_ts: u64,
    tombstone: bool,
    value: Option<i64>,
}

impl Version {
    fn put(commit_ts: u64, value: i64) -> Self {
        Self {
            commit_ts,
            tombstone: false,
            value: Some(value),
        }
    }

    fn delete(commit_ts: u64) -> Self {
        Self {
            commit_ts,
            tombstone: true,
            value: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleVersion {
    commit_ts: u64,
    tombstone: bool,
    value: Option<i64>,
}

impl From<&Version> for VisibleVersion {
    fn from(version: &Version) -> Self {
        Self {
            commit_ts: version.commit_ts,
            tombstone: version.tombstone,
            value: version.value,
        }
    }
}

#[derive(Default, Debug)]
struct MvccOracle {
    versions: BTreeMap<String, Vec<Version>>,
}

impl MvccOracle {
    // Phase 0 invariants are type-agnostic, so we intentionally keep values as i64 for clarity.
    fn put(&mut self, key: impl Into<String>, commit_ts: u64, value: i64) {
        self.versions
            .entry(key.into())
            .or_default()
            .push(Version::put(commit_ts, value));
    }

    fn delete(&mut self, key: impl Into<String>, commit_ts: u64) {
        self.versions
            .entry(key.into())
            .or_default()
            .push(Version::delete(commit_ts));
    }

    fn visible_version(&self, key: &str, snapshot_ts: u64) -> Option<VisibleVersion> {
        let versions = self.versions.get(key)?;
        let mut best: Option<VisibleVersion> = None;
        for version in versions {
            if version.commit_ts > snapshot_ts {
                continue;
            }
            match &best {
                None => best = Some(VisibleVersion::from(version)),
                Some(best_version) => {
                    if version.commit_ts > best_version.commit_ts {
                        best = Some(VisibleVersion::from(version));
                    } else if version.commit_ts == best_version.commit_ts
                        && version.tombstone
                        && !best_version.tombstone
                    {
                        best = Some(VisibleVersion::from(version));
                    }
                }
            }
        }
        best
    }

    fn get(&self, key: &str, snapshot_ts: u64) -> Option<i64> {
        self.visible_version(key, snapshot_ts).and_then(|version| {
            if version.tombstone {
                None
            } else {
                version.value
            }
        })
    }

    fn scan_range(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        snapshot_ts: u64,
    ) -> Vec<(String, i64)> {
        use std::ops::Bound;

        let start_bound = match start {
            Some(bound) => Bound::Included(bound),
            None => Bound::Unbounded,
        };
        let end_bound = match end {
            Some(bound) => Bound::Included(bound),
            None => Bound::Unbounded,
        };

        let mut rows = Vec::new();
        for (key, _) in self.versions.range::<str, _>((start_bound, end_bound)) {
            if let Some(value) = self.get(key, snapshot_ts) {
                rows.push((key.clone(), value));
            }
        }
        rows
    }
}

struct ScenarioHarness {
    db: DB<LocalFs, TokioExecutor>,
    base_schema: SchemaRef,
    schema: SchemaRef,
    sst_cfg: Arc<SsTableConfig>,
    next_sst_id: u64,
    db_root: PathBuf,
}

impl ScenarioHarness {
    async fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db_root = workspace_temp_dir(name);
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let (db, schema) = Self::open_db(Arc::clone(&base_schema), &db_root).await?;
        let sst_cfg = Self::build_sst_cfg(Arc::clone(&schema), &db_root)?;

        Ok(Self {
            db,
            base_schema,
            schema,
            sst_cfg,
            next_sst_id: 1,
            db_root,
        })
    }

    async fn reopen(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (db, schema) = Self::open_db(Arc::clone(&self.base_schema), &self.db_root).await?;
        self.db = db;
        self.schema = schema;
        self.sst_cfg = Self::build_sst_cfg(Arc::clone(&self.schema), &self.db_root)?;
        Ok(())
    }

    async fn open_db(
        base_schema: SchemaRef,
        db_root: &PathBuf,
    ) -> Result<(DB<LocalFs, TokioExecutor>, SchemaRef), Box<dyn std::error::Error>> {
        let config = SchemaBuilder::from_schema(Arc::clone(&base_schema))
            .primary_key("id")
            .with_metadata()
            .build()?;
        let schema = Arc::clone(&config.schema);
        let db: DB<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(config)
            .on_disk(db_root)?
            .disable_minor_compaction()
            .build()
            .await?;
        let mut inner = db.into_inner();
        inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
        let db = DB::from_inner(Arc::new(inner));
        Ok((db, schema))
    }

    fn build_sst_cfg(
        schema: SchemaRef,
        db_root: &PathBuf,
    ) -> Result<Arc<SsTableConfig>, Box<dyn std::error::Error>> {
        let extractor = extractor::projection_for_field(Arc::clone(&schema), 0)?;
        let sst_root = Path::from_filesystem_path(db_root.join("sst"))?;
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        Ok(Arc::new(
            SsTableConfig::new(Arc::clone(&schema), fs, sst_root)
                .with_key_extractor(extractor.into()),
        ))
    }

    fn next_commit_ts(&self) -> u64 {
        self.db.inner().commit_clock.peek().get()
    }

    async fn ingest_put(
        &self,
        key: &str,
        value: i64,
        oracle: &mut MvccOracle,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let commit_ts = self.next_commit_ts();
        let batch = row_batch(Arc::clone(&self.schema), key, value)?;
        self.db.ingest(batch).await?;
        oracle.put(key, commit_ts, value);
        Ok(commit_ts)
    }

    async fn ingest_delete(
        &self,
        key: &str,
        oracle: &mut MvccOracle,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let commit_ts = self.next_commit_ts();
        let batch = row_batch(Arc::clone(&self.schema), key, 0)?;
        self.db.ingest_with_tombstones(batch, vec![true]).await?;
        oracle.delete(key, commit_ts);
        Ok(commit_ts)
    }

    async fn flush_immutables_to_l0(&mut self) -> Result<u64, Box<dyn std::error::Error>> {
        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let descriptor = SsTableDescriptor::new(SsTableId::new(sst_id), 0);
        self.db
            .inner()
            .flush_immutables_with_descriptor(Arc::clone(&self.sst_cfg), descriptor)
            .await?;
        Ok(sst_id)
    }

    async fn try_flush_immutables_to_l0(
        &mut self,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let sst_id = self.next_sst_id;
        let descriptor = SsTableDescriptor::new(SsTableId::new(sst_id), 0);
        match self
            .db
            .inner()
            .flush_immutables_with_descriptor(Arc::clone(&self.sst_cfg), descriptor)
            .await
        {
            Ok(_) => {
                self.next_sst_id += 1;
                Ok(Some(sst_id))
            }
            Err(SsTableError::NoImmutableSegments) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn compact_l0(
        &mut self,
        sst_ids: Vec<u64>,
        target_level: u32,
    ) -> Result<crate::compaction::executor::CompactionOutcome, Box<dyn std::error::Error>> {
        let start_id = self.next_sst_id.max(10_000);
        let outcome = compact_merge_l0(
            self.db.inner().as_ref(),
            sst_ids,
            target_level,
            Arc::clone(&self.sst_cfg),
            start_id,
        )
        .await?;
        let next_generated_id = outcome
            .add_ssts
            .iter()
            .map(|entry| entry.sst_id().raw())
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or_else(|| start_id.saturating_add(1));
        self.next_sst_id = self.next_sst_id.max(next_generated_id);
        Ok(outcome)
    }

    async fn assert_post_compaction_snapshot(
        &self,
        scenario: &str,
        pre_snapshot: &TxSnapshot,
        oracle: &MvccOracle,
        range: Option<(&str, &str)>,
        ctx: Option<&FailureContext<'_>>,
    ) -> Result<TxSnapshot, Box<dyn std::error::Error>> {
        let pre_ts = pre_snapshot.read_view().read_ts().get();
        let pre_head = pre_snapshot.head().last_manifest_txn;

        let post_snapshot = self.db.begin_snapshot().await?;
        let post_ts = post_snapshot.read_view().read_ts().get();
        assert_eq!(
            post_ts, pre_ts,
            "scenario={scenario} expected snapshot ts to be stable across compaction"
        );
        assert_ne!(
            post_snapshot.head().last_manifest_txn,
            pre_head,
            "scenario={scenario} expected manifest head to advance after compaction"
        );
        assert_oracle_matches(scenario, post_ts, &post_snapshot, oracle, &self.db, ctx).await?;
        if let Some((start, end)) = range {
            assert_range_matches(
                scenario,
                post_ts,
                &post_snapshot,
                oracle,
                &self.db,
                start,
                end,
                ctx,
            )
            .await?;
        }
        Ok(post_snapshot)
    }
}

fn row_batch(
    schema: SchemaRef,
    key: &str,
    value: i64,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let rows = vec![DynRow(vec![
        Some(DynCell::Str(key.into())),
        Some(DynCell::I64(value)),
    ])];
    Ok(build_batch(schema, rows)?)
}

fn collect_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        for idx in 0..batch.num_rows() {
            rows.push((ids.value(idx).to_string(), values.value(idx)));
        }
    }
    rows
}

async fn scan_all(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &TxSnapshot,
) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    let batches = snapshot.scan(db).collect().await?;
    Ok(collect_rows(&batches))
}

async fn scan_key(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &TxSnapshot,
    key: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let predicate = Expr::eq("id", ScalarValue::from(key));
    let batches = snapshot.scan(db).filter(predicate).collect().await?;
    let mut rows = collect_rows(&batches);
    if rows.is_empty() {
        return Ok(None);
    }
    assert!(
        rows.len() == 1,
        "expected at most one row for key {key}, got {rows:?}"
    );
    Ok(Some(rows.remove(0).1))
}

async fn scan_range(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &TxSnapshot,
    start: &str,
    end: &str,
) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    let predicate = Expr::between("id", ScalarValue::from(start), ScalarValue::from(end), true);
    let batches = snapshot.scan(db).filter(predicate).collect().await?;
    Ok(collect_rows(&batches))
}

async fn assert_oracle_matches(
    scenario: &str,
    snapshot_ts: u64,
    snapshot: &TxSnapshot,
    oracle: &MvccOracle,
    db: &DB<LocalFs, TokioExecutor>,
    ctx: Option<&FailureContext<'_>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_scan = oracle.scan_range(None, None, snapshot_ts);
    let actual_scan = scan_all(db, snapshot).await?;
    assert_eq!(
        actual_scan,
        expected_scan,
        "scenario={scenario} snapshot_ts={snapshot_ts} scan mismatch: expected={expected_scan:?} \
         actual={actual_scan:?}{}",
        ctx_suffix(ctx),
    );

    for key in oracle.versions.keys() {
        let expected = oracle.get(key, snapshot_ts);
        let actual = scan_key(db, snapshot, key).await?;
        assert_eq!(
            actual,
            expected,
            "scenario={scenario} snapshot_ts={snapshot_ts} key={key} mismatch: \
             expected={expected:?} actual={actual:?}{}",
            ctx_suffix(ctx),
        );
    }
    Ok(())
}

async fn assert_range_matches(
    scenario: &str,
    snapshot_ts: u64,
    snapshot: &TxSnapshot,
    oracle: &MvccOracle,
    db: &DB<LocalFs, TokioExecutor>,
    start: &str,
    end: &str,
    ctx: Option<&FailureContext<'_>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = oracle.scan_range(Some(start), Some(end), snapshot_ts);
    let actual = scan_range(db, snapshot, start, end).await?;
    assert_eq!(
        actual,
        expected,
        "scenario={scenario} snapshot_ts={snapshot_ts} range={start}..={end} mismatch: \
         expected={expected:?} actual={actual:?}{}",
        ctx_suffix(ctx),
    );
    Ok(())
}

fn ctx_suffix(ctx: Option<&FailureContext<'_>>) -> String {
    ctx.map(|ctx| format!("\n{}", ctx.render()))
        .unwrap_or_default()
}

const MODEL_TRACE_CAPACITY: usize = 200;
const MODEL_OPS_PER_SEED: usize = 200;
const MODEL_KEYSPACE: usize = 20;
const MODEL_SEEDS: &[u64] = &[1, 13, 57, 101, 1009];
const SEEK_HIGH_KEY: &str = "kzzzz";

#[derive(Clone, Debug)]
enum Op {
    Put {
        key: String,
        value: i64,
    },
    Delete {
        key: String,
    },
    Flush,
    Compact {
        sst_ids: Vec<u64>,
        target_level: u32,
    },
    Get {
        key: String,
        snapshot_ts: u64,
    },
    Scan {
        start: String,
        end: String,
        snapshot_ts: u64,
    },
    Snapshot {
        snapshot_ts: u64,
    },
    Reopen,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Op::Put { key, value } => write!(f, "Put key={key} value={value}"),
            Op::Delete { key } => write!(f, "Delete key={key}"),
            Op::Flush => write!(f, "Flush immutables"),
            Op::Compact {
                sst_ids,
                target_level,
            } => write!(f, "Compact L0 -> L{target_level} sst_ids={sst_ids:?}"),
            Op::Get { key, snapshot_ts } => write!(f, "Get key={key} ts={snapshot_ts}"),
            Op::Scan {
                start,
                end,
                snapshot_ts,
            } => write!(f, "Scan {start}..={end} ts={snapshot_ts}"),
            Op::Snapshot { snapshot_ts } => write!(f, "Snapshot ts={snapshot_ts}"),
            Op::Reopen => write!(f, "Reopen"),
        }
    }
}

#[derive(Clone, Debug)]
struct LoggedOp {
    step: usize,
    op: Op,
}

#[derive(Clone, Debug)]
struct OperationTrace {
    entries: VecDeque<LoggedOp>,
    capacity: usize,
    next_step: usize,
}

impl OperationTrace {
    fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
            capacity,
            next_step: 0,
        }
    }

    fn push(&mut self, op: Op) {
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        let entry = LoggedOp {
            step: self.next_step,
            op,
        };
        self.entries.push_back(entry);
        self.next_step = self.next_step.saturating_add(1);
    }

    fn render(&self) -> String {
        let mut lines = String::new();
        for entry in &self.entries {
            let _ = writeln!(&mut lines, "{:04}: {}", entry.step, entry.op);
        }
        lines
    }
}

struct FailureContext<'a> {
    seed: u64,
    trace: &'a OperationTrace,
    snapshot_ts: Option<u64>,
}

impl<'a> FailureContext<'a> {
    fn render(&self) -> String {
        let snapshot = self
            .snapshot_ts
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "none".to_string());
        format!(
            "seed={}\nactive_snapshot_ts={}\noperation_trace:\n{}",
            self.seed,
            snapshot,
            self.trace.render()
        )
    }
}

#[derive(Clone, Debug)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        let mut z = self.state.wrapping_add(0x9E3779B97F4A7C15);
        self.state = z;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() % upper as u64) as usize
    }

    fn next_i64(&mut self, upper: i64) -> i64 {
        if upper <= 0 {
            return 0;
        }
        (self.next_u64() % upper as u64) as i64
    }
}

#[derive(Clone, Copy, Debug)]
enum OpKind {
    Put,
    Delete,
    Flush,
    Compact,
    Get,
    Scan,
    Snapshot,
    Reopen,
}

struct ModelRunner {
    seed: u64,
    rng: SplitMix64,
    trace: OperationTrace,
    harness: ScenarioHarness,
    oracle: MvccOracle,
    l0_ssts: Vec<u64>,
    active_snapshot_ts: Option<u64>,
    active_snapshot: Option<TxSnapshot>,
    allow_reopen: bool,
    eager_flush: bool,
    allow_sst: bool,
}

impl ModelRunner {
    async fn new(seed: u64, name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Baseline runs leave SST/reopen off for deterministic behavior. Expanded CI coverage
        // opts into randomized SST/compaction/reopen paths via these env toggles.
        let allow_reopen = env::var("TONBO_COMPACTION_REOPEN").is_ok();
        let allow_sst = env::var("TONBO_COMPACTION_MODEL_SST").is_ok();
        let eager_flush = allow_sst
            && env::var("TONBO_COMPACTION_EAGER_FLUSH")
                .map(|val| val != "0")
                .unwrap_or(true);
        Ok(Self {
            seed,
            rng: SplitMix64::new(seed),
            trace: OperationTrace::new(MODEL_TRACE_CAPACITY),
            harness: ScenarioHarness::new(name).await?,
            oracle: MvccOracle::default(),
            l0_ssts: Vec::new(),
            active_snapshot_ts: None,
            active_snapshot: None,
            allow_reopen,
            eager_flush,
            allow_sst,
        })
    }

    fn failure_context(&self, snapshot_ts: Option<u64>) -> FailureContext<'_> {
        FailureContext {
            seed: self.seed,
            trace: &self.trace,
            snapshot_ts,
        }
    }

    fn clear_active_snapshot(&mut self) {
        self.active_snapshot_ts = None;
        self.active_snapshot = None;
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..MODEL_OPS_PER_SEED {
            let op_kind = self.pick_op();
            self.apply_op(op_kind).await?;
        }
        Ok(())
    }

    fn pick_op(&mut self) -> OpKind {
        let roll = self.rng.next_usize(100);
        match roll {
            0..=29 => OpKind::Put,
            30..=44 => OpKind::Delete,
            45..=58 => OpKind::Get,
            59..=71 => OpKind::Scan,
            72..=82 => OpKind::Flush,
            83..=93 => OpKind::Compact,
            94..=97 if self.allow_reopen => OpKind::Reopen,
            _ => OpKind::Snapshot,
        }
    }

    fn pick_key(&mut self) -> String {
        let idx = self.rng.next_usize(MODEL_KEYSPACE);
        format!("k{idx:02}")
    }

    async fn apply_op(&mut self, op_kind: OpKind) -> Result<(), Box<dyn std::error::Error>> {
        match op_kind {
            OpKind::Put => {
                self.clear_active_snapshot();
                let key = self.pick_key();
                let value = self.rng.next_i64(10_000);
                self.trace.push(Op::Put {
                    key: key.clone(),
                    value,
                });
                self.harness
                    .ingest_put(&key, value, &mut self.oracle)
                    .await?;
                if self.eager_flush {
                    self.trace.push(Op::Flush);
                    if let Some(sst_id) = self.harness.try_flush_immutables_to_l0().await? {
                        self.l0_ssts.push(sst_id);
                    }
                }
            }
            OpKind::Delete => {
                self.clear_active_snapshot();
                let key = self.pick_key();
                self.trace.push(Op::Delete { key: key.clone() });
                self.harness.ingest_delete(&key, &mut self.oracle).await?;
                if self.eager_flush {
                    self.trace.push(Op::Flush);
                    if let Some(sst_id) = self.harness.try_flush_immutables_to_l0().await? {
                        self.l0_ssts.push(sst_id);
                    }
                }
            }
            OpKind::Flush => {
                self.clear_active_snapshot();
                self.trace.push(Op::Flush);
                if self.allow_sst {
                    if let Some(sst_id) = self.harness.try_flush_immutables_to_l0().await? {
                        self.l0_ssts.push(sst_id);
                    }
                }
            }
            OpKind::Compact => {
                if !self.allow_sst || self.l0_ssts.len() < 2 {
                    return Ok(());
                }
                let max = self.l0_ssts.len().min(6);
                let count = 2 + self.rng.next_usize(max - 1);
                let mut sst_ids = Vec::with_capacity(count);
                for _ in 0..count {
                    let idx = self.rng.next_usize(self.l0_ssts.len());
                    sst_ids.push(self.l0_ssts.remove(idx));
                }
                sst_ids.sort();
                self.trace.push(Op::Compact {
                    sst_ids: sst_ids.clone(),
                    target_level: 1,
                });
                let outcome = self.harness.compact_l0(sst_ids.clone(), 1).await?;
                assert_eq!(
                    outcome.remove_ssts.len(),
                    sst_ids.len(),
                    "compaction removed count mismatch{}",
                    ctx_suffix(Some(&self.failure_context(self.active_snapshot_ts))),
                );
                if outcome.add_ssts.is_empty() {
                    return Err("compaction produced no output sst".into());
                }
                let (snapshot, snapshot_ts) = self.read_snapshot().await?;
                let ctx = self.failure_context(Some(snapshot_ts));
                assert_oracle_matches(
                    "model_based_compaction",
                    snapshot_ts,
                    &snapshot,
                    &self.oracle,
                    &self.harness.db,
                    Some(&ctx),
                )
                .await?;
            }
            OpKind::Get => {
                let (snapshot, snapshot_ts) = self.read_snapshot().await?;
                let key = self.pick_key();
                self.trace.push(Op::Get {
                    key: key.clone(),
                    snapshot_ts,
                });
                let expected = self.oracle.get(&key, snapshot_ts);
                let actual = scan_key(&self.harness.db, &snapshot, &key).await?;
                let ctx = self.failure_context(Some(snapshot_ts));
                assert_eq!(
                    actual,
                    expected,
                    "scenario=model_based_get snapshot_ts={snapshot_ts} key={key} mismatch: \
                     expected={expected:?} actual={actual:?}{}",
                    ctx_suffix(Some(&ctx)),
                );
            }
            OpKind::Scan => {
                let (snapshot, snapshot_ts) = self.read_snapshot().await?;
                let start = self.pick_key();
                let end = self.pick_key();
                let (lo, hi) = if start <= end {
                    (start, end)
                } else {
                    (end, start)
                };
                self.trace.push(Op::Scan {
                    start: lo.clone(),
                    end: hi.clone(),
                    snapshot_ts,
                });
                let ctx = self.failure_context(Some(snapshot_ts));
                assert_range_matches(
                    "model_based_scan",
                    snapshot_ts,
                    &snapshot,
                    &self.oracle,
                    &self.harness.db,
                    &lo,
                    &hi,
                    Some(&ctx),
                )
                .await?;
            }
            OpKind::Snapshot => {
                let snapshot = self.harness.db.begin_snapshot().await?;
                let snapshot_ts = snapshot.read_view().read_ts().get();
                self.active_snapshot_ts = Some(snapshot_ts);
                self.active_snapshot = Some(snapshot.clone());
                self.trace.push(Op::Snapshot { snapshot_ts });
                let ctx = self.failure_context(Some(snapshot_ts));
                assert_oracle_matches(
                    "model_based_snapshot",
                    snapshot_ts,
                    &snapshot,
                    &self.oracle,
                    &self.harness.db,
                    Some(&ctx),
                )
                .await?;
            }
            OpKind::Reopen => {
                self.trace.push(Op::Reopen);
                let _ = self.harness.try_flush_immutables_to_l0().await?;
                self.harness.reopen().await?;
                if let Some(snapshot_ts) = self.active_snapshot_ts {
                    let snapshot = self
                        .harness
                        .db
                        .snapshot_at(Timestamp::new(snapshot_ts))
                        .await?;
                    let ctx = self.failure_context(Some(snapshot_ts));
                    assert_oracle_matches(
                        "model_based_reopen",
                        snapshot_ts,
                        &snapshot,
                        &self.oracle,
                        &self.harness.db,
                        Some(&ctx),
                    )
                    .await?;
                    self.active_snapshot = Some(snapshot);
                } else {
                    self.active_snapshot = None;
                }
            }
        }
        Ok(())
    }

    async fn read_snapshot(&mut self) -> Result<(TxSnapshot, u64), Box<dyn std::error::Error>> {
        if let Some(snapshot) = self.active_snapshot.as_ref()
            && let Some(ts) = self.active_snapshot_ts
        {
            Ok((snapshot.clone(), ts))
        } else if let Some(ts) = self.active_snapshot_ts {
            let snapshot = self.harness.db.snapshot_at(Timestamp::new(ts)).await?;
            self.active_snapshot = Some(snapshot.clone());
            Ok((snapshot, ts))
        } else {
            let snapshot = self.harness.db.begin_snapshot().await?;
            let ts = snapshot.read_view().read_ts().get();
            Ok((snapshot, ts))
        }
    }
}

fn seed_list() -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    if let Ok(raw) = env::var("TONBO_COMPACTION_SEED") {
        let mut seeds = Vec::new();
        for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let seed = if let Some(hex) = part.strip_prefix("0x") {
                u64::from_str_radix(hex, 16)?
            } else {
                part.parse::<u64>()?
            };
            seeds.push(seed);
        }
        if seeds.is_empty() {
            return Err("TONBO_COMPACTION_SEED was set but no seeds parsed".into());
        }
        Ok(seeds)
    } else {
        Ok(MODEL_SEEDS.to_vec())
    }
}

async fn assert_seek_matches(
    scenario: &str,
    snapshot_ts: u64,
    snapshot: &TxSnapshot,
    oracle: &MvccOracle,
    db: &DB<LocalFs, TokioExecutor>,
    start: &str,
    ctx: Option<&FailureContext<'_>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = oracle.scan_range(Some(start), None, snapshot_ts);
    let actual = scan_range(db, snapshot, start, SEEK_HIGH_KEY).await?;
    assert_eq!(
        actual,
        expected,
        "scenario={scenario} snapshot_ts={snapshot_ts} seek_start={start} mismatch: \
         expected={expected:?} actual={actual:?}{}",
        ctx_suffix(ctx),
    );
    Ok(())
}

#[test]
fn mvcc_oracle_visible_version_prefers_tombstone() {
    let mut oracle = MvccOracle::default();
    oracle.put("a", 5, 11);
    oracle.delete("a", 5);
    oracle.put("a", 7, 13);
    oracle.put("b", 3, 21);
    oracle.delete("b", 4);
    oracle.put("c", 2, 31);

    assert_eq!(oracle.visible_version("a", 4), None);
    assert_eq!(
        oracle.visible_version("a", 5),
        Some(VisibleVersion {
            commit_ts: 5,
            tombstone: true,
            value: None,
        })
    );
    assert_eq!(oracle.get("a", 6), None);
    assert_eq!(oracle.get("a", 7), Some(13));

    assert_eq!(oracle.get("b", 3), Some(21));
    assert_eq!(oracle.get("b", 4), None);
    assert_eq!(oracle.get("b", 5), None);

    let scan = oracle.scan_range(Some("a"), Some("c"), 6);
    assert_eq!(scan, vec![("c".to_string(), 31)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_overwrite_chain() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = "overwrite_chain";
    let mut harness = ScenarioHarness::new("compaction-correctness-overwrite-chain").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k1", 10, &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;
    let _ts1 = harness.ingest_put("k1", 20, &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;
    let _ts2 = harness.ingest_put("k1", 30, &mut oracle).await?;
    let sst2 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;

    let outcome = harness.compact_l0(vec![sst0, sst1, sst2], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        3,
        "scenario={scenario} expected compaction to remove 3 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    harness
        .assert_post_compaction_snapshot(scenario, &snapshot, &oracle, None, None)
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_delete_heavy() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = "delete_heavy";
    let mut harness = ScenarioHarness::new("compaction-correctness-delete-heavy").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k1", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_delete("k1", &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts2 = harness.ingest_put("k2", 20, &mut oracle).await?;
    let _ts3 = harness.ingest_delete("k2", &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let _ts4 = harness.ingest_put("k3", 30, &mut oracle).await?;
    let _ts5 = harness.ingest_delete("k3", &mut oracle).await?;
    let sst2 = harness.flush_immutables_to_l0().await?;

    let _ts6 = harness.ingest_put("k4", 40, &mut oracle).await?;
    let sst3 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;

    let outcome = harness.compact_l0(vec![sst0, sst1, sst2, sst3], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        4,
        "scenario={scenario} expected compaction to remove 4 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    harness
        .assert_post_compaction_snapshot(scenario, &snapshot, &oracle, None, None)
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_range_scan_with_deletes() -> Result<(), Box<dyn std::error::Error>>
{
    let scenario = "range_scan_with_deletes";
    let mut harness = ScenarioHarness::new("compaction-correctness-range-scan").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k01", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let _ts2 = harness.ingest_put("k03", 30, &mut oracle).await?;
    let _ts3 = harness.ingest_delete("k02", &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts4 = harness.ingest_put("k04", 40, &mut oracle).await?;
    let _ts5 = harness.ingest_put("k05", 50, &mut oracle).await?;
    let _ts6 = harness.ingest_delete("k05", &mut oracle).await?;
    let _ts7 = harness.ingest_put("k06", 60, &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &snapshot,
        &oracle,
        &harness.db,
        "k02",
        "k05",
        None,
    )
    .await?;

    let outcome = harness.compact_l0(vec![sst0, sst1], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        2,
        "scenario={scenario} expected compaction to remove 2 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    harness
        .assert_post_compaction_snapshot(scenario, &snapshot, &oracle, Some(("k02", "k05")), None)
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_cross_segment_overlap() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = "cross_segment_overlap";
    let mut harness = ScenarioHarness::new("compaction-correctness-cross-segment").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k01", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let _ts2 = harness.ingest_put("k03", 30, &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts3 = harness.ingest_put("k02", 200, &mut oracle).await?;
    let _ts4 = harness.ingest_put("k04", 40, &mut oracle).await?;
    let _ts5 = harness.ingest_delete("k01", &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let _ts6 = harness.ingest_put("k03", 300, &mut oracle).await?;
    let _ts7 = harness.ingest_delete("k02", &mut oracle).await?;
    let _ts8 = harness.ingest_put("k05", 50, &mut oracle).await?;
    let sst2 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &snapshot,
        &oracle,
        &harness.db,
        "k01",
        "k05",
        None,
    )
    .await?;

    let outcome = harness.compact_l0(vec![sst0, sst1, sst2], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        3,
        "scenario={scenario} expected compaction to remove 3 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    harness
        .assert_post_compaction_snapshot(scenario, &snapshot, &oracle, Some(("k01", "k05")), None)
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_model_randomized() -> Result<(), Box<dyn std::error::Error>> {
    let seeds = seed_list()?;
    for seed in seeds {
        let name = format!("compaction-correctness-model-{seed}");
        let mut runner = ModelRunner::new(seed, &name).await?;
        if let Err(err) = runner.run().await {
            let ctx = runner.failure_context(runner.active_snapshot_ts);
            panic!("model-based run failed: {err}\n{}", ctx.render());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_reopen_multi_immutable_flush_ordering()
-> Result<(), Box<dyn std::error::Error>> {
    let scenario = "reopen_multi_immutable_flush_ordering";
    let mut harness = ScenarioHarness::new("compaction-correctness-reopen-flush-ordering").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let _ts1 = harness.ingest_put("k01", 10, &mut oracle).await?;
    let _ts2 = harness.ingest_put("k02", 30, &mut oracle).await?;
    let _ts3 = harness.ingest_put("k03", 40, &mut oracle).await?;
    let _sst = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();
    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &snapshot,
        &oracle,
        &harness.db,
        "k01",
        "k03",
        None,
    )
    .await?;

    harness.reopen().await?;
    let reopened = harness.db.snapshot_at(Timestamp::new(snapshot_ts)).await?;
    assert_oracle_matches(scenario, snapshot_ts, &reopened, &oracle, &harness.db, None).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &reopened,
        &oracle,
        &harness.db,
        "k01",
        "k03",
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_reopen_snapshot_durability()
-> Result<(), Box<dyn std::error::Error>> {
    let scenario = "reopen_snapshot_durability";
    let mut harness = ScenarioHarness::new("compaction-correctness-reopen-snapshot").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k01", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let _ts2 = harness.ingest_delete("k02", &mut oracle).await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();
    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;

    harness.reopen().await?;
    let reopened_snapshot = harness.db.snapshot_at(Timestamp::new(snapshot_ts)).await?;
    assert_oracle_matches(
        scenario,
        snapshot_ts,
        &reopened_snapshot,
        &oracle,
        &harness.db,
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_delete_only_sst_reopen_and_compact()
-> Result<(), Box<dyn std::error::Error>> {
    let scenario = "delete_only_sst_reopen_and_compact";
    let mut harness = ScenarioHarness::new("compaction-correctness-delete-only-sst").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_delete("k01", &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts1 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();
    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &snapshot,
        &oracle,
        &harness.db,
        "k01",
        "k02",
        None,
    )
    .await?;

    harness.reopen().await?;
    let reopened = harness.db.snapshot_at(Timestamp::new(snapshot_ts)).await?;
    assert_oracle_matches(scenario, snapshot_ts, &reopened, &oracle, &harness.db, None).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &reopened,
        &oracle,
        &harness.db,
        "k01",
        "k02",
        None,
    )
    .await?;

    let outcome = harness.compact_l0(vec![sst0, sst1], 1).await?;
    assert_eq!(
        outcome.remove_ssts.len(),
        2,
        "scenario={scenario} expected compaction to remove 2 SSTs"
    );
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );

    let post_snapshot = harness.db.snapshot_at(Timestamp::new(snapshot_ts)).await?;
    assert_oracle_matches(
        scenario,
        snapshot_ts,
        &post_snapshot,
        &oracle,
        &harness.db,
        None,
    )
    .await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &post_snapshot,
        &oracle,
        &harness.db,
        "k01",
        "k02",
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_iterator_seek_stability() -> Result<(), Box<dyn std::error::Error>>
{
    let scenario = "iterator_seek_stability";
    let mut harness = ScenarioHarness::new("compaction-correctness-iterator-seek").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k01", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_put("k03", 30, &mut oracle).await?;
    let _ts2 = harness.ingest_put("k05", 50, &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts3 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let _ts4 = harness.ingest_delete("k03", &mut oracle).await?;
    let _ts5 = harness.ingest_put("k06", 60, &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let _ts6 = harness.ingest_put("k04", 40, &mut oracle).await?;
    let _ts7 = harness.ingest_put("k05", 55, &mut oracle).await?;
    let sst2 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();
    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db, None).await?;
    for start in ["k00", "k02", "k03", "k05", "k07"] {
        assert_seek_matches(
            scenario,
            snapshot_ts,
            &snapshot,
            &oracle,
            &harness.db,
            start,
            None,
        )
        .await?;
    }

    let outcome = harness.compact_l0(vec![sst0, sst1, sst2], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        3,
        "scenario={scenario} expected compaction to remove 3 SSTs"
    );

    let post_snapshot = harness.db.snapshot_at(Timestamp::new(snapshot_ts)).await?;
    assert_oracle_matches(
        scenario,
        snapshot_ts,
        &post_snapshot,
        &oracle,
        &harness.db,
        None,
    )
    .await?;
    for start in ["k00", "k02", "k03", "k05", "k07"] {
        assert_seek_matches(
            scenario,
            snapshot_ts,
            &post_snapshot,
            &oracle,
            &harness.db,
            start,
            None,
        )
        .await?;
    }

    Ok(())
}
