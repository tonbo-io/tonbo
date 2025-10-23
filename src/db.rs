//! Generic DB parametrized by a `Mode` implementation.
//!
//! At the moment Tonbo only ships with the dynamic runtime-schema mode. The
//! trait-driven structure remains so that compile-time typed dispatch can be
//! reintroduced without reshaping the API.

use std::{sync::Arc, time::Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::executor::{Executor, Timer};

pub use crate::mode::{DynMode, DynModeConfig, Mode};
use crate::{
    inmem::{
        immutable::Immutable,
        mutable::MutableLayout,
        policy::{SealDecision, SealPolicy, StatsProvider},
    },
    mvcc::{CommitClock, Timestamp},
    ondisk::sstable::{SsTable, SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableError},
    record::extract::{KeyDyn, KeyExtractError},
    scan::RangeSet,
    wal::{WalConfig, WalHandle, frame::WalEvent, replay::Replayer},
};

/// A DB parametrized by a mode `M` that defines key, payload and insert interface.
pub struct DB<M: Mode, E: Executor + Timer + Send + Sync> {
    mode: M,
    mem: M::Mutable,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest)
    immutables: Vec<Immutable<M>>,
    // Sealing policy and last seal timestamp
    policy: Box<dyn SealPolicy + Send + Sync>,
    last_seal_at: Option<Instant>,
    // Executor powering async subsystems such as the WAL.
    executor: Arc<E>,
    // Optional WAL handle when durability is enabled.
    wal: Option<WalHandle<E>>,
    /// Pending WAL configuration captured before the writer is installed.
    wal_config: Option<WalConfig>,
    /// Monotonic commit timestamp assigned to ingests (autocommit path for now).
    commit_clock: CommitClock,
}

impl<E> DB<DynMode, E>
where
    E: Executor + Timer,
{
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.mode.schema
    }

    pub(crate) fn insert_into_mutable(
        &mut self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        self.mem
            .insert_batch(self.mode.extractor.as_ref(), batch, commit_ts)
    }

    pub(crate) fn replay_wal_events(
        &mut self,
        events: Vec<WalEvent>,
    ) -> Result<Option<Timestamp>, KeyExtractError> {
        use std::collections::HashMap;

        let mut last_commit_ts: Option<Timestamp> = None;
        let mut pending: HashMap<u64, Vec<(RecordBatch, Vec<bool>)>> = HashMap::new();
        for event in events {
            match event {
                WalEvent::DynAppend {
                    provisional_id,
                    batch,
                    tombstones,
                } => {
                    pending
                        .entry(provisional_id)
                        .or_default()
                        .push((batch, tombstones));
                }
                WalEvent::TxnCommit {
                    provisional_id,
                    commit_ts,
                } => {
                    if let Some(batches) = pending.remove(&provisional_id) {
                        for (batch, tombstones) in batches {
                            apply_dyn_wal_batch(self, batch, tombstones, commit_ts)?;
                        }
                        last_commit_ts = Some(match last_commit_ts {
                            Some(prev) => prev.max(commit_ts),
                            None => commit_ts,
                        });
                    }
                }
                WalEvent::TxnBegin { .. } | WalEvent::TxnAbort { .. } | WalEvent::SealMarker => {
                    // TODO: implement once transactional semantics are wired up.
                }
            }
        }

        Ok(last_commit_ts)
    }

    /// Scan the dynamic mutable memtable over key ranges, yielding owned dynamic rows.
    pub fn scan_mutable_rows<'a>(
        &'a self,
        ranges: &'a RangeSet<KeyDyn>,
    ) -> impl Iterator<Item = typed_arrow_dyn::DynRow> + 'a {
        self.mem.scan_rows(ranges)
    }

    /// Scan the dynamic mutable memtable with MVCC visibility at `read_ts`.
    pub fn scan_mutable_rows_at<'a>(
        &'a self,
        ranges: &'a RangeSet<KeyDyn>,
        read_ts: Timestamp,
    ) -> impl Iterator<Item = typed_arrow_dyn::DynRow> + 'a {
        self.mem.scan_rows_at(ranges, read_ts)
    }

    pub(crate) fn maybe_seal_after_insert(&mut self) -> Result<(), KeyExtractError> {
        let since = self.last_seal_at.map(|t| t.elapsed());
        let stats = self.mem.build_stats(since);
        if let SealDecision::Seal(_reason) = self.policy.evaluate(&stats) {
            if let Some(seg) = self.mem.seal_into_immutable(&self.mode.schema)? {
                self.immutables.push(seg);
            }
            self.last_seal_at = Some(Instant::now());
        }
        Ok(())
    }

    /// Ingest a batch along with its tombstone bitmap, routing through the WAL when enabled.
    pub async fn ingest_with_tombstones(
        &mut self,
        batch: RecordBatch,
        tombstones: Vec<bool>,
    ) -> Result<(), KeyExtractError> {
        insert_dyn_wal_batch(self, batch, tombstones).await
    }

    /// Ingest multiple batches, each paired with a tombstone bitmap.
    pub async fn ingest_many_with_tombstones(
        &mut self,
        batches: Vec<(RecordBatch, Vec<bool>)>,
    ) -> Result<(), KeyExtractError> {
        insert_dyn_wal_batches(self, batches).await
    }
}

// Methods common to all modes
impl<M: Mode, E: Executor + Timer> DB<M, E> {
    /// Construct a new DB in mode `M` using its configuration.
    pub fn new(config: M::Config, executor: Arc<E>) -> Result<Self, KeyExtractError>
    where
        M: Sized,
    {
        let (mode, mem) = M::build(config)?;
        Ok(Self {
            mode,
            mem,
            immutables: Vec::new(),
            policy: crate::inmem::policy::default_policy(),
            last_seal_at: None,
            executor,
            wal: None,
            wal_config: None,
            commit_clock: CommitClock::default(),
        })
    }

    /// Recover a DB by replaying WAL segments before enabling ingest.
    pub async fn recover_with_wal(
        config: M::Config,
        executor: Arc<E>,
        wal_cfg: WalConfig,
    ) -> Result<Self, KeyExtractError>
    where
        M: Sized,
    {
        let mut db = Self::new(config, executor)?;
        db.set_wal_config(Some(wal_cfg.clone()));

        let replayer = Replayer::new(wal_cfg);
        let events = replayer.scan().await.map_err(KeyExtractError::from)?;

        let last_commit_ts = M::replay_wal(&mut db, events)?;
        if let Some(ts) = last_commit_ts {
            db.commit_clock.advance_to_at_least(ts.saturating_add(1));
        }

        Ok(db)
    }

    /// Unified ingestion entry point using the mode's insertion contract.
    pub async fn ingest<'a>(&'a mut self, input: M::InsertInput) -> Result<(), KeyExtractError>
    where
        M: Sized,
    {
        M::insert(self, input).await
    }

    /// Ingest many inputs sequentially.
    pub async fn ingest_many<I>(&mut self, inputs: I) -> Result<(), KeyExtractError>
    where
        I: IntoIterator<Item = M::InsertInput>,
        M: Sized,
    {
        for item in inputs.into_iter() {
            M::insert(self, item).await?;
        }
        Ok(())
    }

    /// Approximate bytes used by keys in the mutable memtable.
    pub fn approx_mutable_bytes(&self) -> usize {
        <M::Mutable as MutableLayout<M::Key>>::approx_bytes(&self.mem)
    }

    /// Attach WAL configuration prior to enabling durability.
    pub fn with_wal_config(mut self, cfg: WalConfig) -> Self {
        self.wal_config = Some(cfg);
        self
    }

    /// Access the configured WAL settings, if any.
    pub fn wal_config(&self) -> Option<&WalConfig> {
        self.wal_config.as_ref()
    }

    /// Access the executor powering async subsystems.
    pub(crate) fn executor(&self) -> &Arc<E> {
        &self.executor
    }

    /// Allocate the next commit timestamp for WAL/autocommit flows.
    pub(crate) fn next_commit_ts(&mut self) -> Timestamp {
        self.commit_clock.next()
    }

    /// Access the active WAL handle, if any.
    pub(crate) fn wal_handle(&self) -> Option<&WalHandle<E>> {
        self.wal.as_ref()
    }

    /// Replace the WAL handle (used by WAL extension methods).
    pub(crate) fn set_wal_handle(&mut self, handle: Option<WalHandle<E>>) {
        self.wal = handle;
    }

    /// Replace the stored WAL configuration.
    pub(crate) fn set_wal_config(&mut self, cfg: Option<WalConfig>) {
        self.wal_config = cfg;
    }

    /// Number of immutable segments attached to this DB (oldest..newest).
    pub fn num_immutable_segments(&self) -> usize {
        self.immutables.len()
    }

    /// Plan and flush immutable segments into a Parquet-backed SSTable.
    pub async fn flush_immutables_with_descriptor(
        &self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<SsTable<M>, SsTableError>
    where
        M: Sized,
    {
        if self.immutables.is_empty() {
            return Err(SsTableError::NoImmutableSegments);
        }
        let mut builder = SsTableBuilder::<M>::new(config, descriptor);
        for seg in &self.immutables {
            builder.add_immutable(seg)?;
        }
        builder.finish().await
    }

    // Key-only merged scans have been removed.
}

// Segment management (generic, zero-cost)
impl<M: Mode, E: Executor + Timer> DB<M, E> {
    #[allow(dead_code)]
    pub(crate) fn add_immutable(&mut self, seg: Immutable<M>) {
        self.immutables.push(seg);
    }

    /// Set or replace the sealing policy used by this DB.
    pub fn set_seal_policy(&mut self, policy: Box<dyn SealPolicy + Send + Sync>) {
        self.policy = policy;
    }
}

async fn insert_dyn_wal_batch<E>(
    db: &mut DB<DynMode, E>,
    batch: RecordBatch,
    tombstones: Vec<bool>,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    validate_record_batch_schema(db, &batch)?;
    validate_tombstone_bitmap(&batch, &tombstones)?;
    let commit_ts = db.next_commit_ts();
    if let Some(handle) = db.wal_handle().cloned() {
        let ticket = handle
            .append(&batch, &tombstones, commit_ts)
            .await
            .map_err(KeyExtractError::from)?;
        ticket.durable().await.map_err(KeyExtractError::from)?;
    }
    apply_dyn_wal_batch(db, batch, tombstones, commit_ts)
}

async fn insert_dyn_wal_batches<E>(
    db: &mut DB<DynMode, E>,
    batches: Vec<(RecordBatch, Vec<bool>)>,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    for (batch, tombstones) in batches {
        insert_dyn_wal_batch(db, batch, tombstones).await?;
    }
    Ok(())
}

fn apply_dyn_wal_batch<E>(
    db: &mut DB<DynMode, E>,
    batch: RecordBatch,
    tombstones: Vec<bool>,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    validate_record_batch_schema(db, &batch)?;
    validate_tombstone_bitmap(&batch, &tombstones)?;
    let bits = tombstones;
    db.mem
        .insert_batch_with_ts(db.mode.extractor.as_ref(), batch, commit_ts, move |row| {
            bits[row]
        })?;
    db.maybe_seal_after_insert()?;
    Ok(())
}

fn validate_record_batch_schema<E>(
    db: &DB<DynMode, E>,
    batch: &RecordBatch,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    if db.mode.schema.as_ref() != batch.schema().as_ref() {
        return Err(KeyExtractError::SchemaMismatch {
            expected: db.mode.schema.clone(),
            actual: batch.schema(),
        });
    }
    Ok(())
}

fn validate_tombstone_bitmap(
    batch: &RecordBatch,
    tombstones: &[bool],
) -> Result<(), KeyExtractError> {
    if batch.num_rows() != tombstones.len() {
        return Err(KeyExtractError::TombstoneLengthMismatch {
            expected: batch.num_rows(),
            actual: tombstones.len(),
        });
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use fusio::executor::BlockingExecutor;
    use futures::executor::block_on;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        inmem::policy::BatchesThreshold,
        test_util::build_batch,
        wal::{
            WalPayload, append_tombstone_column,
            frame::{INITIAL_FRAME_SEQ, encode_payload},
        },
    };

    #[test]
    fn ingest_tombstone_length_mismatch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("config");
        let executor = Arc::new(BlockingExecutor::default());
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).expect("db");

        let err = block_on(db.ingest_with_tombstones(batch, vec![])).expect_err("length mismatch");
        assert!(matches!(
            err,
            KeyExtractError::TombstoneLengthMismatch {
                expected: 1,
                actual: 0
            }
        ));
    }

    #[test]
    fn ingest_batch_with_tombstones_marks_versions_and_visibility() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");
        let executor = Arc::new(BlockingExecutor::default());
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).expect("db");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let result = block_on(db.ingest_with_tombstones(batch, vec![false, true]));
        result.expect("ingest");

        let chain_k1 = db
            .mem
            .inspect_versions(&KeyDyn::from("k1"))
            .expect("chain k1");
        assert_eq!(chain_k1.len(), 1);
        assert!(!chain_k1[0].1);

        let chain_k2 = db
            .mem
            .inspect_versions(&KeyDyn::from("k2"))
            .expect("chain k2");
        assert_eq!(chain_k2.len(), 1);
        assert!(chain_k2[0].1);

        let all: RangeSet<KeyDyn> = RangeSet::all();
        let visible: Vec<String> = db
            .scan_mutable_rows(&all)
            .map(|row| match &row.0[0] {
                Some(typed_arrow_dyn::DynCell::Str(s)) => s.clone(),
                _ => panic!("unexpected cell variant"),
            })
            .collect();
        assert_eq!(visible, vec!["k1".to_string()]);
    }

    #[test]
    fn dynamic_seal_on_batches_threshold() {
        // Build a simple schema: id: Utf8 (key), v: Int32
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // Build one batch with two rows
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");

        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::new(BlockingExecutor::default())).expect("schema ok");
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));
        assert_eq!(db.num_immutable_segments(), 0);
        block_on(db.ingest(batch)).expect("insert batch");
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[test]
    fn dynamic_new_from_metadata_field_marker() {
        use std::collections::HashMap;
        // Schema: mark id with field-level metadata tonbo.key = true
        let mut fm = HashMap::new();
        fm.insert("tonbo.key".to_string(), "true".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(fm);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata key config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::new(BlockingExecutor::default())).expect("metadata key");

        // Build one batch and insert to ensure extractor wired
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_schema_level() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "id".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]).with_metadata(sm));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("schema metadata config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::new(BlockingExecutor::default())).expect("schema metadata key");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("x".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("y".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_conflicts_and_missing() {
        use std::collections::HashMap;
        // Conflict: two fields marked as key
        let mut fm1 = HashMap::new();
        fm1.insert("tonbo.key".to_string(), "true".to_string());
        let mut fm2 = HashMap::new();
        fm2.insert("tonbo.key".to_string(), "1".to_string());
        let f1 = Field::new("id1", DataType::Utf8, false).with_metadata(fm1);
        let f2 = Field::new("id2", DataType::Utf8, false).with_metadata(fm2);
        let schema_conflict = std::sync::Arc::new(Schema::new(vec![f1, f2]));
        assert!(DynModeConfig::from_metadata(schema_conflict).is_err());

        // Missing: no markers at field or schema level
        let schema_missing = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        assert!(DynModeConfig::from_metadata(schema_missing).is_err());
    }

    #[test]
    fn dynamic_composite_from_field_ordinals_and_scan() {
        use std::collections::HashMap;
        // Fields: id (Utf8, ord 1), ts (Int64, ord 2), v (Int32)
        let mut m1 = HashMap::new();
        m1.insert("tonbo.key".to_string(), "1".to_string());
        let mut m2 = HashMap::new();
        m2.insert("tonbo.key".to_string(), "2".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
        let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::new(BlockingExecutor::default()))
                .expect("composite field metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert batch");

        use std::ops::Bound as B;
        let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(5i64)]);
        let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
        let rs = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(lo),
            B::Included(hi),
        )]);
        let got: Vec<(String, i64)> = db
            .scan_mutable_rows(&rs)
            .map(|row| match (&row.0[0], &row.0[1]) {
                (
                    Some(typed_arrow_dyn::DynCell::Str(s)),
                    Some(typed_arrow_dyn::DynCell::I64(ts)),
                ) => (s.clone(), *ts),
                _ => panic!("unexpected row content"),
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }

    #[test]
    fn dynamic_composite_from_schema_list_and_scan() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_ts = Field::new("ts", DataType::Int64, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "[\"id\", \"ts\"]".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]).with_metadata(sm));
        let config = DynModeConfig::from_metadata(schema.clone()).expect("metadata config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::new(BlockingExecutor::default()))
                .expect("composite schema metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert batch");

        use std::ops::Bound as B;
        let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(1i64)]);
        let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
        let rs = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(lo),
            B::Included(hi),
        )]);
        let got: Vec<(String, i64)> = db
            .scan_mutable_rows(&rs)
            .map(|row| match (&row.0[0], &row.0[1]) {
                (
                    Some(typed_arrow_dyn::DynCell::Str(s)),
                    Some(typed_arrow_dyn::DynCell::I64(ts)),
                ) => (s.clone(), *ts),
                _ => panic!("unexpected row content"),
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }

    #[test]
    fn recover_replays_commit_timestamps_and_advances_clock() {
        use std::{
            fs,
            time::{Duration, SystemTime},
        };

        use tokio::runtime::Runtime;

        let wal_dir = std::env::temp_dir().join(format!(
            "tonbo-replay-test-{}",
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&wal_dir).expect("create wal dir");

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch");

        let wal_batch =
            append_tombstone_column(&batch, Some(&[true])).expect("batch with tombstone");
        let payload = WalPayload::DynBatch {
            batch: wal_batch,
            commit_ts: Timestamp::new(42),
        };
        let frames = encode_payload(payload, 7).expect("encode");
        let mut seq = INITIAL_FRAME_SEQ;
        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend_from_slice(&frame.into_bytes(seq));
            seq += 1;
        }
        fs::write(wal_dir.join("wal-00000000000000000001.tonwal"), bytes).expect("write wal");

        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");
        let mut cfg = WalConfig::default();
        cfg.dir = fusio::path::Path::from_filesystem_path(&wal_dir).expect("wal fusio path");
        let executor = Arc::new(BlockingExecutor::default());
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let runtime = Runtime::new().expect("tokio runtime");
        let mut db: DB<DynMode, BlockingExecutor> = runtime
            .block_on(DB::recover_with_wal(config, executor.clone(), cfg))
            .expect("recover");
        runtime.shutdown_timeout(Duration::from_secs(0));

        // Replayed version retains commit_ts 42 and tombstone state.
        let chain = db.mem.inspect_versions(&KeyDyn::from("k")).expect("chain");
        assert_eq!(chain, vec![(Timestamp::new(42), true)]);

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(KeyDyn::from("k")),
            B::Included(KeyDyn::from("k")),
        )]);
        let visible: Vec<_> = db
            .scan_mutable_rows_at(&ranges, Timestamp::new(50))
            .collect();
        assert!(visible.is_empty());

        // New ingest should advance to > 42 (next clock tick).
        let new_batch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");
        block_on(db.ingest(new_batch)).expect("ingest new");

        let chain = db.mem.inspect_versions(&KeyDyn::from("k")).expect("chain");
        assert_eq!(
            chain,
            vec![(Timestamp::new(42), true), (Timestamp::new(43), false)]
        );

        fs::remove_dir_all(&wal_dir).expect("cleanup");
    }
}
