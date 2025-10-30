//! Generic DB parametrized by a `Mode` implementation.
//!
//! At the moment Tonbo only ships with the dynamic runtime-schema mode. The
//! trait-driven structure remains so that compile-time typed dispatch can be
//! reintroduced without reshaping the API.

use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::executor::{Executor, Timer};
use futures::executor::block_on;

pub use crate::mode::{DynMode, DynModeConfig, Mode};
use crate::{
    fs::FileId,
    inmem::{
        immutable::Immutable,
        mutable::MutableLayout,
        policy::{SealDecision, SealPolicy, StatsProvider},
    },
    manifest::{
        InMemoryManifest, ManifestError, SstEntry, TableId, VersionEdit, WalSegmentRef,
        init_in_memory_manifest,
    },
    mvcc::{CommitClock, Timestamp},
    ondisk::sstable::{SsTable, SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableError},
    record::extract::{KeyDyn, KeyExtractError},
    scan::RangeSet,
    wal::{WalConfig, WalHandle, frame::WalEvent, manifest_ext, replay::Replayer},
};

type PendingWalBatches = HashMap<u64, Vec<(RecordBatch, Vec<bool>, Option<Timestamp>)>>;

/// A DB parametrized by a mode `M` that defines key, payload and insert interface.
pub struct DB<M: Mode, E: Executor + Timer + Send + Sync> {
    mode: M,
    mem: M::Mutable,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest)
    immutables: Vec<Immutable<M>>,
    immutable_wal_ids: Vec<Option<Vec<FileId>>>,
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
    manifest: InMemoryManifest,
    manifest_table: TableId,
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
        let mut last_commit_ts: Option<Timestamp> = None;
        let mut pending: PendingWalBatches = HashMap::new();
        for event in events {
            match event {
                WalEvent::DynAppend {
                    provisional_id,
                    batch,
                    commit_ts_hint,
                    tombstones,
                    ..
                } => {
                    pending.entry(provisional_id).or_default().push((
                        batch,
                        tombstones,
                        commit_ts_hint,
                    ));
                }
                WalEvent::TxnCommit {
                    provisional_id,
                    commit_ts,
                } => {
                    if let Some(batches) = pending.remove(&provisional_id) {
                        for (batch, tombstones, hinted_ts) in batches {
                            if let Some(hint) = hinted_ts {
                                debug_assert_eq!(
                                    hint, commit_ts,
                                    "commit timestamp derived from append payload diverged"
                                );
                            }
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
                self.add_immutable(seg);
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
        let (manifest, manifest_table) =
            init_in_memory_manifest(0).expect("manifest initialization");
        Ok(Self {
            mode,
            mem,
            immutables: Vec::new(),
            immutable_wal_ids: Vec::new(),
            policy: crate::inmem::policy::default_policy(),
            last_seal_at: None,
            executor,
            wal: None,
            wal_config: None,
            commit_clock: CommitClock::default(),
            manifest,
            manifest_table,
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

        let _wal_floor = db.manifest_wal_floor();
        // TODO: filter WAL events newer than `_wal_floor` once manifest persistence wires real
        // references.

        let last_commit_ts = M::replay_wal(&mut db, events)?;
        if let Some(ts) = last_commit_ts {
            db.commit_clock.advance_to_at_least(ts.saturating_add(1));
        }

        Ok(db)
    }

    /// Unified ingestion entry point using the mode's insertion contract.
    pub async fn ingest(&mut self, input: M::InsertInput) -> Result<(), KeyExtractError>
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
        self.commit_clock.alloc()
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

    fn manifest_wal_floor(&self) -> Option<WalSegmentRef> {
        block_on(self.manifest.wal_floor(self.manifest_table))
            .ok()
            .flatten()
    }

    /// Number of immutable segments attached to this DB (oldest..newest).
    pub fn num_immutable_segments(&self) -> usize {
        self.immutables.len()
    }

    /// Plan and flush immutable segments into a Parquet-backed SSTable.
    pub async fn flush_immutables_with_descriptor(
        &mut self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<SsTable<M>, SsTableError>
    where
        M: Sized + Mode<ImmLayout = RecordBatch, Key = KeyDyn>,
    {
        if self.immutables.is_empty() {
            return Err(SsTableError::NoImmutableSegments);
        }
        let mut builder = SsTableBuilder::<M>::new(config, descriptor);
        for seg in &self.immutables {
            builder.add_immutable(seg)?;
        }
        let wal_ids_flat: Vec<FileId> = self
            .immutable_wal_ids
            .iter()
            .filter_map(|ids| ids.as_ref())
            .flat_map(|ids| ids.clone())
            .collect();
        let wal_ids = (!wal_ids_flat.is_empty()).then_some(wal_ids_flat);
        builder.set_wal_ids(wal_ids);

        match builder.finish().await {
            Ok(table) => {
                // Persist Sst change into manifest
                let descriptor_ref = table.descriptor();
                let data_path = descriptor_ref.data_path().cloned().ok_or_else(|| {
                    SsTableError::Manifest(ManifestError::Invariant(
                        "sst descriptor missing data path",
                    ))
                })?;
                let mvcc_path = descriptor_ref.mvcc_path().cloned().ok_or_else(|| {
                    SsTableError::Manifest(ManifestError::Invariant(
                        "sst descriptor missing mvcc path",
                    ))
                })?;
                let wal_ids: Vec<FileId> = descriptor_ref
                    .wal_ids()
                    .map(|ids| ids.to_vec())
                    .unwrap_or_default();
                let stats = descriptor_ref.stats().cloned();
                let sst_entry = SstEntry::new(
                    descriptor_ref.id().clone(),
                    stats,
                    (!wal_ids.is_empty()).then_some(wal_ids.clone()),
                    data_path,
                    mvcc_path,
                );
                let mut edits = vec![VersionEdit::AddSsts {
                    level: descriptor_ref.level() as u32,
                    entries: vec![sst_entry],
                }];

                if !wal_ids.is_empty() {
                    let wal_refs = manifest_ext::mock_wal_segments(&wal_ids);
                    // For now just comput the floor WAL to be the latest one as we have
                    // successfully persist them in parquet table writer
                    let wal_floor = wal_refs
                        .iter()
                        .max_by_key(|segment| segment.seq())
                        .cloned()
                        .unwrap();
                    edits.push(VersionEdit::SetWalSegment { segment: wal_floor });
                }
                self.manifest
                    .apply_version_edits(self.manifest_table, &edits)
                    .await?;

                // Cleanup immutable memtable
                self.immutables.clear();
                self.immutable_wal_ids.clear();
                Ok(table)
            }
            Err(err) => Err(err),
        }
    }

    // Key-only merged scans have been removed.
}

// Segment management (generic, zero-cost)
impl<M: Mode, E: Executor + Timer> DB<M, E> {
    #[allow(dead_code)]
    pub(crate) fn add_immutable(&mut self, seg: Immutable<M>) {
        self.immutables.push(seg);
        self.immutable_wal_ids.push(None);
    }

    /// Record the WAL identifiers for the most recently sealed immutable.
    pub fn set_last_immutable_wal_ids(&mut self, wal_ids: Option<Vec<FileId>>) {
        if let Some(slot) = self.immutable_wal_ids.last_mut() {
            *slot = wal_ids;
        }
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
    db.mem
        .insert_batch_with_ts(db.mode.extractor.as_ref(), batch, commit_ts, move |row| {
            tombstones[row]
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
    use std::{sync::Arc, time::Duration};

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        disk::LocalFs,
        dynamic::DynFs,
        executor::{BlockingExecutor, tokio::TokioExecutor},
        path::Path,
    };
    use futures::{StreamExt, channel::mpsc, executor::block_on};
    use tokio::sync::{Mutex, oneshot};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        inmem::policy::BatchesThreshold,
        mvcc::Timestamp,
        ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableError, SsTableId},
        test_util::build_batch,
        wal::{
            WalPayload,
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
        let executor = Arc::new(BlockingExecutor);
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
        let executor = Arc::new(BlockingExecutor);
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
            DB::new(config, Arc::new(BlockingExecutor)).expect("schema ok");
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));
        assert_eq!(db.num_immutable_segments(), 0);
        block_on(db.ingest(batch)).expect("insert batch");
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingest_waits_for_wal_durable_ack() {
        use crate::wal::{WalAck, WalHandle, frame, writer};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");

        let executor = Arc::new(TokioExecutor::default());
        let (sender, mut receiver) = mpsc::channel(1);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ack_slot = Arc::new(Mutex::new(None));
        let (ack_ready_tx, ack_ready_rx) = oneshot::channel();
        let (release_ack_tx, release_ack_rx) = oneshot::channel();

        let ack_slot_clone = Arc::clone(&ack_slot);
        let join = executor.spawn(async move {
            if let Some(writer::WriterMsg::Enqueue { ack_tx, .. }) = receiver.next().await {
                {
                    let mut slot = ack_slot_clone.lock().await;
                    *slot = Some(ack_tx);
                }
                let _ = ack_ready_tx.send(());
                let _ = release_ack_rx.await;
                let ack = WalAck {
                    seq: frame::INITIAL_FRAME_SEQ,
                    bytes_flushed: 0,
                    elapsed: Duration::from_millis(0),
                };
                let mut slot = ack_slot_clone.lock().await;
                if let Some(sender) = slot.take() {
                    let _ = sender.send(Ok(ack));
                }
            }
            Ok(())
        });

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");

        let mut db: DB<DynMode, TokioExecutor> =
            DB::new(config, Arc::clone(&executor)).expect("db");
        let handle =
            WalHandle::test_from_parts(sender, queue_depth, join, frame::INITIAL_FRAME_SEQ);
        db.set_wal_handle(Some(handle));

        let mut ingest_future = Box::pin(db.ingest(batch));
        tokio::select! {
            _ = ack_ready_rx => {}
            res = &mut ingest_future => panic!("ingest finished early: {:?}", res),
        }

        release_ack_tx.send(()).expect("release ack");
        ingest_future.await.expect("ingest after ack");

        let ranges = RangeSet::<KeyDyn>::all();
        let rows: Vec<_> = db
            .scan_mutable_rows(&ranges)
            .map(|row| match &row.0[0] {
                Some(DynCell::Str(s)) => s.clone(),
                _ => panic!("unexpected row"),
            })
            .collect();
        assert_eq!(rows, vec!["k".to_string()]);
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
            DB::new(config, Arc::new(BlockingExecutor)).expect("metadata key");

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
            DB::new(config, Arc::new(BlockingExecutor)).expect("schema metadata key");

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
    fn flush_without_immutables_errors() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let config = DynModeConfig::from_key_name(schema.clone(), "id").expect("key name config");
        let executor = Arc::new(BlockingExecutor);
        let mut db: DB<DynMode, BlockingExecutor> = DB::new(config, executor).expect("db init");

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sstable_cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            fs,
            Path::from("/tmp/tonbo-flush-test"),
        ));
        let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);

        let result = block_on(db.flush_immutables_with_descriptor(sstable_cfg, descriptor.clone()));
        assert!(matches!(result, Err(SsTableError::NoImmutableSegments)));
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn flush_publishes_manifest_version() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");
        let executor = Arc::new(BlockingExecutor);
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let mut db: DB<DynMode, BlockingExecutor> =
            DB::new(config, Arc::clone(&executor)).expect("db");
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("valid dyn rows");
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async {
            db.ingest(batch).await.expect("ingest triggers seal");
        });
        assert_eq!(db.num_immutable_segments(), 1);

        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sstable_cfg = Arc::new(SsTableConfig::new(
            schema.clone(),
            fs,
            Path::from("/tmp/tonbo-flush-ok"),
        ));
        let descriptor = SsTableDescriptor::new(SsTableId::new(7), 0);

        let table = rt
            .block_on(async {
                db.flush_immutables_with_descriptor(sstable_cfg, descriptor.clone())
                    .await
            })
            .expect("flush succeeds");
        assert_eq!(db.num_immutable_segments(), 0);

        let snapshot = rt
            .block_on(async { db.manifest.snapshot_latest(db.manifest_table).await })
            .expect("manifest snapshot");
        assert_eq!(
            snapshot.head.last_manifest_txn,
            Some(Timestamp::new(1)),
            "first flush should publish manifest txn 1"
        );
        let latest = snapshot
            .latest_version
            .expect("latest version must exist after flush");
        assert_eq!(
            latest.commit_timestamp(),
            Timestamp::new(1),
            "latest version should reflect manifest txn 1"
        );
        assert_eq!(latest.ssts().len(), 1);
        assert_eq!(latest.ssts()[0].len(), 1);
        let recorded = &latest.ssts()[0][0];
        assert_eq!(recorded.sst_id(), descriptor.id());
        assert!(
            recorded.stats().is_some() || table.descriptor().stats().is_none(),
            "stats should propagate when available"
        );
        assert!(
            recorded.wal_segments().is_none(),
            "no WAL segments recorded since none were attached"
        );
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
            DB::new(config, Arc::new(BlockingExecutor)).expect("composite field metadata");

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
            DB::new(config, Arc::new(BlockingExecutor)).expect("composite schema metadata");

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

        let payload =
            WalPayload::new(batch.clone(), vec![true], Timestamp::new(42)).expect("payload");
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
        let executor = Arc::new(BlockingExecutor);
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
