use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::take::take;
use fusio::executor::{Executor, Timer};
use web_time::Instant;

use crate::{
    db::DB,
    extractor::KeyExtractError,
    inmem::{
        immutable::{Immutable, memtable::MVCC_COMMIT_COL},
        mutable::DynMem,
        policy::{SealDecision, StatsProvider},
    },
    key::KeyOwned,
    manifest::{TableId, TonboManifest},
    mode::{DynMode, Mode},
    mvcc::Timestamp,
    wal::{
        WalConfig as RuntimeWalConfig, WalError, WalHandle,
        frame::{DynAppendEvent, WalEvent},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalFrameRange {
    pub(crate) first: u64,
    pub(crate) last: u64,
}

impl WalFrameRange {
    fn include_seq(&mut self, seq: u64) {
        if seq < self.first {
            self.first = seq;
        }
        if seq > self.last {
            self.last = seq;
        }
    }

    pub(crate) fn extend(&mut self, other: &WalFrameRange) {
        self.include_seq(other.first);
        self.include_seq(other.last);
    }
}

#[derive(Default)]
struct WalRangeAccumulator {
    range: Option<WalFrameRange>,
}

impl WalRangeAccumulator {
    fn observe_range(&mut self, first: u64, last: u64) {
        debug_assert!(first <= last, "wal frame range inverted");
        match self.range.as_mut() {
            Some(range) => {
                range.include_seq(first);
                range.include_seq(last);
            }
            None => self.range = Some(WalFrameRange { first, last }),
        }
    }

    fn into_range(self) -> Option<WalFrameRange> {
        self.range
    }
}

#[derive(Clone)]
pub(crate) struct TxnWalPublishContext {
    pub(crate) manifest: TonboManifest,
    pub(crate) manifest_table: TableId,
    pub(crate) wal_config: Option<RuntimeWalConfig>,
    pub(crate) mutable_wal_range: Arc<Mutex<Option<WalFrameRange>>>,
    pub(crate) prev_live_floor: Option<u64>,
}

pub(crate) struct SealState<M: Mode> {
    pub(crate) immutables: Vec<Arc<Immutable<M>>>,
    pub(crate) immutable_wal_ranges: Vec<Option<WalFrameRange>>,
    pub(crate) last_seal_at: Option<Instant>,
}

impl<M: Mode> Default for SealState<M> {
    fn default() -> Self {
        Self {
            immutables: Vec::new(),
            immutable_wal_ranges: Vec::new(),
            last_seal_at: None,
        }
    }
}

type PendingWalTxns = HashMap<u64, PendingWalTxn>;

enum PendingWalPayload {
    Upsert {
        batch: RecordBatch,
        commit_ts_column: ArrayRef,
        commit_ts_hint: Option<Timestamp>,
    },
    Delete {
        batch: RecordBatch,
        commit_ts_hint: Option<Timestamp>,
    },
}

#[derive(Default)]
struct PendingWalTxn {
    payloads: Vec<PendingWalPayload>,
}

impl PendingWalTxn {
    fn push(&mut self, payload: PendingWalPayload) {
        self.payloads.push(payload);
    }

    fn into_payloads(self) -> Vec<PendingWalPayload> {
        self.payloads
    }

    fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
}

impl<E> DB<DynMode, E>
where
    E: Executor + Timer,
{
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.mode.schema
    }

    /// Wrap this database in an executor-provided read-write lock for shared transactional use.
    pub fn into_shared(self) -> Arc<DB<DynMode, E>> {
        Arc::new(self)
    }

    pub(crate) fn insert_into_mutable(
        &self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        self.insert_with_seal_retry(|mem| {
            mem.insert_batch(self.mode.extractor.as_ref(), batch.clone(), commit_ts)
        })
    }

    pub(crate) fn replay_wal_events(
        &mut self,
        events: Vec<WalEvent>,
    ) -> Result<Option<Timestamp>, KeyExtractError> {
        let mut last_commit_ts: Option<Timestamp> = None;
        let mut pending: PendingWalTxns = HashMap::new();
        for event in events {
            match event {
                WalEvent::TxnBegin { provisional_id } => {
                    pending.entry(provisional_id).or_default();
                }
                WalEvent::DynAppend {
                    provisional_id,
                    payload,
                } => {
                    let DynAppendEvent {
                        batch,
                        commit_ts_hint,
                        commit_ts_column,
                    } = payload;
                    pending
                        .entry(provisional_id)
                        .or_default()
                        .push(PendingWalPayload::Upsert {
                            batch,
                            commit_ts_column,
                            commit_ts_hint,
                        });
                }
                WalEvent::DynDelete {
                    provisional_id,
                    payload,
                } => {
                    pending
                        .entry(provisional_id)
                        .or_default()
                        .push(PendingWalPayload::Delete {
                            batch: payload.batch,
                            commit_ts_hint: payload.commit_ts_hint,
                        });
                }
                WalEvent::TxnCommit {
                    provisional_id,
                    commit_ts,
                } => {
                    if let Some(txn) = pending.remove(&provisional_id) {
                        if txn.is_empty() {
                            continue;
                        }
                        for entry in txn.into_payloads() {
                            match entry {
                                PendingWalPayload::Upsert {
                                    batch,
                                    commit_ts_column,
                                    commit_ts_hint,
                                } => {
                                    if let Some(hint) = commit_ts_hint {
                                        debug_assert_eq!(
                                            hint, commit_ts,
                                            "commit timestamp derived from append payload diverged"
                                        );
                                    }
                                    apply_dyn_wal_batch(self, batch, commit_ts_column, commit_ts)?;
                                    self.maybe_seal_after_insert()?;
                                }
                                PendingWalPayload::Delete {
                                    batch,
                                    commit_ts_hint,
                                } => {
                                    if let Some(hint) = commit_ts_hint {
                                        debug_assert_eq!(
                                            hint, commit_ts,
                                            "commit timestamp derived from delete payload diverged"
                                        );
                                    }
                                    apply_dyn_delete_wal_batch(self, batch, commit_ts)?;
                                    self.maybe_seal_after_insert()?;
                                }
                            }
                        }
                        last_commit_ts = Some(match last_commit_ts {
                            Some(prev) => prev.max(commit_ts),
                            None => commit_ts,
                        });
                    }
                }
                WalEvent::TxnAbort { provisional_id } => {
                    pending.remove(&provisional_id);
                }
                WalEvent::SealMarker => {
                    // TODO: implement once transactional semantics are wired up.
                }
            }
        }

        Ok(last_commit_ts)
    }

    fn insert_with_seal_retry<F>(&self, mut op: F) -> Result<(), KeyExtractError>
    where
        F: FnMut(&mut DynMem) -> Result<(), KeyExtractError>,
    {
        loop {
            let mut mem = self.mem_write();
            match op(&mut mem) {
                Ok(()) => return Ok(()),
                Err(KeyExtractError::MemtableFull { .. }) => {
                    drop(mem);
                    self.seal_mutable_now()?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn seal_mutable_now(&self) -> Result<(), KeyExtractError> {
        let seg_opt = {
            let mut mem = self.mem_write();
            mem.seal_into_immutable(&self.mode.schema, self.mode.extractor.as_ref())?
        };
        if let Some(seg) = seg_opt {
            let wal_range = self.take_mutable_wal_range();
            self.add_immutable(seg, wal_range);
            let mut seal = self.seal_state_lock();
            seal.last_seal_at = Some(Instant::now());
        }
        Ok(())
    }

    pub(crate) fn apply_committed_batch(
        &self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        let commit_array: ArrayRef =
            Arc::new(UInt64Array::from(vec![commit_ts.get(); batch.num_rows()]));
        apply_dyn_wal_batch(self, batch, commit_array, commit_ts)
    }

    pub(crate) fn apply_committed_deletes(
        &self,
        batch: RecordBatch,
    ) -> Result<(), KeyExtractError> {
        let mut mem = self.mem_write();
        mem.insert_delete_batch(self.mode.delete_projection.as_ref(), batch)
    }

    pub(crate) fn mutable_has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        self.mem_read().has_conflict(key, snapshot_ts)
    }

    pub(crate) fn immutable_has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        self.seal_state_lock()
            .immutables
            .iter()
            .any(|segment| segment.has_conflict(key, snapshot_ts))
    }

    pub(crate) fn maybe_seal_after_insert(&self) -> Result<(), KeyExtractError> {
        let last_seal = { self.seal_state_lock().last_seal_at };
        let since = last_seal.map(|t| t.elapsed());
        let stats = { self.mem_read().build_stats(since) };

        if let SealDecision::Seal(_reason) = self.policy.evaluate(&stats) {
            self.seal_mutable_now()?;
        }
        Ok(())
    }

    /// Ingest a batch along with its tombstone bitmap, routing through the WAL when enabled.
    pub async fn ingest_with_tombstones(
        &self,
        batch: RecordBatch,
        tombstones: Vec<bool>,
    ) -> Result<(), KeyExtractError> {
        insert_dyn_wal_batch(self, batch, tombstones).await
    }

    /// Ingest multiple batches, each paired with a tombstone bitmap.
    pub async fn ingest_many_with_tombstones(
        &self,
        batches: Vec<(RecordBatch, Vec<bool>)>,
    ) -> Result<(), KeyExtractError> {
        insert_dyn_wal_batches(self, batches).await
    }
}

impl<M, E> DB<M, E>
where
    M: Mode,
    M::Key: Eq + std::hash::Hash + Clone,
    E: Executor + Timer,
{
    /// Attach WAL configuration prior to enabling durability.
    pub fn with_wal_config(mut self, cfg: RuntimeWalConfig) -> Self {
        self.wal_config = Some(cfg);
        self
    }

    /// Access the configured WAL settings, if any.
    pub fn wal_config(&self) -> Option<&RuntimeWalConfig> {
        self.wal_config.as_ref()
    }

    pub(crate) fn wal_handle(&self) -> Option<&WalHandle<E>> {
        self.wal.as_ref()
    }

    pub(crate) fn set_wal_handle(&mut self, handle: Option<WalHandle<E>>) {
        self.wal = handle;
    }

    pub(crate) fn set_wal_config(&mut self, cfg: Option<RuntimeWalConfig>) {
        self.wal_config = cfg;
    }

    pub(crate) async fn disable_wal_async(&mut self) -> Result<(), WalError> {
        if let Some(handle) = self.wal_handle().cloned() {
            self.set_wal_handle(None);
            handle.shutdown().await?;
        } else {
            self.set_wal_handle(None);
        }
        Ok(())
    }

    pub(crate) fn wal_live_frame_floor(&self) -> Option<u64> {
        let mutable_floor = self.mutable_wal_range_snapshot().map(|range| range.first);
        let seal = self.seal_state_lock();
        let immutable_floor = seal
            .immutable_wal_ranges
            .iter()
            .filter_map(|range| range.as_ref().map(|r| r.first))
            .min();

        match (mutable_floor, immutable_floor) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        }
    }

    pub(crate) fn txn_publish_context(&self, prev_live_floor: Option<u64>) -> TxnWalPublishContext {
        TxnWalPublishContext {
            manifest: self.manifest.clone(),
            manifest_table: self.manifest_table,
            wal_config: self.wal_config.clone(),
            mutable_wal_range: Arc::clone(&self.mutable_wal_range),
            prev_live_floor,
        }
    }

    pub(crate) fn observe_mutable_wal_span(&self, first_seq: u64, last_seq: u64) {
        let (first, last) = if first_seq <= last_seq {
            (first_seq, last_seq)
        } else {
            (last_seq, first_seq)
        };
        self.record_mutable_wal_range(WalFrameRange { first, last });
    }

    pub(crate) fn add_immutable(&self, seg: Immutable<M>, wal_range: Option<WalFrameRange>) {
        let mut seal = self.seal_state_lock();
        seal.immutables.push(Arc::new(seg));
        seal.immutable_wal_ranges.push(wal_range);
    }

    fn record_mutable_wal_range(&self, range: WalFrameRange) {
        let mut guard = self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned");
        match &mut *guard {
            Some(existing) => existing.extend(&range),
            None => *guard = Some(range),
        }
    }

    pub(crate) fn take_mutable_wal_range(&self) -> Option<WalFrameRange> {
        self.mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned")
            .take()
    }

    pub(crate) fn mutable_wal_range_snapshot(&self) -> Option<WalFrameRange> {
        *self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned")
    }

    pub(crate) fn set_mutable_wal_range(&self, value: Option<WalFrameRange>) {
        *self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned") = value;
    }
}

async fn insert_dyn_wal_batch<E>(
    db: &DB<DynMode, E>,
    batch: RecordBatch,
    tombstones: Vec<bool>,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    validate_record_batch_schema(db, &batch)?;
    validate_vec_tombstone_bitmap(&batch, &tombstones)?;

    if batch.num_rows() == 0 {
        return Ok(());
    }

    let commit_ts = db.next_commit_ts();

    let (upsert_batch, delete_batch) =
        partition_batch_for_mutations(&db.mode, batch, &tombstones, commit_ts)?;

    if upsert_batch.is_none() && delete_batch.is_none() {
        return Ok(());
    }

    let mut wal_range: Option<WalFrameRange> = None;
    if let Some(handle) = db.wal_handle().cloned() {
        let provisional_id = handle.next_provisional_id();
        let mut append_tickets = Vec::new();
        if let Some(ref batch) = upsert_batch {
            let ticket = handle
                .txn_append(provisional_id, batch, commit_ts)
                .await
                .map_err(KeyExtractError::from)?;
            append_tickets.push(ticket);
        }
        if let Some(ref batch) = delete_batch {
            let ticket = handle
                .txn_append_delete(provisional_id, batch.clone())
                .await
                .map_err(KeyExtractError::from)?;
            append_tickets.push(ticket);
        }
        let mut tracker = WalRangeAccumulator::default();
        for ticket in append_tickets {
            let ack = ticket.durable().await.map_err(KeyExtractError::from)?;
            tracker.observe_range(ack.first_seq, ack.last_seq);
        }
        let commit_ticket = handle
            .txn_commit(provisional_id, commit_ts)
            .await
            .map_err(KeyExtractError::from)?;
        let commit_ack = commit_ticket
            .durable()
            .await
            .map_err(KeyExtractError::from)?;
        tracker.observe_range(commit_ack.first_seq, commit_ack.last_seq);
        wal_range = tracker.into_range();
    }

    let mutated = upsert_batch.is_some() || delete_batch.is_some();
    if mutated {
        db.insert_with_seal_retry(|mem| {
            if let Some(ref batch) = upsert_batch {
                mem.insert_batch(db.mode.extractor.as_ref(), batch.clone(), commit_ts)?;
            }
            if let Some(ref batch) = delete_batch {
                mem.insert_delete_batch(db.mode.delete_projection.as_ref(), batch.clone())?;
            }
            Ok(())
        })?;
        if let Some(range) = wal_range {
            db.record_mutable_wal_range(range);
        }
        db.maybe_seal_after_insert()?;
    }
    Ok(())
}

async fn insert_dyn_wal_batches<E>(
    db: &DB<DynMode, E>,
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

pub(crate) fn apply_dyn_wal_batch<E>(
    db: &DB<DynMode, E>,
    batch: RecordBatch,
    commit_ts_column: ArrayRef,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    let schema_contains_tombstone = db
        .mode
        .schema
        .fields()
        .iter()
        .any(|field| field.name() == crate::inmem::immutable::memtable::MVCC_TOMBSTONE_COL);
    let batch_contains_tombstone = batch
        .schema()
        .fields()
        .iter()
        .any(|field| field.name() == crate::inmem::immutable::memtable::MVCC_TOMBSTONE_COL);
    if batch_contains_tombstone && !schema_contains_tombstone {
        return Err(KeyExtractError::from(WalError::Codec(
            "wal batch contained unexpected _tombstone column".into(),
        )));
    }
    validate_record_batch_schema(db, &batch)?;
    let commit_array = commit_ts_column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| KeyExtractError::from(WalError::Codec("_commit_ts column not u64".into())))?
        .clone();
    if commit_array.null_count() > 0 {
        return Err(KeyExtractError::from(WalError::Codec(
            "commit_ts column contained null".into(),
        )));
    }
    if commit_array
        .iter()
        .any(|value| value.map(|v| v != commit_ts.get()).unwrap_or(true))
    {
        return Err(KeyExtractError::from(WalError::Codec(
            "commit_ts column mismatch commit timestamp".into(),
        )));
    }

    db.insert_with_seal_retry(|mem| {
        mem.insert_batch_with_mvcc(
            db.mode.extractor.as_ref(),
            batch.clone(),
            commit_array.clone(),
        )
    })?;
    Ok(())
}

fn apply_dyn_delete_wal_batch<E>(
    db: &DB<DynMode, E>,
    batch: RecordBatch,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    validate_delete_batch_schema(db, &batch)?;
    validate_delete_commit_ts(&batch, commit_ts)?;
    {
        let mut mem = db.mem_write();
        mem.insert_delete_batch(db.mode.delete_projection.as_ref(), batch)?;
    }
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

fn validate_vec_tombstone_bitmap(
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

fn validate_delete_batch_schema<E>(
    db: &DB<DynMode, E>,
    batch: &RecordBatch,
) -> Result<(), KeyExtractError>
where
    E: Executor + Timer,
{
    if db.mode.delete_schema.as_ref() != batch.schema().as_ref() {
        return Err(KeyExtractError::SchemaMismatch {
            expected: db.mode.delete_schema.clone(),
            actual: batch.schema(),
        });
    }
    Ok(())
}

fn validate_delete_commit_ts(
    batch: &RecordBatch,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError> {
    let schema = batch.schema();
    let commit_idx = schema
        .fields()
        .iter()
        .position(|field| field.name() == MVCC_COMMIT_COL)
        .ok_or_else(|| {
            KeyExtractError::from(WalError::Codec("_commit_ts column missing".into()))
        })?;
    let commit_array = batch
        .column(commit_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            KeyExtractError::from(WalError::Codec("_commit_ts column not u64".into()))
        })?;
    if commit_array.null_count() > 0 {
        return Err(KeyExtractError::from(WalError::Codec(
            "delete payload commit_ts column contained null".into(),
        )));
    }
    if commit_array
        .iter()
        .any(|value| value.map(|v| v != commit_ts.get()).unwrap_or(true))
    {
        return Err(KeyExtractError::from(WalError::Codec(
            "delete payload commit_ts column mismatch commit timestamp".into(),
        )));
    }
    Ok(())
}

fn partition_batch_for_mutations(
    mode: &DynMode,
    batch: RecordBatch,
    tombstones: &[bool],
    commit_ts: Timestamp,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>), KeyExtractError> {
    if batch.num_rows() == 0 {
        return Ok((None, None));
    }
    let mut upsert_indices = Vec::new();
    let mut delete_indices = Vec::new();
    for (idx, tombstone) in tombstones.iter().enumerate() {
        if *tombstone {
            delete_indices.push(idx as u32);
        } else {
            upsert_indices.push(idx as u32);
        }
    }
    let delete_batch = if delete_indices.is_empty() {
        None
    } else {
        Some(build_delete_batch(
            mode,
            &batch,
            &delete_indices,
            commit_ts,
            None,
        )?)
    };
    let upsert_batch = if delete_indices.is_empty() {
        Some(batch)
    } else if upsert_indices.is_empty() {
        None
    } else {
        Some(take_full_batch(&batch, &upsert_indices).map_err(KeyExtractError::Arrow)?)
    };
    Ok((upsert_batch, delete_batch))
}

fn build_delete_batch(
    mode: &DynMode,
    batch: &RecordBatch,
    delete_indices: &[u32],
    commit_ts: Timestamp,
    commit_ts_column: Option<&UInt64Array>,
) -> Result<RecordBatch, KeyExtractError> {
    let idx_array = UInt32Array::from(delete_indices.to_vec());
    let mut columns = Vec::with_capacity(mode.extractor.key_indices().len() + 1);
    for &col_idx in mode.extractor.key_indices() {
        let column = batch.column(col_idx);
        let taken = take(column.as_ref(), &idx_array, None).map_err(KeyExtractError::Arrow)?;
        columns.push(taken);
    }
    let commit_values: ArrayRef = if let Some(column) = commit_ts_column {
        take(column, &idx_array, None).map_err(KeyExtractError::Arrow)?
    } else {
        Arc::new(UInt64Array::from(vec![
            commit_ts.get();
            delete_indices.len()
        ])) as ArrayRef
    };
    columns.push(commit_values);
    RecordBatch::try_new(mode.delete_schema.clone(), columns).map_err(KeyExtractError::Arrow)
}

fn take_full_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ArrowError> {
    let idx_array = UInt32Array::from(indices.to_vec());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), &idx_array, None)?);
    }
    RecordBatch::try_new(batch.schema(), columns)
}
