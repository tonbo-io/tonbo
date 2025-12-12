use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::ArrowError;
use arrow_select::take::take;
use fusio::executor::{Executor, Instant, Timer};

use crate::{
    db::DbInner,
    extractor::KeyExtractError,
    inmem::{
        immutable::{ImmutableSegment, memtable::MVCC_COMMIT_COL},
        policy::{SealDecision, StatsProvider},
    },
    key::KeyOwned,
    manifest::{ManifestFs, TableId, TonboManifest},
    mvcc::Timestamp,
    ondisk::sstable::SsTableError,
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
pub(crate) struct TxnWalPublishContext<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) manifest: TonboManifest<FS, E>,
    pub(crate) manifest_table: TableId,
    pub(crate) wal_config: Option<RuntimeWalConfig>,
    pub(crate) mutable_wal_range: Arc<Mutex<Option<WalFrameRange>>>,
    pub(crate) prev_live_floor: Option<u64>,
}

#[derive(Default)]
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) struct SealState {
    pub(crate) immutables: Vec<Arc<ImmutableSegment>>,
    pub(crate) immutable_wal_ranges: Vec<Option<WalFrameRange>>,
    pub(crate) last_seal_at: Option<Instant>,
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

impl<FS, E> DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) fn insert_into_mutable(
        &self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        if let Some(sealed) = self.mem.insert_batch(batch, commit_ts)? {
            let wal_range = self.take_mutable_wal_range();
            self.add_immutable(sealed, wal_range);
            let mut seal = self.seal_state_lock();
            seal.last_seal_at = Some(self.executor.now());
        }
        Ok(())
    }

    fn seal_mutable_now(&self) -> Result<(), KeyExtractError> {
        if let Some(seg) = self.mem.seal_now()? {
            let wal_range = self.take_mutable_wal_range();
            self.add_immutable(seg, wal_range);
            let mut seal = self.seal_state_lock();
            seal.last_seal_at = Some(self.executor.now());
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
        if let Some(sealed) = self.mem.insert_delete_batch(batch)? {
            let wal_range = self.take_mutable_wal_range();
            self.add_immutable(sealed, wal_range);
            let mut seal = self.seal_state_lock();
            seal.last_seal_at = Some(self.executor.now());
        }
        Ok(())
    }

    pub(crate) fn mutable_has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        self.mem.has_conflict(key, snapshot_ts)
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
        let stats = { self.mem.build_stats(since) };

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
}

impl<FS, E> DbInner<FS, E>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Access the configured WAL settings, if any.
    #[cfg(test)]
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

    pub(crate) fn txn_publish_context(
        &self,
        prev_live_floor: Option<u64>,
    ) -> TxnWalPublishContext<FS, E>
    where
        E: Clone,
    {
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

    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) fn add_immutable(&self, seg: ImmutableSegment, wal_range: Option<WalFrameRange>) {
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

async fn insert_dyn_wal_batch<FS, E>(
    db: &DbInner<FS, E>,
    batch: RecordBatch,
    tombstones: Vec<bool>,
) -> Result<(), KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    validate_record_batch_schema(db, &batch)?;
    validate_vec_tombstone_bitmap(&batch, &tombstones)?;

    if batch.num_rows() == 0 {
        return Ok(());
    }

    let commit_ts = db.next_commit_ts();

    let (upsert_batch, delete_batch) =
        partition_batch_for_mutations(db, batch, &tombstones, commit_ts)?;

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
        // Record WAL range BEFORE inserts that may trigger auto-seal.
        // If we record after, a sealed segment would miss the current batch's frames,
        // causing WAL GC to prematurely delete frames needed for recovery.
        if let Some(range) = wal_range {
            db.record_mutable_wal_range(range);
        }
        if let Some(batch) = upsert_batch
            && let Some(sealed) = db.mem.insert_batch(batch, commit_ts)?
        {
            let wal_range_take = db.take_mutable_wal_range();
            db.add_immutable(sealed, wal_range_take);
        }
        if let Some(batch) = delete_batch
            && let Some(sealed) = db.mem.insert_delete_batch(batch)?
        {
            let wal_range_take = db.take_mutable_wal_range();
            db.add_immutable(sealed, wal_range_take);
        }
        db.maybe_seal_after_insert()?;
        db.maybe_run_minor_compaction()
            .await
            .map_err(compaction_as_key_extract_error)?;
    }
    Ok(())
}

fn compaction_as_key_extract_error(err: SsTableError) -> KeyExtractError {
    KeyExtractError::Arrow(ArrowError::ComputeError(format!(
        "minor compaction failed: {err}"
    )))
}

pub(crate) fn apply_dyn_wal_batch<FS, E>(
    db: &DbInner<FS, E>,
    batch: RecordBatch,
    commit_ts_column: ArrayRef,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    let schema_contains_tombstone = db
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

    if let Some(sealed) = db.mem.insert_batch_with_mvcc(batch, commit_array)? {
        let wal_range = db.take_mutable_wal_range();
        db.add_immutable(sealed, wal_range);
    }
    Ok(())
}

fn apply_dyn_delete_wal_batch<FS, E>(
    db: &DbInner<FS, E>,
    batch: RecordBatch,
    commit_ts: Timestamp,
) -> Result<(), KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    validate_delete_batch_schema(db, &batch)?;
    validate_delete_commit_ts(&batch, commit_ts)?;
    if let Some(sealed) = db.mem.insert_delete_batch(batch)? {
        let wal_range = db.take_mutable_wal_range();
        db.add_immutable(sealed, wal_range);
    }
    Ok(())
}

fn validate_record_batch_schema<FS, E>(
    db: &DbInner<FS, E>,
    batch: &RecordBatch,
) -> Result<(), KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    if db.schema.as_ref() != batch.schema().as_ref() {
        return Err(KeyExtractError::SchemaMismatch {
            expected: db.schema.clone(),
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

fn validate_delete_batch_schema<FS, E>(
    db: &DbInner<FS, E>,
    batch: &RecordBatch,
) -> Result<(), KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    if db.delete_schema.as_ref() != batch.schema().as_ref() {
        return Err(KeyExtractError::SchemaMismatch {
            expected: db.delete_schema.clone(),
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

fn partition_batch_for_mutations<FS, E>(
    db: &DbInner<FS, E>,
    batch: RecordBatch,
    tombstones: &[bool],
    commit_ts: Timestamp,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>), KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
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
            db,
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

fn build_delete_batch<FS, E>(
    db: &DbInner<FS, E>,
    batch: &RecordBatch,
    delete_indices: &[u32],
    commit_ts: Timestamp,
    commit_ts_column: Option<&UInt64Array>,
) -> Result<RecordBatch, KeyExtractError>
where
    FS: ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    let idx_array = UInt32Array::from(delete_indices.to_vec());
    let mut columns = Vec::with_capacity(db.extractor().key_indices().len() + 1);
    for &col_idx in db.extractor().key_indices() {
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
    RecordBatch::try_new(db.delete_schema.clone(), columns).map_err(KeyExtractError::Arrow)
}

fn take_full_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ArrowError> {
    let idx_array = UInt32Array::from(indices.to_vec());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), &idx_array, None)?);
    }
    RecordBatch::try_new(batch.schema(), columns)
}
