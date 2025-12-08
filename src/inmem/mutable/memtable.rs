use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
    vec,
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_select::concat::concat_batches;
use crossbeam_skiplist::SkipMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};
use typed_arrow_dyn::{DynProjection, DynRowRaw, DynSchema, DynViewError};

use super::MutableMemTableMetrics;
use crate::{
    extractor::{self, KeyExtractError, KeyProjection, map_view_err},
    inmem::{
        immutable::memtable::{
            DeleteSidecar, ImmutableIndexEntry, ImmutableMemTable, MVCC_COMMIT_COL,
            bundle_mvcc_sidecar,
        },
        policy::{MemStats, StatsProvider},
    },
    key::{KeyOwned, KeyTsOwned, KeyTsViewRaw},
    mutation::DynMutation,
    mvcc::Timestamp,
};

#[derive(Debug)]
struct BatchAttachment {
    storage: RecordBatch,
    commit_ts: UInt64Array,
}

impl BatchAttachment {
    fn new(storage: RecordBatch, commit_ts: UInt64Array) -> Self {
        Self { storage, commit_ts }
    }

    fn storage(&self) -> &RecordBatch {
        &self.storage
    }

    fn commit_ts(&self, row: usize) -> Timestamp {
        Timestamp::new(self.commit_ts.value(row))
    }

    #[cfg(test)]
    fn into_storage(self) -> RecordBatch {
        self.storage
    }
}

#[derive(Debug)]
struct DeleteAttachment {
    keys: RecordBatch,
    commit_ts: UInt64Array,
}

impl DeleteAttachment {
    fn new(keys: RecordBatch, commit_ts: UInt64Array) -> Self {
        Self { keys, commit_ts }
    }

    fn keys(&self) -> &RecordBatch {
        &self.keys
    }

    fn commit_ts(&self, row: usize) -> Timestamp {
        Timestamp::new(self.commit_ts.value(row))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DeleteRowLoc {
    batch_idx: usize,
    row_idx: usize,
}

impl DeleteRowLoc {
    fn new(batch_idx: usize, row_idx: usize) -> Self {
        Self { batch_idx, row_idx }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BatchRowLoc {
    batch_idx: usize,
    row_idx: usize,
}

impl BatchRowLoc {
    fn new(batch_idx: usize, row_idx: usize) -> Self {
        Self { batch_idx, row_idx }
    }
}

/// Default capacity for batch slots when not specified.
pub const DEFAULT_BATCH_CAPACITY: usize = 1024;

/// Mutable state that lives behind the RwLock.
pub(crate) struct DynMemInner {
    /// MVCC index keyed by `(key, commit_ts)` (timestamp descending per key).
    index: SkipMap<KeyTsOwned, DynMutation<BatchRowLoc, DeleteRowLoc>>,
    /// Pre-allocated batch slots using write-once semantics.
    /// Each slot is written exactly once during insert, then read during scans.
    batch_slots: Box<[OnceLock<BatchAttachment>]>,
    /// Atomic cursor for reserving the next available batch slot.
    batch_cursor: AtomicUsize,
    /// Pre-allocated delete slots using write-once semantics (same pattern as batch_slots).
    delete_slots: Box<[OnceLock<DeleteAttachment>]>,
    /// Atomic cursor for reserving the next available delete slot.
    delete_cursor: AtomicUsize,
    metrics: MutableMemTableMetrics,
}

impl DynMemInner {
    fn new(capacity: usize) -> Self {
        let batch_slots: Vec<OnceLock<BatchAttachment>> =
            (0..capacity).map(|_| OnceLock::new()).collect();
        let delete_slots: Vec<OnceLock<DeleteAttachment>> =
            (0..capacity).map(|_| OnceLock::new()).collect();
        Self {
            index: SkipMap::new(),
            batch_slots: batch_slots.into_boxed_slice(),
            batch_cursor: AtomicUsize::new(0),
            delete_slots: delete_slots.into_boxed_slice(),
            delete_cursor: AtomicUsize::new(0),
            metrics: MutableMemTableMetrics::new(32),
        }
    }

    /// Attempt to insert a batch. Returns `Err(())` if capacity exhausted.
    fn try_insert_batch(
        &self,
        extractor: &dyn KeyProjection,
        batch: RecordBatch,
        commit_ts_column: UInt64Array,
    ) -> Result<Result<(), KeyExtractError>, ()> {
        // Reserve a slot atomically - no global lock needed.
        let batch_id = self.batch_cursor.fetch_add(1, Ordering::SeqCst);
        if batch_id >= self.batch_slots.len() {
            // Undo the increment so we don't leak slots
            self.batch_cursor.fetch_sub(1, Ordering::SeqCst);
            return Err(()); // Capacity exhausted
        }

        // Write to our exclusively reserved slot using OnceLock.
        // This is guaranteed to succeed since we own this slot index.
        self.batch_slots[batch_id]
            .set(BatchAttachment::new(
                batch.clone(),
                commit_ts_column.clone(),
            ))
            .expect("batch slot already initialized - invariant violated");

        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let key_rows = match extractor.project_view(&batch, &row_indices) {
            Ok(rows) => rows,
            Err(e) => return Ok(Err(e)),
        };

        for (row_idx, key_row) in key_rows.into_iter().enumerate() {
            let key_size = key_row.heap_size();
            let key_owned = key_row.to_owned();
            let has_existing = self
                .index
                .range(
                    KeyTsOwned::new(key_owned.clone(), Timestamp::MAX)
                        ..=KeyTsOwned::new(key_owned.clone(), Timestamp::MIN),
                )
                .next()
                .is_some();
            self.metrics.record_write(has_existing, key_size);

            let commit_ts = Timestamp::new(commit_ts_column.value(row_idx));
            let composite = KeyTsOwned::new(key_owned.clone(), commit_ts);
            self.index.insert(
                composite,
                DynMutation::Upsert(BatchRowLoc::new(batch_id, row_idx)),
            );
        }
        Ok(Ok(()))
    }

    /// Attempt to insert a delete batch. Returns `Err(())` if capacity exhausted.
    fn try_insert_delete_batch(
        &self,
        delete_projection: &dyn KeyProjection,
        batch: RecordBatch,
    ) -> Result<Result<(), KeyExtractError>, ()> {
        if batch.num_rows() == 0 {
            return Ok(Ok(()));
        }

        let commit_idx = match batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == MVCC_COMMIT_COL)
        {
            Some(idx) => idx,
            None => {
                return Ok(Err(KeyExtractError::NoSuchField {
                    name: MVCC_COMMIT_COL.to_string(),
                }));
            }
        };

        let commit_array = match batch
            .column(commit_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
        {
            Some(arr) => arr.clone(),
            None => {
                return Ok(Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                    format!("{MVCC_COMMIT_COL} column not UInt64"),
                ))));
            }
        };

        if commit_array.len() != batch.num_rows() {
            return Ok(Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                "commit_ts column length mismatch delete batch".to_string(),
            ))));
        }
        if commit_array.null_count() > 0 {
            return Ok(Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                "commit_ts column contained null".to_string(),
            ))));
        }

        // Reserve a delete slot atomically (lock-free).
        let slot_idx = self.delete_cursor.fetch_add(1, Ordering::SeqCst);
        if slot_idx >= self.delete_slots.len() {
            self.delete_cursor.fetch_sub(1, Ordering::SeqCst);
            return Err(()); // Capacity exhausted
        }

        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let key_rows = match delete_projection.project_view(&batch, &row_indices) {
            Ok(rows) => rows,
            Err(e) => return Ok(Err(e)),
        };
        let key_schema = delete_projection.key_schema();
        let key_columns = (0..batch.num_columns())
            .filter(|idx| *idx != commit_idx)
            .map(|idx| batch.column(idx).clone())
            .collect();
        let key_batch = match RecordBatch::try_new(key_schema, key_columns) {
            Ok(b) => b,
            Err(e) => return Ok(Err(KeyExtractError::Arrow(e))),
        };
        let attachment = DeleteAttachment::new(key_batch, commit_array.clone());

        // Write once via OnceLock (no concurrent writes to same slot).
        self.delete_slots[slot_idx]
            .set(attachment)
            .expect("delete slot already initialized");

        for (row_idx, key_row) in key_rows.into_iter().enumerate() {
            let key_size = key_row.heap_size();
            let key_owned = key_row.to_owned();
            let has_existing = self
                .index
                .range(
                    KeyTsOwned::new(key_owned.clone(), Timestamp::MAX)
                        ..=KeyTsOwned::new(key_owned.clone(), Timestamp::MIN),
                )
                .next()
                .is_some();
            self.metrics.record_write(has_existing, key_size);

            let commit_ts = Timestamp::new(commit_array.value(row_idx));
            let composite = KeyTsOwned::new(key_owned.clone(), commit_ts);
            self.index.insert(
                composite,
                DynMutation::Delete(DeleteRowLoc::new(slot_idx, row_idx)),
            );
        }
        Ok(Ok(()))
    }

    /// Seal into immutable, resetting this inner state.
    fn seal(
        &mut self,
        schema: &SchemaRef,
        extractor: &dyn KeyProjection,
    ) -> Result<Option<ImmutableMemTable>, KeyExtractError> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let pop_single_batch = |batches: &mut Vec<RecordBatch>, label: &str| {
            batches.pop().ok_or_else(|| {
                KeyExtractError::Arrow(ArrowError::ComputeError(format!(
                    "{label} unexpectedly empty while sealing mutable memtable"
                )))
            })
        };

        // Take ownership of the index. SkipMap iteration is already sorted by
        // KeyTsOwned's Ord impl: (key ascending, timestamp descending).
        // No additional sorting is needed.
        let index = std::mem::replace(&mut self.index, SkipMap::new());
        let entry_count = index.len();

        // Pre-allocate vectors with known capacity to avoid reallocations.
        let mut slices = Vec::with_capacity(entry_count);
        let mut commit_ts = Vec::with_capacity(entry_count);
        let mut tombstone = Vec::with_capacity(entry_count);
        let mut delete_slices = Vec::new();
        let mut delete_commit_ts = Vec::new();

        // Track entry kinds for second pass (building composite index).
        // We store only what's needed: timestamp and whether it's an upsert or delete.
        enum EntryKind {
            Upsert,
            Delete,
        }
        let mut entry_metadata: Vec<(Timestamp, EntryKind)> = Vec::with_capacity(entry_count);

        // Single pass: extract row slices and collect metadata.
        // SkipMap iterates in sorted order (key asc, ts desc) - no sort needed.
        for (view, mutation) in index.into_iter() {
            let ts = view.timestamp();
            match mutation {
                DynMutation::Upsert(loc) => {
                    let attachment = self.batch_slots[loc.batch_idx]
                        .get()
                        .expect("batch slot must be initialized for indexed entry");
                    let batch = attachment.storage();
                    let row_batch = batch.slice(loc.row_idx, 1);
                    slices.push(row_batch);
                    let attachment_commit = attachment.commit_ts(loc.row_idx);
                    debug_assert_eq!(attachment_commit, ts);
                    commit_ts.push(attachment_commit);
                    tombstone.push(false);
                    entry_metadata.push((ts, EntryKind::Upsert));
                }
                DynMutation::Delete(loc) => {
                    let attachment = self.delete_slots[loc.batch_idx]
                        .get()
                        .expect("delete slot must be initialized for indexed entry");
                    let row_batch = attachment.keys().slice(loc.row_idx, 1);
                    delete_slices.push(row_batch);
                    let attachment_commit = attachment.commit_ts(loc.row_idx);
                    debug_assert_eq!(attachment_commit, ts);
                    delete_commit_ts.push(attachment_commit);
                    entry_metadata.push((ts, EntryKind::Delete));
                }
            }
        }

        // Reset batch slots by replacing with fresh OnceLocks.
        let capacity = self.batch_slots.len();
        let fresh_batch_slots: Vec<OnceLock<BatchAttachment>> =
            (0..capacity).map(|_| OnceLock::new()).collect();
        self.batch_slots = fresh_batch_slots.into_boxed_slice();
        self.batch_cursor.store(0, Ordering::SeqCst);

        // Reset delete slots by replacing with fresh OnceLocks.
        let delete_capacity = self.delete_slots.len();
        let fresh_delete_slots: Vec<OnceLock<DeleteAttachment>> =
            (0..delete_capacity).map(|_| OnceLock::new()).collect();
        self.delete_slots = fresh_delete_slots.into_boxed_slice();
        self.delete_cursor.store(0, Ordering::SeqCst);

        self.metrics.reset_counters();

        let batch = if slices.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else if slices.len() == 1 {
            pop_single_batch(&mut slices, "upsert slices")?
        } else {
            concat_batches(schema, &slices)?
        };
        let (batch, mvcc) = bundle_mvcc_sidecar(batch, commit_ts, tombstone)?;
        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let upsert_key_rows = extractor.project_view(&batch, &row_indices)?;

        let key_schema = extractor.key_schema();
        let delete_batch = if delete_slices.is_empty() {
            RecordBatch::new_empty(key_schema.clone())
        } else if delete_slices.len() == 1 {
            pop_single_batch(&mut delete_slices, "delete slices")?
        } else {
            concat_batches(&key_schema, &delete_slices)?
        };
        let delete_sidecar = DeleteSidecar::new(delete_batch, delete_commit_ts);
        let delete_key_rows = if delete_sidecar.is_empty() {
            Vec::new()
        } else {
            let identity_indices: Vec<usize> = (0..key_schema.fields().len()).collect();
            let identity_projection =
                extractor::projection_for_columns(key_schema.clone(), identity_indices)?;
            let delete_row_indices: Vec<usize> =
                (0..delete_sidecar.key_batch().num_rows()).collect();
            identity_projection.project_view(delete_sidecar.key_batch(), &delete_row_indices)?
        };

        // Build composite index from metadata collected in the first pass.
        let mut composite_index: BTreeMap<KeyTsViewRaw, ImmutableIndexEntry> = BTreeMap::new();
        let mut upsert_row = 0u32;
        let mut delete_row = 0u32;
        for (commit, kind) in entry_metadata.into_iter() {
            match kind {
                EntryKind::Upsert => {
                    let key_row = upsert_key_rows
                        .get(upsert_row as usize)
                        .expect("upsert key row")
                        .clone();
                    let key_view = KeyTsViewRaw::new(key_row, commit);
                    composite_index.insert(key_view, ImmutableIndexEntry::Row(upsert_row));
                    upsert_row += 1;
                }
                EntryKind::Delete => {
                    let key_row = delete_key_rows
                        .get(delete_row as usize)
                        .expect("delete key row")
                        .clone();
                    let key_view = KeyTsViewRaw::new(key_row, commit);
                    composite_index.insert(key_view, ImmutableIndexEntry::Delete);
                    delete_row += 1;
                }
            }
        }

        Ok(Some(ImmutableMemTable::new(
            batch,
            composite_index,
            mvcc,
            delete_sidecar,
        )))
    }

    /// Scan dynamic rows using MVCC visibility semantics at `read_ts`.
    pub(crate) fn scan_visible<'t>(
        &'t self,
        schema: &SchemaRef,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<DynRowScan<'t>, KeyExtractError> {
        let base_schema = self
            .batch_slots
            .iter()
            .find_map(|slot| slot.get())
            .map(|batch| batch.storage().schema())
            .unwrap_or_else(|| schema.clone());
        let dyn_schema = DynSchema::from_ref(base_schema.clone());
        let projection = build_projection(&base_schema, projection_schema.as_ref())?;
        Ok(DynRowScan::new(
            &self.index,
            &self.batch_slots,
            read_ts,
            dyn_schema,
            projection,
        ))
    }

    /// Return `true` if there is any committed version for `key` newer than `snapshot_ts`.
    pub(crate) fn has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        let lower = KeyTsOwned::new(key.clone(), Timestamp::MAX);
        let upper = KeyTsOwned::new(key.clone(), Timestamp::MIN);
        self.index
            .range(lower..=upper)
            .next()
            .map(|entry| entry.key().timestamp() > snapshot_ts)
            .unwrap_or(false)
    }

    /// Build stats snapshot.
    pub(crate) fn build_stats(&self, since_last_seal: Option<Duration>) -> MemStats {
        let metrics = self.metrics.snapshot();
        let batch_count = self.batch_cursor.load(Ordering::Relaxed);
        let delete_count = self.delete_cursor.load(Ordering::Relaxed);
        MemStats {
            entries: metrics.entries,
            inserts: metrics.inserts,
            replaces: metrics.replaces,
            approx_key_bytes: metrics.approx_key_bytes,
            entry_overhead: metrics.entry_overhead,
            typed_open_rows: None,
            dyn_batches: Some(batch_count + delete_count),
            dyn_approx_batch_bytes: None,
            since_last_seal,
        }
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn inspect_versions(&self, key: &KeyOwned) -> Option<Vec<(Timestamp, bool)>> {
        let key_owned = key.clone();
        let mut out = Vec::new();
        for entry in self.index.range(
            KeyTsOwned::new(key_owned.clone(), Timestamp::MAX)
                ..=KeyTsOwned::new(key_owned.clone(), Timestamp::MIN),
        ) {
            let composite = entry.key();
            let mutation = entry.value();
            if composite.key() != &key_owned {
                break;
            }
            let tombstone = matches!(mutation, DynMutation::Delete(_));
            out.push((composite.timestamp(), tombstone));
        }
        if out.is_empty() {
            None
        } else {
            out.reverse();
            Some(out)
        }
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn batch_count(&self) -> usize {
        self.batch_cursor.load(Ordering::Relaxed)
    }
}

/// Columnar-style mutable table for dynamic mode with auto-sealing.
///
/// - Accepts `RecordBatch` inserts; each batch is stored as a sealed chunk.
/// - Maintains per-key version chains ordered by commit timestamp.
/// - Uses lock-free batch slot reservation for high-throughput concurrent inserts.
/// - Automatically seals into immutable when capacity is exhausted.
pub struct DynMem {
    inner: RwLock<DynMemInner>,
    extractor: Arc<dyn KeyProjection>,
    delete_projection: Arc<dyn KeyProjection>,
    schema: SchemaRef,
    capacity: usize,
}

impl DynMem {
    /// Create an empty columnar mutable table for dynamic batches with default capacity.
    pub(crate) fn new(
        schema: SchemaRef,
        extractor: Arc<dyn KeyProjection>,
        delete_projection: Arc<dyn KeyProjection>,
    ) -> Self {
        Self::with_capacity(schema, extractor, delete_projection, DEFAULT_BATCH_CAPACITY)
    }

    /// Create an empty columnar mutable table with the specified batch slot capacity.
    pub(crate) fn with_capacity(
        schema: SchemaRef,
        extractor: Arc<dyn KeyProjection>,
        delete_projection: Arc<dyn KeyProjection>,
        capacity: usize,
    ) -> Self {
        Self {
            inner: RwLock::new(DynMemInner::new(capacity)),
            extractor,
            delete_projection,
            schema,
            capacity,
        }
    }

    /// Acquire a read guard for scanning operations.
    ///
    /// The returned guard keeps the memtable locked for reading, allowing
    /// concurrent reads but blocking writes. Use this for scan operations
    /// that need to hold a consistent view.
    pub(crate) fn read(&self) -> DynMemReadGuard<'_> {
        DynMemReadGuard {
            guard: self.inner.read(),
            schema: &self.schema,
        }
    }

    /// Insert a dynamic batch, auto-sealing if capacity is reached.
    ///
    /// Returns `Some(immutable)` if the memtable was sealed during this insert.
    pub(crate) fn insert_batch(
        &self,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<Option<ImmutableMemTable>, KeyExtractError> {
        self.extractor.validate_schema(&batch.schema())?;
        let rows = batch.num_rows();
        let commit_ts_column = UInt64Array::from(vec![commit_ts.get(); rows]);
        self.insert_batch_with_mvcc(batch, commit_ts_column)
    }

    /// Insert a batch using explicit MVCC metadata columns, auto-sealing if capacity reached.
    ///
    /// Returns `Some(immutable)` if the memtable was sealed during this insert.
    pub(crate) fn insert_batch_with_mvcc(
        &self,
        batch: RecordBatch,
        commit_ts_column: UInt64Array,
    ) -> Result<Option<ImmutableMemTable>, KeyExtractError> {
        let rows = batch.num_rows();
        if commit_ts_column.len() != rows {
            return Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                "commit_ts column length mismatch record batch".to_string(),
            )));
        }
        if commit_ts_column.null_count() > 0 {
            return Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                "commit_ts column contained null".to_string(),
            )));
        }

        // Use upgradable read lock - allows concurrent reads, can upgrade to write for seal
        let guard = self.inner.upgradable_read();

        // Try insert with upgradable read lock (concurrent reads allowed)
        match guard.try_insert_batch(
            self.extractor.as_ref(),
            batch.clone(),
            commit_ts_column.clone(),
        ) {
            Ok(Ok(())) => return Ok(None),
            Ok(Err(e)) => return Err(e),
            Err(()) => {
                // Capacity exhausted - upgrade to write lock and seal
            }
        }

        // Upgrade to write lock atomically (no other thread can interleave)
        let mut write_guard = RwLockUpgradableReadGuard::upgrade(guard);

        // Seal the current state
        let sealed = write_guard.seal(&self.schema, self.extractor.as_ref())?;

        // Now retry the insert on the fresh state
        match write_guard.try_insert_batch(self.extractor.as_ref(), batch, commit_ts_column) {
            Ok(Ok(())) => Ok(sealed),
            Ok(Err(e)) => Err(e),
            Err(()) => {
                // Should not happen - we just sealed and reset
                Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                    "insert failed after seal - batch too large for capacity".to_string(),
                )))
            }
        }
    }

    /// Insert a batch of key-only deletes, auto-sealing if capacity reached.
    ///
    /// Returns `Some(immutable)` if the memtable was sealed during this insert.
    pub(crate) fn insert_delete_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<Option<ImmutableMemTable>, KeyExtractError> {
        self.delete_projection.validate_schema(&batch.schema())?;

        // Use upgradable read lock
        let guard = self.inner.upgradable_read();

        match guard.try_insert_delete_batch(self.delete_projection.as_ref(), batch.clone()) {
            Ok(Ok(())) => return Ok(None),
            Ok(Err(e)) => return Err(e),
            Err(()) => {
                // Capacity exhausted - upgrade and seal
            }
        }

        let mut write_guard = RwLockUpgradableReadGuard::upgrade(guard);
        let sealed = write_guard.seal(&self.schema, self.extractor.as_ref())?;

        match write_guard.try_insert_delete_batch(self.delete_projection.as_ref(), batch) {
            Ok(Ok(())) => Ok(sealed),
            Ok(Err(e)) => Err(e),
            Err(()) => Err(KeyExtractError::Arrow(ArrowError::ComputeError(
                "delete insert failed after seal - batch too large for capacity".to_string(),
            ))),
        }
    }

    /// Force seal the current memtable state into an immutable.
    ///
    /// Returns `None` if the memtable was empty.
    pub(crate) fn seal_now(&self) -> Result<Option<ImmutableMemTable>, KeyExtractError> {
        let mut guard = self.inner.write();
        guard.seal(&self.schema, self.extractor.as_ref())
    }

    /// Return `true` if there is any committed version for `key` newer than `snapshot_ts`.
    pub(crate) fn has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        self.inner.read().has_conflict(key, snapshot_ts)
    }

    /// Access the extractor.
    pub(crate) fn extractor(&self) -> &Arc<dyn KeyProjection> {
        &self.extractor
    }

    pub(crate) fn delete_projection(&self) -> &Arc<dyn KeyProjection> {
        &self.delete_projection
    }

    /// Consume the memtable and return any batches that were still pinned.
    ///
    /// This keeps the borrowed key views sound by dropping the pinned owners at
    /// the same time the batches are released.
    #[cfg(test)]
    pub(crate) fn into_attached_batches(self) -> Vec<RecordBatch> {
        self.inner
            .into_inner()
            .batch_slots
            .into_vec()
            .into_iter()
            .filter_map(|slot| slot.into_inner())
            .map(BatchAttachment::into_storage)
            .collect()
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn inspect_versions(&self, key: &KeyOwned) -> Option<Vec<(Timestamp, bool)>> {
        self.inner.read().inspect_versions(key)
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn batch_count(&self) -> usize {
        self.inner.read().batch_count()
    }
}

/// Guard providing read access to the memtable's inner state.
pub struct DynMemReadGuard<'a> {
    guard: RwLockReadGuard<'a, DynMemInner>,
    schema: &'a SchemaRef,
}

impl<'a> DynMemReadGuard<'a> {
    /// Scan dynamic rows using MVCC visibility semantics at `read_ts`.
    pub fn scan_visible(
        &self,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<DynRowScan<'_>, KeyExtractError> {
        self.guard
            .scan_visible(self.schema, projection_schema, read_ts)
    }
}

/// A lightweight reference wrapper for testing that delegates to DynMem's test methods.
#[cfg(all(test, feature = "tokio"))]
pub struct TestMemRef<'a>(pub(crate) &'a DynMem);

#[cfg(all(test, feature = "tokio"))]
impl<'a> TestMemRef<'a> {
    /// Inspect all versions of a key in the memtable.
    pub fn inspect_versions(&self, key: &KeyOwned) -> Option<Vec<(Timestamp, bool)>> {
        self.0.inspect_versions(key)
    }

    /// Return the number of batches in the memtable.
    pub fn batch_count(&self) -> usize {
        self.0.batch_count()
    }
}

impl fmt::Debug for DynMem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynMem")
            .field("capacity", &self.capacity)
            .finish()
    }
}

fn build_projection(
    schema: &SchemaRef,
    projection_schema: Option<&SchemaRef>,
) -> Result<DynProjection, KeyExtractError> {
    if let Some(projected) = projection_schema {
        if projected.fields().is_empty() {
            return Err(KeyExtractError::Arrow(
                arrow_schema::ArrowError::ComputeError(
                    "projection requires at least one column".to_string(),
                ),
            ));
        }
        DynProjection::from_schema(schema.as_ref(), projected.as_ref()).map_err(map_view_err)
    } else {
        DynProjection::from_schema(schema.as_ref(), schema.as_ref()).map_err(map_view_err)
    }
}

// ---- StatsProvider implementations ----

impl StatsProvider for DynMem {
    fn build_stats(&self, since_last_seal: Option<Duration>) -> MemStats {
        self.inner.read().build_stats(since_last_seal)
    }
}

/// Iterator over dynamic rows, materializing from `RecordBatch`es and filtering by MVCC visibility.
pub(crate) struct DynRowScan<'t> {
    batch_slots: &'t [OnceLock<BatchAttachment>],
    cursor: crossbeam_skiplist::map::Iter<'t, KeyTsOwned, DynMutation<BatchRowLoc, DeleteRowLoc>>,
    read_ts: Timestamp,
    current_key: Option<KeyOwned>,
    emitted_for_key: bool,
    dyn_schema: DynSchema,
    projection: DynProjection,
}

pub(crate) enum DynRowScanEntry {
    Row(KeyTsViewRaw, DynRowRaw),
    Tombstone(KeyTsViewRaw),
}

impl DynRowScanEntry {
    #[cfg(test)]
    pub(crate) fn into_row(self) -> Option<(KeyTsViewRaw, DynRowRaw)> {
        match self {
            DynRowScanEntry::Row(key, row) => Some((key, row)),
            DynRowScanEntry::Tombstone(_) => None,
        }
    }
}

impl<'t> fmt::Debug for DynRowScan<'t> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynRowScan")
            .field("read_ts", &self.read_ts)
            .field("emitted_for_key", &self.emitted_for_key)
            .finish()
    }
}

impl<'t> DynRowScan<'t> {
    fn new(
        index: &'t SkipMap<KeyTsOwned, DynMutation<BatchRowLoc, DeleteRowLoc>>,
        batch_slots: &'t [OnceLock<BatchAttachment>],
        read_ts: Timestamp,
        dyn_schema: DynSchema,
        projection: DynProjection,
    ) -> Self {
        Self {
            batch_slots,
            cursor: index.iter(),
            read_ts,
            current_key: None,
            emitted_for_key: false,
            dyn_schema,
            projection,
        }
    }
}

impl<'t> Iterator for DynRowScan<'t> {
    type Item = Result<DynRowScanEntry, DynViewError>;
    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.cursor.by_ref() {
            let composite = entry.key();
            let mutation = entry.value();
            let key_owned = composite.key().clone();
            if self
                .current_key
                .as_ref()
                .map(|k| k == &key_owned)
                .unwrap_or(false)
            {
                if self.emitted_for_key {
                    continue;
                }
            } else {
                self.current_key = Some(key_owned.clone());
                self.emitted_for_key = false;
            }

            if composite.timestamp() > self.read_ts {
                continue;
            }

            if let DynMutation::Delete(_) = mutation {
                self.emitted_for_key = true;
                return Some(Ok(DynRowScanEntry::Tombstone(composite.as_raw_view())));
            }
            let loc = match mutation {
                DynMutation::Upsert(loc) => *loc,
                DynMutation::Delete(_) => unreachable!(),
            };
            let attachment = self.batch_slots[loc.batch_idx]
                .get()
                .expect("batch slot must be initialized for indexed entry");
            let batch = attachment.storage();
            let row = match self
                .projection
                .project_row_raw(&self.dyn_schema, batch, loc.row_idx)
            {
                Ok(row) => row,
                Err(err) => return Some(Err(err)),
            };

            self.emitted_for_key = true;
            return Some(Ok(DynRowScanEntry::Row(composite.as_raw_view(), row)));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{inmem::policy::StatsProvider, test::build_batch};

    fn make_test_mem(schema: SchemaRef) -> DynMem {
        let extractor: Arc<dyn KeyProjection> =
            crate::extractor::projection_for_field(schema.clone(), 0)
                .expect("extractor")
                .into();
        let delete_projection: Arc<dyn KeyProjection> = crate::extractor::projection_for_columns(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![0],
        )
        .expect("delete projection")
        .into();
        DynMem::new(schema, extractor, delete_projection)
    }

    #[test]
    fn dyn_stats_and_scan() {
        // Build a batch: id Utf8 is key
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let m = make_test_mem(schema.clone());
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(3))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("ok");
        m.insert_batch(batch, Timestamp::MIN).expect("insert");

        let s = m.build_stats(None);
        assert_eq!(s.inserts, 3);
        assert_eq!(s.replaces, 1);
        assert_eq!(s.entries, 2);
        assert_eq!(s.dyn_batches, Some(1));
        // approx_key_bytes for "a" and "b" is 1 + 1
        assert_eq!(s.approx_key_bytes, 2);

        // Scan rows and retain keys >= "b" (latest per key)
        let got: Vec<String> = m
            .read()
            .scan_visible(None, Timestamp::MAX)
            .expect("scan rows")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().map(|(key, row)| (key, row))
            })
            .filter_map(|(key, row)| {
                let key_str = key
                    .key()
                    .to_owned()
                    .as_utf8()
                    .map(str::to_string)
                    .expect("utf8 key");
                (key_str.as_str() >= "b").then_some((key_str, row))
            })
            .map(
                |(_k, row)| match row.into_owned().expect("row").0[0].as_ref() {
                    Some(typed_arrow_dyn::DynCell::Str(s)) => s.clone(),
                    _ => unreachable!(),
                },
            )
            .collect();
        assert_eq!(got, vec!["b".to_string()]);

        // Drain attached batches
        let drained = m.into_attached_batches();
        assert_eq!(drained.len(), 1);
    }

    #[test]
    fn conflict_detection_checks_latest_commit_ts() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let mem = make_test_mem(schema.clone());

        let rows_v1 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch_v1 = build_batch(schema.clone(), rows_v1).expect("batch v1");
        mem.insert_batch(batch_v1, Timestamp::new(10))
            .expect("insert v1");

        let rows_v2 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(2)),
        ])];
        let batch_v2 = build_batch(schema.clone(), rows_v2).expect("batch v2");
        mem.insert_batch(batch_v2, Timestamp::new(20))
            .expect("insert v2");

        let key = KeyOwned::from("k");
        // Version at ts=20 is newer than snapshot at ts=15
        assert!(mem.has_conflict(&key, Timestamp::new(15)));
        // No version newer than ts=20 (equal doesn't count)
        assert!(!mem.has_conflict(&key, Timestamp::new(20)));
        // No version newer than ts=25
        assert!(!mem.has_conflict(&key, Timestamp::new(25)));
        // Different key has no conflict
        assert!(!mem.has_conflict(&KeyOwned::from("other"), Timestamp::new(5)));
    }

    #[test]
    fn auto_seal_on_capacity_exhausted() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor: Arc<dyn crate::extractor::KeyProjection> =
            crate::extractor::projection_for_field(schema.clone(), 0)
                .expect("extractor")
                .into();
        let delete_projection: Arc<dyn crate::extractor::KeyProjection> =
            crate::extractor::projection_for_columns(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
                vec![0],
            )
            .expect("delete projection")
            .into();
        // Create with capacity of 2 batch slots
        let mem = DynMem::with_capacity(schema.clone(), extractor, delete_projection, 2);

        // First two inserts should succeed without sealing
        let batch1 = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch1");
        let result1 = mem
            .insert_batch(batch1, Timestamp::new(1))
            .expect("insert1");
        assert!(result1.is_none(), "first insert should not seal");

        let batch2 = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");
        let result2 = mem
            .insert_batch(batch2, Timestamp::new(2))
            .expect("insert2");
        assert!(result2.is_none(), "second insert should not seal");

        // Third insert should trigger auto-seal
        let batch3 = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("c".into())),
                Some(DynCell::I32(3)),
            ])],
        )
        .expect("batch3");
        let result3 = mem
            .insert_batch(batch3, Timestamp::new(3))
            .expect("insert3");
        assert!(result3.is_some(), "third insert should trigger seal");

        let sealed = result3.unwrap();
        assert_eq!(
            sealed.storage().num_rows(),
            2,
            "sealed should have 2 rows from first two inserts"
        );
    }

    #[test]
    fn scan_respects_mvcc_visibility() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let m = make_test_mem(schema.clone());

        // Insert same key at different timestamps
        let batch1 = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch1");
        m.insert_batch(batch1, Timestamp::new(10)).expect("insert1");

        let batch2 = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");
        m.insert_batch(batch2, Timestamp::new(20)).expect("insert2");

        // Scan at ts=15 should see version at ts=10
        let guard = m.read();
        let results: Vec<_> = guard
            .scan_visible(None, Timestamp::new(15))
            .expect("scan")
            .filter_map(|r| r.ok())
            .filter_map(|e| e.into_row())
            .collect();
        assert_eq!(results.len(), 1);

        // Scan at ts=25 should see version at ts=20 (latest visible)
        let results2: Vec<_> = guard
            .scan_visible(None, Timestamp::new(25))
            .expect("scan")
            .filter_map(|r| r.ok())
            .filter_map(|e| e.into_row())
            .collect();
        assert_eq!(results2.len(), 1);
    }
}
