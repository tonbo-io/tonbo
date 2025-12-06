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
use arrow_schema::{ArrowError, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use crossbeam_skiplist::SkipMap;
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
    key::{KeyOwned, KeyRow, KeyTsOwned, KeyTsViewRaw},
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

/// Columnar-style mutable table for dynamic mode.
///
/// - Accepts `RecordBatch` inserts; each batch is stored as a sealed chunk.
/// - Maintains per-key version chains ordered by commit timestamp.
/// - Uses lock-free batch slot reservation for high-throughput inserts.
pub struct DynMem {
    /// MVCC index keyed by `(key, commit_ts)` (timestamp descending per key).
    index: SkipMap<KeyTsOwned, DynMutation<BatchRowLoc, DeleteRowLoc>>,
    /// Pre-allocated batch slots using write-once semantics.
    /// Each slot is written exactly once during insert, then read during scans.
    batch_slots: Box<[OnceLock<BatchAttachment>]>,
    /// Atomic cursor for reserving the next available batch slot.
    batch_cursor: AtomicUsize,
    /// Key-only delete batches tracked separately from value payloads.
    delete_batches: Vec<DeleteAttachment>,
    metrics: MutableMemTableMetrics,
    schema: SchemaRef,
}

impl DynMem {
    /// Create an empty columnar mutable table for dynamic batches with default capacity.
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self::with_capacity(schema, DEFAULT_BATCH_CAPACITY)
    }

    /// Create an empty columnar mutable table with the specified batch slot capacity.
    ///
    /// When the capacity is exhausted, [`insert_batch`] returns
    /// [`KeyExtractError::MemtableFull`], signaling that the memtable should be sealed.
    pub(crate) fn with_capacity(schema: SchemaRef, capacity: usize) -> Self {
        let batch_slots: Vec<OnceLock<BatchAttachment>> =
            (0..capacity).map(|_| OnceLock::new()).collect();
        Self {
            index: SkipMap::new(),
            batch_slots: batch_slots.into_boxed_slice(),
            batch_cursor: AtomicUsize::new(0),
            delete_batches: Vec::new(),
            metrics: MutableMemTableMetrics::new(32),
            schema,
        }
    }

    #[cfg(all(test, feature = "tokio"))]
    /// Returns the number of batches currently stored.
    pub(crate) fn batch_count(&self) -> usize {
        self.batch_cursor.load(Ordering::Relaxed)
    }

    /// Insert a dynamic batch by indexing each row's key.
    pub(crate) fn insert_batch(
        &self,
        extractor: &dyn KeyProjection,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), crate::extractor::KeyExtractError> {
        let rows = batch.num_rows();
        let commit_ts_column = UInt64Array::from(vec![commit_ts.get(); rows]);
        self.insert_batch_with_mvcc(extractor, batch, commit_ts_column)
    }

    /// Insert a batch using explicit MVCC metadata columns.
    ///
    /// Returns [`KeyExtractError::MemtableFull`] when no batch slots remain,
    /// signaling the caller to seal and retry.
    pub(crate) fn insert_batch_with_mvcc(
        &self,
        extractor: &dyn KeyProjection,
        batch: RecordBatch,
        commit_ts_column: UInt64Array,
    ) -> Result<(), crate::extractor::KeyExtractError> {
        extractor.validate_schema(&batch.schema())?;
        let rows = batch.num_rows();
        if commit_ts_column.len() != rows {
            return Err(crate::extractor::KeyExtractError::Arrow(
                arrow_schema::ArrowError::ComputeError(
                    "commit_ts column length mismatch record batch".to_string(),
                ),
            ));
        }
        if commit_ts_column.null_count() > 0 {
            return Err(crate::extractor::KeyExtractError::Arrow(
                arrow_schema::ArrowError::ComputeError(
                    "commit_ts column contained null".to_string(),
                ),
            ));
        }

        // Reserve a slot atomically - no global lock needed.
        let batch_id = self.batch_cursor.fetch_add(1, Ordering::SeqCst);
        if batch_id >= self.batch_slots.len() {
            return Err(crate::extractor::KeyExtractError::MemtableFull {
                capacity: self.batch_slots.len(),
            });
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
        let key_rows = extractor.project_view(&batch, &row_indices)?;
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
        Ok(())
    }

    /// Insert a batch of key-only deletes encoded with the delete schema.
    pub(crate) fn insert_delete_batch(
        &mut self,
        delete_projection: &dyn KeyProjection,
        batch: RecordBatch,
    ) -> Result<(), crate::extractor::KeyExtractError> {
        delete_projection.validate_schema(&batch.schema())?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let commit_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == MVCC_COMMIT_COL)
            .ok_or_else(|| crate::extractor::KeyExtractError::NoSuchField {
                name: MVCC_COMMIT_COL.to_string(),
            })?;
        let commit_array = batch
            .column(commit_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                crate::extractor::KeyExtractError::Arrow(arrow_schema::ArrowError::ComputeError(
                    format!("{MVCC_COMMIT_COL} column not UInt64"),
                ))
            })?
            .clone();
        if commit_array.len() != batch.num_rows() {
            return Err(crate::extractor::KeyExtractError::Arrow(
                arrow_schema::ArrowError::ComputeError(
                    "commit_ts column length mismatch delete batch".to_string(),
                ),
            ));
        }
        if commit_array.null_count() > 0 {
            return Err(crate::extractor::KeyExtractError::Arrow(
                arrow_schema::ArrowError::ComputeError(
                    "commit_ts column contained null".to_string(),
                ),
            ));
        }

        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let key_rows = delete_projection.project_view(&batch, &row_indices)?;
        let key_schema = delete_projection.key_schema();
        let key_columns = (0..batch.num_columns())
            .filter(|idx| *idx != commit_idx)
            .map(|idx| batch.column(idx).clone())
            .collect();
        let key_batch = RecordBatch::try_new(key_schema, key_columns)
            .map_err(crate::extractor::KeyExtractError::Arrow)?;
        let attachment = DeleteAttachment::new(key_batch, commit_array.clone());
        let batch_idx = self.delete_batches.len();
        self.delete_batches.push(attachment);

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
                DynMutation::Delete(DeleteRowLoc::new(batch_idx, row_idx)),
            );
        }
        Ok(())
    }

    #[cfg(test)]
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

    /// Scan dynamic rows using MVCC visibility semantics at `read_ts`.
    pub(crate) fn scan_visible<'t>(
        &'t self,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<DynRowScan<'t>, KeyExtractError> {
        let base_schema = self
            .batch_slots
            .iter()
            .find_map(|slot| slot.get())
            .map(|batch| batch.storage().schema())
            .unwrap_or_else(|| self.schema.clone());
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

    /// Approximate memory usage for keys stored in the mutable table.
    pub(crate) fn approx_bytes(&self) -> usize {
        self.metrics.approx_bytes()
    }

    /// Consume the memtable and return any batches that were still pinned.
    ///
    /// This keeps the borrowed key views sound by dropping the pinned owners at
    /// the same time the batches are released.
    #[cfg(test)]
    pub(crate) fn into_attached_batches(self) -> Vec<RecordBatch> {
        self.batch_slots
            .into_vec()
            .into_iter()
            .filter_map(|slot| slot.into_inner())
            .map(BatchAttachment::into_storage)
            .collect()
    }

    pub(crate) fn seal_into_immutable(
        &mut self,
        schema: &SchemaRef,
        extractor: &dyn KeyProjection,
    ) -> Result<Option<ImmutableMemTable<RecordBatch>>, crate::extractor::KeyExtractError> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let pop_single_batch = |batches: &mut Vec<RecordBatch>, label: &str| {
            batches.pop().ok_or_else(|| {
                crate::extractor::KeyExtractError::Arrow(ArrowError::ComputeError(format!(
                    "{label} unexpectedly empty while sealing mutable memtable"
                )))
            })
        };

        let mut slices = Vec::new();
        let mut commit_ts = Vec::new();
        let mut tombstone = Vec::new();
        let mut delete_slices = Vec::new();
        let mut delete_commit_ts = Vec::new();

        enum EntryKind {
            Upsert(BatchRowLoc),
            Delete(DeleteRowLoc),
        }

        let index = std::mem::replace(&mut self.index, SkipMap::new());
        let mut entries: Vec<(KeyRow, Timestamp, EntryKind)> = index
            .into_iter()
            .map(|(view, mutation)| {
                let key_row = KeyRow::from_owned(view.key())
                    .expect("stored keys should contain supported components");
                let ts = view.timestamp();
                let kind = match mutation {
                    DynMutation::Upsert(loc) => EntryKind::Upsert(loc),
                    DynMutation::Delete(loc) => EntryKind::Delete(loc),
                };
                (key_row, ts, kind)
            })
            .collect();
        entries.sort_by(
            |(key_a, ts_a, ..), (key_b, ts_b, ..)| match key_a.cmp(key_b) {
                std::cmp::Ordering::Equal => ts_b.cmp(ts_a),
                other => other,
            },
        );

        for (_key, commit, kind) in entries.iter() {
            match kind {
                EntryKind::Upsert(loc) => {
                    let attachment = self.batch_slots[loc.batch_idx]
                        .get()
                        .expect("batch slot must be initialized for indexed entry");
                    let batch = attachment.storage();
                    let row_batch = batch.slice(loc.row_idx, 1);
                    slices.push(row_batch);
                    let attachment_commit = attachment.commit_ts(loc.row_idx);
                    debug_assert_eq!(attachment_commit, *commit);
                    commit_ts.push(attachment_commit);
                    tombstone.push(false);
                }
                EntryKind::Delete(loc) => {
                    let attachment = &self.delete_batches[loc.batch_idx];
                    let row_batch = attachment.keys().slice(loc.row_idx, 1);
                    delete_slices.push(row_batch);
                    let attachment_commit = attachment.commit_ts(loc.row_idx);
                    debug_assert_eq!(attachment_commit, *commit);
                    delete_commit_ts.push(attachment_commit);
                }
            }
        }

        // Reset batch slots by replacing with fresh OnceLocks.
        let capacity = self.batch_slots.len();
        let fresh_slots: Vec<OnceLock<BatchAttachment>> =
            (0..capacity).map(|_| OnceLock::new()).collect();
        self.batch_slots = fresh_slots.into_boxed_slice();
        self.batch_cursor.store(0, Ordering::SeqCst);
        self.delete_batches.clear();
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

        let mut composite_index: BTreeMap<KeyTsViewRaw, ImmutableIndexEntry> = BTreeMap::new();
        let mut upsert_row = 0u32;
        let mut delete_row = 0u32;
        for (_key_row, commit, kind) in entries.into_iter() {
            match kind {
                EntryKind::Upsert(_) => {
                    let key_row = upsert_key_rows
                        .get(upsert_row as usize)
                        .expect("upsert key row")
                        .clone();
                    let key_view = KeyTsViewRaw::new(key_row, commit);
                    composite_index.insert(key_view, ImmutableIndexEntry::Row(upsert_row));
                    upsert_row += 1;
                }
                EntryKind::Delete(_) => {
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

impl Default for DynMem {
    fn default() -> Self {
        Self::new(Arc::new(Schema::new(Vec::<arrow_schema::Field>::new())))
    }
}

// ---- StatsProvider implementations ----

impl StatsProvider for DynMem {
    fn build_stats(&self, since_last_seal: Option<Duration>) -> MemStats {
        let metrics = self.metrics.snapshot();
        let batch_count = self.batch_cursor.load(Ordering::Relaxed);
        MemStats {
            entries: metrics.entries,
            inserts: metrics.inserts,
            replaces: metrics.replaces,
            approx_key_bytes: metrics.approx_key_bytes,
            entry_overhead: metrics.entry_overhead,
            typed_open_rows: None,
            dyn_batches: Some(batch_count + self.delete_batches.len()),
            dyn_approx_batch_bytes: None,
            since_last_seal,
        }
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
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::projection_for_columns, inmem::policy::StatsProvider, test::build_batch,
    };

    #[test]
    fn dyn_stats_and_scan() {
        // Build a batch: id Utf8 is key
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let m = DynMem::new(schema.clone());
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(3))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("ok");
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        m.insert_batch(extractor.as_ref(), batch, Timestamp::MIN)
            .expect("insert");

        let s = m.build_stats(None);
        assert_eq!(s.inserts, 3);
        assert_eq!(s.replaces, 1);
        assert_eq!(s.entries, 2);
        assert_eq!(s.dyn_batches, Some(1));
        // approx_key_bytes for "a" and "b" is 1 + 1
        assert_eq!(s.approx_key_bytes, 2);

        // Scan rows and retain keys >= "b" (latest per key)
        let got: Vec<String> = m
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
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let mem = DynMem::new(schema.clone());

        let rows_v1 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch_v1 = build_batch(schema.clone(), rows_v1).expect("batch v1");
        mem.insert_batch(extractor.as_ref(), batch_v1, Timestamp::new(10))
            .expect("insert v1");

        let rows_v2 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(2)),
        ])];
        let batch_v2 = build_batch(schema.clone(), rows_v2).expect("batch v2");
        mem.insert_batch(extractor.as_ref(), batch_v2, Timestamp::new(20))
            .expect("insert v2");

        let key = KeyOwned::from("k");

        assert!(mem.has_conflict(&key, Timestamp::new(15)));
        assert!(!mem.has_conflict(&key, Timestamp::new(20)));
        assert!(!mem.has_conflict(&key, Timestamp::new(25)));
        assert!(!mem.has_conflict(&KeyOwned::from("other"), Timestamp::new(5)));
    }

    #[test]
    fn mvcc_scan_respects_read_ts() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let m = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        // First commit at ts=10
        let rows_v1 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let batch_v1: RecordBatch = build_batch(schema.clone(), rows_v1).expect("batch v1");
        m.insert_batch(extractor.as_ref(), batch_v1, Timestamp::new(10))
            .expect("insert v1");

        // Second commit overwrites key at ts=20
        let rows_v2 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(2)),
        ])];
        let batch_v2: RecordBatch = build_batch(schema.clone(), rows_v2).expect("batch v2");
        m.insert_batch(extractor.as_ref(), batch_v2, Timestamp::new(20))
            .expect("insert v2");

        // Third commit overwrites key at ts=30
        let rows_v3 = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(3)),
        ])];
        let batch_v3: RecordBatch = build_batch(schema.clone(), rows_v3).expect("batch v3");
        m.insert_batch(extractor.as_ref(), batch_v3, Timestamp::new(30))
            .expect("insert v3");

        // Before the first commit nothing should be visible
        let rows_before: Vec<DynRow> = m
            .scan_visible(None, Timestamp::new(5))
            .expect("scan rows at")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert!(rows_before.is_empty());

        // Between first and second commits the first value is visible
        let rows_after_first: Vec<i32> = m
            .scan_visible(None, Timestamp::new(15))
            .expect("scan rows at")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().map(|(_, row)| {
                    let row = row.into_owned().expect("row");
                    match row.0[1].as_ref() {
                        Some(DynCell::I32(v)) => *v,
                        _ => unreachable!(),
                    }
                })
            })
            .collect();
        assert_eq!(rows_after_first, vec![1]);

        // Between second and third commits the second value is visible
        let rows_after_second: Vec<i32> = m
            .scan_visible(None, Timestamp::new(25))
            .expect("scan rows at")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().map(|(_, row)| {
                    let row = row.into_owned().expect("row");
                    match row.0[1].as_ref() {
                        Some(DynCell::I32(v)) => *v,
                        _ => unreachable!(),
                    }
                })
            })
            .collect();
        assert_eq!(rows_after_second, vec![2]);

        // Between third and fourth commits the third value is visible
        let row_latest: Vec<i32> = m
            .scan_visible(None, Timestamp::new(35))
            .expect("scan rows at")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().map(|(_, row)| {
                    let row = row.into_owned().expect("row");
                    match row.0[1].as_ref() {
                        Some(DynCell::I32(v)) => *v,
                        _ => unreachable!(),
                    }
                })
            })
            .collect();
        assert_eq!(row_latest, vec![3]);
    }

    #[test]
    fn scan_rows_at_hides_versions_after_delete() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let mut m = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let initial = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let initial_batch: RecordBatch = build_batch(schema.clone(), initial).expect("batch");
        m.insert_batch(extractor.as_ref(), initial_batch, Timestamp::new(10))
            .expect("insert");

        let delete_schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let delete_projection =
            projection_for_columns(delete_schema.clone(), vec![0]).expect("delete projection");
        let delete_rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::U64(20)),
        ])];
        let delete_batch: RecordBatch = build_batch(delete_schema, delete_rows).expect("delete");
        m.insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("delete row");

        let visible_before_delete: Vec<_> = m
            .scan_visible(None, Timestamp::new(15))
            .expect("scan before delete")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(visible_before_delete.len(), 1);

        let after_delete: Vec<_> = m
            .scan_visible(None, Timestamp::new(25))
            .expect("scan after delete")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert!(
            after_delete.is_empty(),
            "rows at or below delete_ts should be hidden"
        );
    }

    #[test]
    fn scan_rows_at_sees_newer_upsert_after_delete() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let mut m = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let initial = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ])];
        let initial_batch: RecordBatch = build_batch(schema.clone(), initial).expect("batch");
        m.insert_batch(extractor.as_ref(), initial_batch, Timestamp::new(10))
            .expect("insert");

        let delete_schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let delete_projection =
            projection_for_columns(delete_schema.clone(), vec![0]).expect("delete projection");
        let delete_rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::U64(20)),
        ])];
        let delete_batch: RecordBatch = build_batch(delete_schema, delete_rows).expect("delete");
        m.insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("delete row");

        let reinserts = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(99)),
        ])];
        let reinserts_batch: RecordBatch = build_batch(schema.clone(), reinserts).expect("batch");
        m.insert_batch(extractor.as_ref(), reinserts_batch, Timestamp::new(30))
            .expect("reinsert");

        let after_delete: Vec<_> = m
            .scan_visible(None, Timestamp::new(25))
            .expect("scan after delete")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert!(after_delete.is_empty());

        let after_reinsert: Vec<_> = m
            .scan_visible(None, Timestamp::new(35))
            .expect("scan after reinsert")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(after_reinsert.len(), 1);
        match after_reinsert[0].0[1].as_ref() {
            Some(DynCell::I32(v)) => assert_eq!(*v, 99),
            other => panic!("unexpected cell {other:?}"),
        }
    }

    #[test]
    fn seal_into_immutable_emits_mvcc_segments() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let mut layout = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        // four versions for the same key
        let batch1: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch1");
        layout
            .insert_batch(extractor.as_ref(), batch1, Timestamp::new(10))
            .expect("insert");

        let batch2: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");
        layout
            .insert_batch(extractor.as_ref(), batch2, Timestamp::new(20))
            .expect("insert");

        let batch3: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(3)),
            ])],
        )
        .expect("batch3");
        layout
            .insert_batch(extractor.as_ref(), batch3, Timestamp::new(30))
            .expect("insert");

        let batch4: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(4)),
            ])],
        )
        .expect("batch4");
        layout
            .insert_batch(extractor.as_ref(), batch4, Timestamp::new(40))
            .expect("insert");

        let segment = layout
            .seal_into_immutable(&schema, extractor.as_ref())
            .expect("seal ok")
            .expect("segment");
        assert_eq!(segment.len(), 4);

        let row_after_first = segment
            .scan_visible(None, Timestamp::new(15))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().and_then(|(key, row)| {
                    if key.key().to_owned().as_utf8() == Some("k") {
                        Some(row.into_owned().expect("row"))
                    } else {
                        None
                    }
                })
            })
            .next()
            .expect("row after first commit");
        let row_after_second = segment
            .scan_visible(None, Timestamp::new(25))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().and_then(|(key, row)| {
                    if key.key().to_owned().as_utf8() == Some("k") {
                        Some(row.into_owned().expect("row"))
                    } else {
                        None
                    }
                })
            })
            .next()
            .expect("row after second commit");
        let row_after_third = segment
            .scan_visible(None, Timestamp::new(35))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().and_then(|(key, row)| {
                    if key.key().to_owned().as_utf8() == Some("k") {
                        Some(row.into_owned().expect("row"))
                    } else {
                        None
                    }
                })
            })
            .next()
            .expect("row after third commit");
        let row_latest = segment
            .scan_visible(None, Timestamp::new(45))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .next()
            .expect("row after latest commit");

        let cell_value = |row: &DynRow| match &row.0[1] {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };

        assert_eq!(cell_value(&row_after_first), 1);
        assert_eq!(cell_value(&row_after_second), 2);
        assert_eq!(cell_value(&row_after_third), 3);
        assert_eq!(cell_value(&row_latest), 4);
    }

    #[test]
    fn scan_rows_respects_projection_indices() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let m = DynMem::new(schema.clone());
        let rows = vec![
            DynRow(vec![
                Some(DynCell::I64(1)),
                Some(DynCell::Str("alice".into())),
                Some(DynCell::I32(10)),
            ]),
            DynRow(vec![
                Some(DynCell::I64(2)),
                Some(DynCell::Str("bob".into())),
                Some(DynCell::I32(20)),
            ]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        m.insert_batch(extractor.as_ref(), batch, Timestamp::new(10))
            .expect("insert");

        let projection_schema = Arc::new(Schema::new(vec![
            schema.field(0).clone(),
            schema.field(2).clone(),
        ]));
        let rows: Vec<DynRow> = m
            .scan_visible(Some(Arc::clone(&projection_schema)), Timestamp::MAX)
            .expect("scan rows")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(rows.len(), 2);

        let first = &rows[0];
        assert_eq!(first.0.len(), 2);
        match first.0[0].as_ref() {
            Some(DynCell::I64(value)) => assert_eq!(*value, 1),
            other => panic!("unexpected id cell {other:?}"),
        }
        match first.0[1].as_ref() {
            Some(DynCell::I32(value)) => assert_eq!(*value, 10),
            other => panic!("unexpected score cell {other:?}"),
        }
    }

    #[test]
    fn sealed_segment_row_iter_matches_versions() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let mut layout = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let insert = |layout: &mut DynMem, val: i32, ts: u64| {
            let batch: RecordBatch = build_batch(
                schema.clone(),
                vec![DynRow(vec![
                    Some(DynCell::Str("k".into())),
                    Some(DynCell::I32(val)),
                ])],
            )
            .expect("batch");
            layout
                .insert_batch_with_mvcc(extractor.as_ref(), batch, UInt64Array::from(vec![ts]))
                .expect("insert");
        };

        insert(&mut layout, 1, 10);
        insert(&mut layout, 3, 30);

        let delete_schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let delete_projection =
            projection_for_columns(delete_schema.clone(), vec![0]).expect("delete projection");
        let delete_batch = build_batch(
            delete_schema,
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::U64(20)),
            ])],
        )
        .expect("delete batch");
        layout
            .insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("delete");

        let segment = layout
            .seal_into_immutable(&schema, extractor.as_ref())
            .expect("sealed")
            .expect("segment");

        let rows: Vec<(u64, bool)> = segment
            .row_iter()
            .map(|entry| (entry.commit_ts.get(), entry.tombstone))
            .collect();
        assert_eq!(
            rows,
            vec![(30, false), (20, true), (10, false)],
            "row iterator should preserve newest→oldest MVCC ordering"
        );
        assert_eq!(segment.len(), 3);
    }

    #[test]
    fn insert_batch_with_mvcc_preserves_metadata() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let layout = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let batch1: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch1");
        layout
            .insert_batch_with_mvcc(extractor.as_ref(), batch1, UInt64Array::from(vec![10]))
            .expect("insert batch1");

        let batch2: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");
        layout
            .insert_batch_with_mvcc(extractor.as_ref(), batch2, UInt64Array::from(vec![20]))
            .expect("insert batch2");

        let chain = layout
            .inspect_versions(&KeyOwned::from("k"))
            .expect("version chain");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0], (Timestamp::new(10), false));
        assert_eq!(chain[1], (Timestamp::new(20), false));
    }

    #[test]
    fn insert_delete_batch_tracks_key_only_tombstones() {
        let table_schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let mut layout = DynMem::new(table_schema.clone());
        let extractor =
            crate::extractor::projection_for_field(table_schema.clone(), 0).expect("extractor");
        let key_schema = extractor.key_schema();

        let mut delete_fields = key_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<Field>>();
        delete_fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false));
        let delete_schema = std::sync::Arc::new(Schema::new(delete_fields));
        let delete_projection = projection_for_columns(
            delete_schema.clone(),
            (0..delete_schema.fields().len() - 1).collect(),
        )
        .expect("delete projection");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::U64(10))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::U64(20))]),
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::U64(30))]),
        ];
        let delete_batch = build_batch(delete_schema, rows).expect("delete batch");

        layout
            .insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("insert delete batch");

        let versions_a = layout
            .inspect_versions(&KeyOwned::from("a"))
            .expect("key a chain");
        assert_eq!(
            versions_a,
            vec![(Timestamp::new(10), true), (Timestamp::new(30), true)]
        );
        let versions_b = layout
            .inspect_versions(&KeyOwned::from("b"))
            .expect("key b chain");
        assert_eq!(versions_b, vec![(Timestamp::new(20), true)]);

        let stats = layout.build_stats(None);
        assert_eq!(stats.dyn_batches, Some(1));
        assert_eq!(stats.entries, 2);
    }

    #[test]
    fn delete_placeholder_handles_timestamp_columns() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let mut layout = DynMem::new(schema.clone());
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let delete_schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let delete_projection =
            projection_for_columns(delete_schema.clone(), vec![0]).expect("delete projection");

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::U64(42)),
        ])];
        let delete_batch = build_batch(delete_schema, rows).expect("delete batch");

        layout
            .insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("insert delete batch");

        let segment = layout
            .seal_into_immutable(&schema, extractor.as_ref())
            .expect("seal succeeds")
            .expect("segment");
        assert_eq!(segment.len(), 1);
    }
}
