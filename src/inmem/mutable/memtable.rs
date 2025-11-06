use std::{
    collections::{BTreeMap, btree_map::Range as BTreeRange},
    ops::Bound,
    time::Duration,
};

use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;

use super::{MutableLayout, MutableMemTableMetrics};
use crate::{
    extractor::KeyProjection,
    inmem::{
        immutable::memtable::{ImmutableMemTable, bundle_mvcc_sidecar},
        policy::{MemStats, StatsProvider},
    },
    key::{KeyOwned, KeyRow, KeyTsViewRaw},
    mvcc::Timestamp,
    scan::{KeyRange, RangeSet},
};

struct BatchAttachment {
    storage: RecordBatch,
    commit_ts: UInt64Array,
    tombstones: BooleanArray,
}

impl BatchAttachment {
    fn new(storage: RecordBatch, commit_ts: UInt64Array, tombstones: BooleanArray) -> Self {
        Self {
            storage,
            commit_ts,
            tombstones,
        }
    }

    fn storage(&self) -> &RecordBatch {
        &self.storage
    }

    fn tombstone(&self, row: usize) -> bool {
        self.tombstones.value(row)
    }

    fn commit_ts(&self, row: usize) -> Timestamp {
        Timestamp::new(self.commit_ts.value(row))
    }

    fn into_storage(self) -> RecordBatch {
        self.storage
    }
}

#[derive(Clone, Debug)]
struct BatchRowLoc {
    batch_idx: usize,
    row_idx: usize,
}

impl BatchRowLoc {
    fn new(batch_idx: usize, row_idx: usize) -> Self {
        Self { batch_idx, row_idx }
    }
}

/// Columnar-style mutable table for dynamic mode.
///
/// - Accepts `RecordBatch` inserts; each batch is stored as a sealed chunk.
/// - Maintains per-key version chains ordered by commit timestamp.
pub struct DynMem {
    /// MVCC index keyed by `(key, commit_ts)` (timestamp descending per key).
    index: BTreeMap<KeyTsViewRaw, BatchRowLoc>,
    /// Attached batches held until compaction.
    batches_attached: Vec<BatchAttachment>,
    metrics: MutableMemTableMetrics,
}

impl DynMem {
    /// Create an empty columnar mutable table for dynamic batches.
    pub(crate) fn new() -> Self {
        Self {
            index: BTreeMap::new(),
            batches_attached: Vec::new(),
            metrics: MutableMemTableMetrics {
                entry_overhead: 32,
                ..Default::default()
            },
        }
    }

    /// Insert a dynamic batch by indexing each row's key.
    pub(crate) fn insert_batch(
        &mut self,
        extractor: &dyn KeyProjection,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), crate::extractor::KeyExtractError> {
        let rows = batch.num_rows();
        let commit_ts_column = UInt64Array::from(vec![commit_ts.get(); rows]);
        let tombstone_column = BooleanArray::from(vec![false; rows]);
        self.insert_batch_with_mvcc(extractor, batch, commit_ts_column, tombstone_column)
    }

    /// Insert a batch using explicit MVCC metadata columns.
    pub(crate) fn insert_batch_with_mvcc(
        &mut self,
        extractor: &dyn KeyProjection,
        batch: RecordBatch,
        commit_ts_column: UInt64Array,
        tombstone_column: BooleanArray,
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
        if tombstone_column.len() != rows {
            return Err(crate::extractor::KeyExtractError::TombstoneLengthMismatch {
                expected: rows,
                actual: tombstone_column.len(),
            });
        }
        if tombstone_column.null_count() > 0 {
            return Err(crate::extractor::KeyExtractError::Arrow(
                arrow_schema::ArrowError::ComputeError(
                    "tombstone column contained null".to_string(),
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

        let batch_id = self.batches_attached.len();
        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let key_rows = extractor.project_view(&batch, &row_indices)?;
        for (row_idx, key_row) in key_rows.into_iter().enumerate() {
            let key_size = key_row.heap_size();
            let has_existing = self
                .index
                .range(
                    KeyTsViewRaw::new(key_row.clone(), Timestamp::MAX)
                        ..=KeyTsViewRaw::new(key_row.clone(), Timestamp::MIN),
                )
                .next()
                .is_some();
            self.metrics.inserts += 1;
            if has_existing {
                self.metrics.replaces += 1;
            } else {
                self.metrics.entries += 1;
                self.metrics.approx_key_bytes += key_size;
            }

            let commit_ts = Timestamp::new(commit_ts_column.value(row_idx));
            let version_loc = BatchRowLoc::new(batch_id, row_idx);
            let composite = KeyTsViewRaw::new(key_row, commit_ts);
            self.index.insert(composite, version_loc);
        }
        self.batches_attached.push(BatchAttachment::new(
            batch,
            commit_ts_column,
            tombstone_column,
        ));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inspect_versions(&self, key: &KeyOwned) -> Option<Vec<(Timestamp, bool)>> {
        let key_row =
            KeyRow::from_owned(key).expect("test keys should only contain supported components");
        let mut out = Vec::new();
        for (composite, loc) in self.index.range(
            KeyTsViewRaw::new(key_row.clone(), Timestamp::MAX)
                ..=KeyTsViewRaw::new(key_row.clone(), Timestamp::MIN),
        ) {
            if composite.key() != &key_row {
                break;
            }
            let attachment = &self.batches_attached[loc.batch_idx];
            out.push((composite.timestamp(), attachment.tombstone(loc.row_idx)));
        }
        if out.is_empty() {
            None
        } else {
            out.reverse();
            Some(out)
        }
    }

    /// Scan dynamic rows in key order returning owned `Vec<Option<DynCell>>` for each key's
    /// latest visible version across attached batches.
    pub(crate) fn scan_rows<'t>(&'t self, ranges: &RangeSet<KeyOwned>) -> DynRowScan<'t> {
        self.scan_rows_at(ranges, Timestamp::MAX)
    }

    /// Scan dynamic rows using MVCC visibility semantics at `read_ts`.
    pub(crate) fn scan_rows_at<'t>(
        &'t self,
        ranges: &RangeSet<KeyOwned>,
        read_ts: Timestamp,
    ) -> DynRowScan<'t> {
        let converted = convert_ranges(ranges);
        DynRowScan::new(&self.index, &self.batches_attached, converted, read_ts)
    }

    /// Approximate memory usage for keys stored in the mutable table.
    pub(crate) fn approx_bytes(&self) -> usize {
        self.metrics.approx_key_bytes + self.metrics.entries * self.metrics.entry_overhead
    }

    /// Consume the memtable and return any batches that were still pinned.
    ///
    /// This keeps the borrowed key views sound by dropping the pinned owners at
    /// the same time the batches are released.
    #[allow(dead_code)]
    pub(crate) fn into_attached_batches(self) -> Vec<RecordBatch> {
        self.batches_attached
            .into_iter()
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

        let mut slices = Vec::new();
        let mut commit_ts = Vec::new();
        let mut tombstone = Vec::new();

        let index = std::mem::take(&mut self.index);
        let mut entries: Vec<(KeyOwned, Timestamp, BatchRowLoc)> = index
            .into_iter()
            .map(|(view, loc)| (view.key().to_owned(), view.timestamp(), loc))
            .collect();
        entries.sort_by(
            |(key_a, ts_a, _), (key_b, ts_b, _)| match key_a.cmp(key_b) {
                std::cmp::Ordering::Equal => ts_b.cmp(ts_a),
                other => other,
            },
        );

        for (_key, commit, version) in entries.into_iter() {
            let attachment = &self.batches_attached[version.batch_idx];
            let batch = attachment.storage();
            let row_batch = batch.slice(version.row_idx, 1);
            slices.push(row_batch);
            let attachment_commit = attachment.commit_ts(version.row_idx);
            debug_assert_eq!(attachment_commit, commit);
            commit_ts.push(attachment_commit);
            tombstone.push(attachment.tombstone(version.row_idx));
        }

        self.batches_attached.clear();
        self.metrics = MutableMemTableMetrics {
            entry_overhead: self.metrics.entry_overhead,
            ..Default::default()
        };

        let batch = concat_batches(schema, &slices)?;
        let (batch, mvcc) = bundle_mvcc_sidecar(batch, commit_ts, tombstone)?;

        let mut composite_index: BTreeMap<KeyTsViewRaw, u32> = BTreeMap::new();
        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let key_rows = extractor.project_view(&batch, &row_indices)?;
        for (row, key_row) in key_rows.into_iter().enumerate() {
            composite_index.insert(KeyTsViewRaw::new(key_row, mvcc.commit_ts[row]), row as u32);
        }

        Ok(Some(ImmutableMemTable::new(batch, composite_index, mvcc)))
    }
}

impl Default for DynMem {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableLayout<KeyOwned> for DynMem {
    fn approx_bytes(&self) -> usize {
        self.approx_bytes()
    }
}

// ---- StatsProvider implementations ----

impl StatsProvider for DynMem {
    fn build_stats(&self, since_last_seal: Option<Duration>) -> MemStats {
        MemStats {
            entries: self.metrics.entries,
            inserts: self.metrics.inserts,
            replaces: self.metrics.replaces,
            approx_key_bytes: self.metrics.approx_key_bytes,
            entry_overhead: self.metrics.entry_overhead,
            typed_open_rows: None,
            dyn_batches: Some(self.batches_attached.len()),
            dyn_approx_batch_bytes: None,
            since_last_seal,
        }
    }
}

/// Iterator over dynamic rows by key ranges, materializing from `RecordBatch`es.
struct ConvertedRanges {
    owned: RangeSet<KeyOwned>,
    composite: RangeSet<KeyTsViewRaw>,
}

pub(crate) struct DynRowScan<'t> {
    index: &'t BTreeMap<KeyTsViewRaw, BatchRowLoc>,
    batches: &'t [BatchAttachment],
    _owned_ranges: RangeSet<KeyOwned>,
    composite_ranges: RangeSet<KeyTsViewRaw>,
    range_idx: usize,
    cursor: Option<BTreeRange<'t, KeyTsViewRaw, BatchRowLoc>>,
    read_ts: Timestamp,
    current_key: Option<KeyRow>,
    emitted_for_key: bool,
}

impl<'t> DynRowScan<'t> {
    fn new(
        index: &'t BTreeMap<KeyTsViewRaw, BatchRowLoc>,
        batches: &'t [BatchAttachment],
        converted: ConvertedRanges,
        read_ts: Timestamp,
    ) -> Self {
        Self {
            index,
            batches,
            _owned_ranges: converted.owned,
            composite_ranges: converted.composite,
            range_idx: 0,
            cursor: None,
            read_ts,
            current_key: None,
            emitted_for_key: false,
        }
    }
}

impl<'t> Iterator for DynRowScan<'t> {
    type Item = Vec<Option<typed_arrow_dyn::DynCell>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cursor.is_none() {
                if self.range_idx >= self.composite_ranges.as_slice().len() {
                    return None;
                }
                let (start, end) =
                    self.composite_ranges.as_slice()[self.range_idx].as_borrowed_bounds();
                self.cursor = Some(self.index.range((start, end)));
                self.range_idx += 1;
                self.current_key = None;
                self.emitted_for_key = false;
            }
            if let Some(cur) = &mut self.cursor {
                for (composite, loc) in cur.by_ref() {
                    let key_raw = composite.key();
                    if self
                        .current_key
                        .as_ref()
                        .map(|k| k == key_raw)
                        .unwrap_or(false)
                    {
                        if self.emitted_for_key {
                            continue;
                        }
                    } else {
                        self.current_key = Some(key_raw.clone());
                        self.emitted_for_key = false;
                    }

                    if composite.timestamp() > self.read_ts {
                        continue;
                    }

                    let attachment = &self.batches[loc.batch_idx];
                    if attachment.tombstone(loc.row_idx) {
                        continue;
                    }

                    let batch = attachment.storage();
                    let row = crate::extractor::row_from_batch(batch, loc.row_idx).unwrap();
                    self.emitted_for_key = true;
                    return Some(row);
                }
                self.cursor = None;
                continue;
            }
        }
    }
}

fn convert_ranges(ranges: &RangeSet<KeyOwned>) -> ConvertedRanges {
    let owned = ranges.clone();

    let composite_ranges: Vec<KeyRange<KeyTsViewRaw>> = owned
        .as_slice()
        .iter()
        .map(|range| {
            KeyRange::new(
                convert_lower_bound(&range.start),
                convert_upper_bound(&range.end),
            )
        })
        .collect();
    let composite = RangeSet::from_ranges(composite_ranges);

    ConvertedRanges { owned, composite }
}

fn convert_lower_bound(bound: &Bound<KeyOwned>) -> Bound<KeyTsViewRaw> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(KeyTsViewRaw::from_owned(key, Timestamp::MAX)),
        Bound::Excluded(key) => Bound::Excluded(KeyTsViewRaw::from_owned(key, Timestamp::MIN)),
    }
}

fn convert_upper_bound(bound: &Bound<KeyOwned>) -> Bound<KeyTsViewRaw> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(KeyTsViewRaw::from_owned(key, Timestamp::MIN)),
        Bound::Excluded(key) => Bound::Excluded(KeyTsViewRaw::from_owned(key, Timestamp::MAX)),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::DynCell;

    use super::*;
    use crate::{inmem::policy::StatsProvider, test_util::build_batch};

    #[test]
    fn dyn_stats_and_scan() {
        let mut m = DynMem::new();
        // Build a batch: id Utf8 is key
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![
            vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))],
            vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))],
            vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(3))],
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

        // Scan >= "b" -> rows where id >= "b" (latest per key)
        use std::ops::Bound as B;
        let rs = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyOwned::from("b")),
            B::Unbounded,
        )]);
        let got: Vec<String> = m
            .scan_rows(&rs)
            .map(|row| match &row[0] {
                Some(typed_arrow_dyn::DynCell::Str(s)) => s.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(got, vec!["b".to_string()]);

        // Drain attached batches
        let drained = m.into_attached_batches();
        assert_eq!(drained.len(), 1);
    }

    #[test]
    fn mvcc_scan_respects_read_ts() {
        let mut m = DynMem::new();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        // First commit at ts=10
        let rows_v1 = vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]];
        let batch_v1: RecordBatch = build_batch(schema.clone(), rows_v1).expect("batch v1");
        m.insert_batch(extractor.as_ref(), batch_v1, Timestamp::new(10))
            .expect("insert v1");

        // Second commit overwrites key at ts=20
        let rows_v2 = vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]];
        let batch_v2: RecordBatch = build_batch(schema.clone(), rows_v2).expect("batch v2");
        m.insert_batch(extractor.as_ref(), batch_v2, Timestamp::new(20))
            .expect("insert v2");

        // Third commit overwrites key at ts=30
        let rows_v3 = vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(3))]];
        let batch_v3: RecordBatch = build_batch(schema.clone(), rows_v3).expect("batch v3");
        m.insert_batch(extractor.as_ref(), batch_v3, Timestamp::new(30))
            .expect("insert v3");

        let ranges = RangeSet::all();

        // Before the first commit nothing should be visible
        let rows_before: Vec<Vec<Option<DynCell>>> =
            m.scan_rows_at(&ranges, Timestamp::new(5)).collect();
        assert!(rows_before.is_empty());

        // Between first and second commits the first value is visible
        let rows_after_first: Vec<i32> = m
            .scan_rows_at(&ranges, Timestamp::new(15))
            .map(|row| match &row[1] {
                Some(DynCell::I32(v)) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(rows_after_first, vec![1]);

        // Between second and third commits the second value is visible
        let rows_after_second: Vec<i32> = m
            .scan_rows_at(&ranges, Timestamp::new(25))
            .map(|row| match &row[1] {
                Some(DynCell::I32(v)) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(rows_after_second, vec![2]);

        // Between third and fourth commits the third value is visible
        let row_latest: Vec<i32> = m
            .scan_rows_at(&ranges, Timestamp::new(35))
            .map(|row| match &row[1] {
                Some(DynCell::I32(v)) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(row_latest, vec![3]);
    }

    #[test]
    fn seal_into_immutable_emits_mvcc_segments() {
        let mut layout = DynMem::new();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        // four versions for the same key
        let batch1: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]],
        )
        .expect("batch1");
        layout
            .insert_batch(extractor.as_ref(), batch1, Timestamp::new(10))
            .expect("insert");

        let batch2: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]],
        )
        .expect("batch2");
        layout
            .insert_batch(extractor.as_ref(), batch2, Timestamp::new(20))
            .expect("insert");

        let batch3: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(3))]],
        )
        .expect("batch3");
        layout
            .insert_batch(extractor.as_ref(), batch3, Timestamp::new(30))
            .expect("insert");

        let batch4: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(4))]],
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

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyOwned::from("k")),
            B::Included(KeyOwned::from("k")),
        )]);

        let visible_after_first: Vec<u32> = segment
            .scan_visible(&ranges, Timestamp::new(15))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(visible_after_first, vec![3]);

        let visible_after_second: Vec<u32> = segment
            .scan_visible(&ranges, Timestamp::new(25))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(visible_after_second, vec![2]);

        let visible_after_third: Vec<u32> = segment
            .scan_visible(&ranges, Timestamp::new(35))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(visible_after_third, vec![1]);

        let visible_latest: Vec<u32> = segment
            .scan_visible(&ranges, Timestamp::new(45))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(visible_latest, vec![0]);

        let batch = segment.storage();
        let row_after_first =
            crate::extractor::row_from_batch(batch, visible_after_first[0] as usize).expect("row");
        let row_after_second =
            crate::extractor::row_from_batch(batch, visible_after_second[0] as usize).expect("row");
        let row_after_third =
            crate::extractor::row_from_batch(batch, visible_after_third[0] as usize).expect("row");
        let row_latest =
            crate::extractor::row_from_batch(batch, visible_latest[0] as usize).expect("row");

        let cell_value = |row: &[Option<DynCell>]| match &row[1] {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };

        assert_eq!(cell_value(&row_after_first), 1);
        assert_eq!(cell_value(&row_after_second), 2);
        assert_eq!(cell_value(&row_after_third), 3);
        assert_eq!(cell_value(&row_latest), 4);
    }

    #[test]
    fn sealed_segment_row_iter_matches_versions() {
        let mut layout = DynMem::new();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let insert = |layout: &mut DynMem, val: i32, ts: u64, tomb: bool| {
            let batch: RecordBatch = build_batch(
                schema.clone(),
                vec![vec![
                    Some(DynCell::Str("k".into())),
                    Some(DynCell::I32(val)),
                ]],
            )
            .expect("batch");
            layout
                .insert_batch_with_mvcc(
                    extractor.as_ref(),
                    batch,
                    UInt64Array::from(vec![ts]),
                    BooleanArray::from(vec![tomb]),
                )
                .expect("insert");
        };

        // NOTE: tombstoned versions retain their original key/value payloads. They are filtered
        // at read time via the `tombstone` flag rather than by zeroing the row.
        insert(&mut layout, 1, 10, false);
        insert(&mut layout, 2, 20, true);
        insert(&mut layout, 3, 30, false);

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
        let mut layout = DynMem::new();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");

        let batch1: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]],
        )
        .expect("batch1");
        layout
            .insert_batch_with_mvcc(
                extractor.as_ref(),
                batch1,
                UInt64Array::from(vec![10]),
                BooleanArray::from(vec![false]),
            )
            .expect("insert batch1");

        let batch2: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]],
        )
        .expect("batch2");
        layout
            .insert_batch_with_mvcc(
                extractor.as_ref(),
                batch2,
                UInt64Array::from(vec![20]),
                BooleanArray::from(vec![true]),
            )
            .expect("insert batch2");

        let chain = layout
            .inspect_versions(&KeyOwned::from("k"))
            .expect("version chain");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0], (Timestamp::new(10), false));
        assert_eq!(chain[1], (Timestamp::new(20), true));
    }
}
