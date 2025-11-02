use std::{
    collections::{BTreeMap, btree_map::Range as BTreeRange},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::{KeyHeapSize, MutableLayout, MutableMemTableMetrics};
use crate::{
    extractor::KeyProjection,
    inmem::{
        immutable::memtable::{ImmutableMemTable, bundle_mvcc_sidecar},
        policy::{MemStats, StatsProvider},
    },
    key::{KeyOwned, KeyTsViewRaw, KeyViewRaw},
    mvcc::Timestamp,
    scan::{KeyRange, RangeSet},
};

#[derive(Clone, Debug)]
struct BatchRowLoc {
    batch_idx: usize,
    row_idx: usize,
    tombstone: bool,
}

impl BatchRowLoc {
    fn new(batch_idx: usize, row_idx: usize, tombstone: bool) -> Self {
        Self {
            batch_idx,
            row_idx,
            tombstone,
        }
    }
}

/// Columnar-style mutable table for dynamic mode.
///
/// - Accepts `RecordBatch` inserts; each batch is stored as a sealed chunk.
/// - Maintains per-key version chains ordered by commit timestamp.
pub struct DynMem {
    /// Ordered versions keyed by `(key, commit_ts)` (ts desc per key).
    versions: BTreeMap<KeyTsViewRaw, BatchRowLoc>,
    /// Attached batches held until compaction.
    batches_attached: Vec<Arc<RecordBatch>>,
    metrics: MutableMemTableMetrics,
}

impl DynMem {
    /// Create an empty columnar mutable table for dynamic batches.
    pub(crate) fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
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
        self.insert_batch_with_ts(extractor, batch, commit_ts, |_| false)
    }

    /// Insert a batch using supplied commit timestamps (replay path).
    pub(crate) fn insert_batch_with_ts<F>(
        &mut self,
        extractor: &dyn KeyProjection,
        batch: RecordBatch,
        commit_ts: Timestamp,
        mut tombstone_at: F,
    ) -> Result<(), crate::extractor::KeyExtractError>
    where
        F: FnMut(usize) -> bool,
    {
        extractor.validate_schema(&batch.schema())?;
        let batch_arc = Arc::new(batch);
        let batch_ref = batch_arc.as_ref();
        let batch_id = self.batches_attached.len();
        for row_idx in 0..batch_ref.num_rows() {
            let mut raw = KeyViewRaw::new();
            extractor.project_view(batch_ref, row_idx, &mut raw)?;
            let key_size = raw.key_heap_size();
            let has_existing = self
                .versions
                .range(
                    unsafe { KeyTsViewRaw::new_unchecked(raw.clone(), Timestamp::MAX) }
                        ..=unsafe { KeyTsViewRaw::new_unchecked(raw.clone(), Timestamp::MIN) },
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

            let version_loc = BatchRowLoc::new(batch_id, row_idx, tombstone_at(row_idx));
            let composite = unsafe { KeyTsViewRaw::new_unchecked(raw, commit_ts) };
            self.versions.insert(composite, version_loc);
        }
        self.batches_attached.push(batch_arc);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inspect_versions(&self, key: &KeyOwned) -> Option<Vec<(Timestamp, bool)>> {
        let raw = KeyViewRaw::from_owned(key);
        let mut out = Vec::new();
        for (composite, loc) in self.versions.range(
            unsafe { KeyTsViewRaw::new_unchecked(raw.clone(), Timestamp::MAX) }
                ..=unsafe { KeyTsViewRaw::new_unchecked(raw.clone(), Timestamp::MIN) },
        ) {
            if composite.key() != &raw {
                break;
            }
            out.push((composite.timestamp(), loc.tombstone));
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
        DynRowScan::new(&self.versions, &self.batches_attached, converted, read_ts)
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
            .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|rc| rc.as_ref().clone()))
            .collect()
    }

    pub(crate) fn seal_into_immutable(
        &mut self,
        schema: &SchemaRef,
        extractor: &dyn KeyProjection,
    ) -> Result<Option<ImmutableMemTable<RecordBatch>>, crate::extractor::KeyExtractError> {
        if self.versions.is_empty() {
            return Ok(None);
        }

        use arrow_select::concat::concat_batches;

        let mut slices = Vec::new();
        let mut commit_ts = Vec::new();
        let mut tombstone = Vec::new();

        let versions = std::mem::take(&mut self.versions);
        let mut entries: Vec<(KeyOwned, Timestamp, BatchRowLoc)> = versions
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
            let batch_arc = &self.batches_attached[version.batch_idx];
            let batch = batch_arc.as_ref();
            let row_batch = batch.slice(version.row_idx, 1);
            slices.push(row_batch);
            commit_ts.push(commit);
            tombstone.push(version.tombstone);
        }

        self.batches_attached.clear();
        self.metrics = MutableMemTableMetrics {
            entry_overhead: self.metrics.entry_overhead,
            ..Default::default()
        };

        let batch = concat_batches(schema, &slices)?;
        let (batch, mvcc) = bundle_mvcc_sidecar(batch, commit_ts, tombstone)?;

        let mut view = KeyViewRaw::new();
        let mut composite_index: BTreeMap<KeyTsViewRaw, u32> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            view.clear();
            extractor.project_view(&batch, row, &mut view)?;
            composite_index.insert(
                unsafe { KeyTsViewRaw::new_unchecked(view.clone(), mvcc.commit_ts[row]) },
                row as u32,
            );
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
    versions: &'t BTreeMap<KeyTsViewRaw, BatchRowLoc>,
    batches: &'t [Arc<RecordBatch>],
    _owned_ranges: RangeSet<KeyOwned>,
    composite_ranges: RangeSet<KeyTsViewRaw>,
    range_idx: usize,
    cursor: Option<BTreeRange<'t, KeyTsViewRaw, BatchRowLoc>>,
    read_ts: Timestamp,
    current_key: Option<KeyViewRaw>,
    emitted_for_key: bool,
}

impl<'t> DynRowScan<'t> {
    fn new(
        versions: &'t BTreeMap<KeyTsViewRaw, BatchRowLoc>,
        batches: &'t [Arc<RecordBatch>],
        converted: ConvertedRanges,
        read_ts: Timestamp,
    ) -> Self {
        Self {
            versions,
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
                self.cursor = Some(self.versions.range((start, end)));
                self.range_idx += 1;
                self.current_key = None;
                self.emitted_for_key = false;
            }
            if let Some(cur) = &mut self.cursor {
                while let Some((composite, loc)) = cur.next() {
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

                    if loc.tombstone {
                        continue;
                    }

                    let batch = self.batches[loc.batch_idx].as_ref();
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
        Bound::Included(key) => {
            Bound::Included(KeyTsViewRaw::from_owned(key, Timestamp::MAX))
        }
        Bound::Excluded(key) => {
            Bound::Excluded(KeyTsViewRaw::from_owned(key, Timestamp::MIN))
        }
    }
}

fn convert_upper_bound(bound: &Bound<KeyOwned>) -> Bound<KeyTsViewRaw> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => {
            Bound::Included(KeyTsViewRaw::from_owned(key, Timestamp::MIN))
        }
        Bound::Excluded(key) => {
            Bound::Excluded(KeyTsViewRaw::from_owned(key, Timestamp::MAX))
        }
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
            crate::extractor::projection_for_field(0, &DataType::Utf8).expect("extractor");
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
            crate::extractor::projection_for_field(0, &DataType::Utf8).expect("extractor");

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
            crate::extractor::projection_for_field(0, &DataType::Utf8).expect("extractor");

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
            crate::extractor::projection_for_field(0, &DataType::Utf8).expect("extractor");

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
                .insert_batch_with_ts(extractor.as_ref(), batch, Timestamp::new(ts), move |_| tomb)
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
    fn insert_batch_with_ts_preserves_metadata() {
        let mut layout = DynMem::new();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(0, &DataType::Utf8).expect("extractor");

        let batch1: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]],
        )
        .expect("batch1");
        layout
            .insert_batch_with_ts(extractor.as_ref(), batch1, Timestamp::new(10), |_| false)
            .expect("insert batch1");

        let batch2: RecordBatch = build_batch(
            schema.clone(),
            vec![vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]],
        )
        .expect("batch2");
        layout
            .insert_batch_with_ts(extractor.as_ref(), batch2, Timestamp::new(20), |row| {
                row == 0
            })
            .expect("insert batch2");

        let chain = layout
            .inspect_versions(&KeyOwned::from("k"))
            .expect("version chain");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0], (Timestamp::new(10), false));
        assert_eq!(chain[1], (Timestamp::new(20), true));
    }
}
