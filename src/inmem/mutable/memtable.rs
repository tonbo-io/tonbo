use std::{
    collections::{
        BTreeMap,
        btree_map::{Entry as BTreeEntry, Range as BTreeRange},
    },
    time::Duration,
};

use arrow_array::{RecordBatch, new_null_array};
use arrow_schema::SchemaRef;

use super::{KeyHeapSize, MutableLayout, MutableMemTableMetrics};
use crate::{
    inmem::{
        immutable::memtable::{ImmutableMemTable, VersionSlice, attach_mvcc_columns},
        policy::{MemStats, StatsProvider},
    },
    mvcc::Timestamp,
    record::extract::{DynKeyExtractor, KeyDyn},
    scan::{KeyRange, RangeSet},
};

#[derive(Clone, Debug)]
struct VersionLoc {
    batch_idx: usize,
    row_idx: usize,
    commit_ts: Timestamp,
    tombstone: bool,
}

impl VersionLoc {
    fn new(batch_idx: usize, row_idx: usize, commit_ts: Timestamp, tombstone: bool) -> Self {
        Self {
            batch_idx,
            row_idx,
            commit_ts,
            tombstone,
        }
    }
}

/// Columnar-style mutable table for dynamic mode.
///
/// - Accepts `RecordBatch` inserts; each batch is stored as a sealed chunk.
/// - Maintains per-key version chains ordered by commit timestamp.
pub struct DynMem {
    /// Version chains per key (oldest..newest).
    versions: BTreeMap<KeyDyn, Vec<VersionLoc>>,
    /// Attached batches held until compaction.
    batches_attached: Vec<RecordBatch>,
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
        extractor: &dyn DynKeyExtractor,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), crate::record::extract::KeyExtractError> {
        self.insert_batch_with_ts(extractor, batch, commit_ts, |_| false)
    }

    /// Insert a batch using supplied commit timestamps (replay path).
    pub(crate) fn insert_batch_with_ts<F>(
        &mut self,
        extractor: &dyn DynKeyExtractor,
        batch: RecordBatch,
        commit_ts: Timestamp,
        mut tombstone_at: F,
    ) -> Result<(), crate::record::extract::KeyExtractError>
    where
        F: FnMut(usize) -> bool,
    {
        extractor.validate_schema(&batch.schema())?;
        let batch_id = self.batches_attached.len();
        for row_idx in 0..batch.num_rows() {
            let k = extractor.key_at(&batch, row_idx)?;
            let key_size = k.key_heap_size();
            self.metrics.inserts += 1;

            let version_loc = VersionLoc::new(batch_id, row_idx, commit_ts, tombstone_at(row_idx));

            match self.versions.entry(k) {
                BTreeEntry::Vacant(v) => {
                    self.metrics.entries += 1;
                    self.metrics.approx_key_bytes += key_size;
                    v.insert(vec![version_loc]);
                }
                BTreeEntry::Occupied(mut o) => {
                    self.metrics.replaces += 1;
                    o.get_mut().push(version_loc);
                }
            }
        }
        self.batches_attached.push(batch);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inspect_versions(&self, key: &KeyDyn) -> Option<Vec<(Timestamp, bool)>> {
        self.versions
            .get(key)
            .map(|chain| chain.iter().map(|v| (v.commit_ts, v.tombstone)).collect())
    }

    // Key-only scan helper removed.

    /// Scan dynamic rows in key order returning owned `DynRow`s for each key's
    /// latest visible version across attached batches.
    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<KeyDyn>) -> DynRowScan<'t, 's> {
        self.scan_rows_at(ranges, Timestamp::MAX)
    }

    /// Scan dynamic rows using MVCC visibility semantics at `read_ts`.
    pub(crate) fn scan_rows_at<'t, 's>(
        &'t self,
        ranges: &'s RangeSet<KeyDyn>,
        read_ts: Timestamp,
    ) -> DynRowScan<'t, 's> {
        DynRowScan::new(&self.versions, &self.batches_attached, ranges, read_ts)
    }

    /// Approximate memory usage for keys stored in the mutable table.
    pub(crate) fn approx_bytes(&self) -> usize {
        self.metrics.approx_key_bytes + self.metrics.entries * self.metrics.entry_overhead
    }

    /// Drain and return attached batches accumulated so far.
    #[allow(dead_code)]
    pub(crate) fn take_attached_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.batches_attached)
    }

    pub(crate) fn seal_into_immutable(
        &mut self,
        schema: &SchemaRef,
    ) -> Result<
        Option<ImmutableMemTable<KeyDyn, RecordBatch>>,
        crate::record::extract::KeyExtractError,
    > {
        if self.versions.is_empty() {
            return Ok(None);
        }

        use arrow_select::concat::concat_batches;

        let mut slices = Vec::new();
        let mut commit_ts = Vec::new();
        let mut tombstone = Vec::new();
        let mut index = BTreeMap::new();
        let mut next_key: u32 = 0;
        let mut null_row_batch: Option<RecordBatch> = None;

        let versions = std::mem::take(&mut self.versions);
        for (key, chain) in versions.into_iter() {
            if chain.is_empty() {
                continue;
            }
            let start = next_key;
            let mut chain_rows = 0u32;
            // Versions are appended to each chain in commit order (oldest → newest) as we ingest
            // into the mutable table. By iterating in reverse we emit newest → oldest so the
            // immutable run stores rows per key in descending commit timestamp order.
            for version in chain.iter().rev() {
                let row_batch = if version.tombstone {
                    if null_row_batch.is_none() {
                        let arrays = schema
                            .fields()
                            .iter()
                            .map(|f| new_null_array(f.data_type(), 1))
                            .collect::<Vec<_>>();
                        null_row_batch = Some(RecordBatch::try_new(schema.clone(), arrays)?);
                    }
                    null_row_batch.as_ref().unwrap().clone()
                } else {
                    let batch = &self.batches_attached[version.batch_idx];
                    batch.slice(version.row_idx, 1)
                };
                slices.push(row_batch);
                commit_ts.push(version.commit_ts);
                tombstone.push(version.tombstone);
                chain_rows += 1;
                next_key += 1;
            }
            index.insert(key, VersionSlice::new(start, chain_rows));
        }

        self.batches_attached.clear();
        self.metrics = MutableMemTableMetrics {
            entry_overhead: self.metrics.entry_overhead,
            ..Default::default()
        };

        let batch = concat_batches(schema, &slices)?;
        let (batch, mvcc) = attach_mvcc_columns(batch, commit_ts, tombstone)?;
        Ok(Some(ImmutableMemTable::new(batch, index, mvcc)))
    }
}

impl Default for DynMem {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableLayout<KeyDyn> for DynMem {
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
pub(crate) struct DynRowScan<'t, 's> {
    versions: &'t BTreeMap<KeyDyn, Vec<VersionLoc>>,
    batches: &'t [RecordBatch],
    ranges: &'s [KeyRange<KeyDyn>],
    range_idx: usize,
    cursor: Option<BTreeRange<'t, KeyDyn, Vec<VersionLoc>>>,
    read_ts: Timestamp,
}

impl<'t, 's> DynRowScan<'t, 's> {
    fn new(
        versions: &'t BTreeMap<KeyDyn, Vec<VersionLoc>>,
        batches: &'t [RecordBatch],
        ranges: &'s RangeSet<KeyDyn>,
        read_ts: Timestamp,
    ) -> Self {
        Self {
            versions,
            batches,
            ranges: ranges.as_slice(),
            range_idx: 0,
            cursor: None,
            read_ts,
        }
    }
}

impl<'t, 's> Iterator for DynRowScan<'t, 's> {
    type Item = typed_arrow_dyn::DynRow;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cursor.is_none() {
                if self.range_idx >= self.ranges.len() {
                    return None;
                }
                let (start, end) = self.ranges[self.range_idx].as_borrowed_bounds();
                self.cursor = Some(self.versions.range((start, end)));
                self.range_idx += 1;
            }
            if let Some(cur) = &mut self.cursor {
                if let Some((_k, chain)) = cur.next() {
                    let candidate = chain
                        .iter()
                        .rev()
                        .find(|v| v.commit_ts <= self.read_ts && !v.tombstone);
                    let Some(version) = candidate else {
                        continue;
                    };
                    let b = &self.batches[version.batch_idx];
                    // Safe: version locations originate from these attached batches.
                    let row = crate::record::extract::row_from_batch(b, version.row_idx).unwrap();
                    return Some(row);
                }
                self.cursor = None;
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};

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
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(3))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("ok");
        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");
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
            B::Included(KeyDyn::from("b")),
            B::Unbounded,
        )]);
        let got: Vec<String> = m
            .scan_rows(&rs)
            .map(|row| match &row.0[0] {
                Some(typed_arrow_dyn::DynCell::Str(s)) => s.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(got, vec!["b".to_string()]);

        // Drain attached batches
        let drained = m.take_attached_batches();
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
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");

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

        let ranges = RangeSet::all();

        // Before the first commit nothing should be visible
        let rows_before: Vec<typed_arrow_dyn::DynRow> =
            m.scan_rows_at(&ranges, Timestamp::new(5)).collect();
        assert!(rows_before.is_empty());

        // Between first and second commits the first value is visible
        let rows_after_first: Vec<i32> = m
            .scan_rows_at(&ranges, Timestamp::new(15))
            .map(|row| match &row.0[1] {
                Some(DynCell::I32(v)) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(rows_after_first, vec![1]);

        // Between second and third commits the second value is visible
        let rows_after_second: Vec<i32> = m
            .scan_rows_at(&ranges, Timestamp::new(25))
            .map(|row| match &row.0[1] {
                Some(DynCell::I32(v)) => *v,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(rows_after_second, vec![2]);

        // Between third and fourth commits the third value is visible
        let row_latest: Vec<i32> = m
            .scan_rows_at(&ranges, Timestamp::new(35))
            .map(|row| match &row.0[1] {
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
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");

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
            .seal_into_immutable(&schema)
            .expect("seal ok")
            .expect("segment");
        assert_eq!(segment.len(), 4);

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyDyn::from("k")),
            B::Included(KeyDyn::from("k")),
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
            crate::record::extract::row_from_batch(batch, visible_after_first[0] as usize)
                .expect("row");
        let row_after_second =
            crate::record::extract::row_from_batch(batch, visible_after_second[0] as usize)
                .expect("row");
        let row_after_third =
            crate::record::extract::row_from_batch(batch, visible_after_third[0] as usize)
                .expect("row");
        let row_latest =
            crate::record::extract::row_from_batch(batch, visible_latest[0] as usize).expect("row");

        let cell_value = |row: &typed_arrow_dyn::DynRow| match &row.0[1] {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };

        assert_eq!(cell_value(&row_after_first), 1);
        assert_eq!(cell_value(&row_after_second), 2);
        assert_eq!(cell_value(&row_after_third), 3);
        assert_eq!(cell_value(&row_latest), 4);
    }

    #[test]
    fn insert_batch_with_ts_preserves_metadata() {
        let mut layout = DynMem::new();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");

        let batch1: RecordBatch = build_batch(
            schema.clone(),
            vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch1");
        layout
            .insert_batch_with_ts(extractor.as_ref(), batch1, Timestamp::new(10), |_| false)
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
            .insert_batch_with_ts(extractor.as_ref(), batch2, Timestamp::new(20), |row| {
                row == 0
            })
            .expect("insert batch2");

        let chain = layout
            .versions
            .get(&KeyDyn::from("k"))
            .expect("version chain");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].commit_ts, Timestamp::new(10));
        assert!(!chain[0].tombstone);
        assert_eq!(chain[1].commit_ts, Timestamp::new(20));
        assert!(chain[1].tombstone);
    }
}
