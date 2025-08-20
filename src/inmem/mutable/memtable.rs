use std::{
    collections::{BTreeMap, btree_map::Range as BTreeRange},
    time::Duration,
};

use typed_arrow::arrow_array::RecordBatch;

use super::{KeyHeapSize, MutableLayout, MutableMemTableMetrics};
use crate::{
    inmem::policy::{MemStats, StatsProvider},
    record::{
        Record,
        extract::{DynKeyExtractor, KeyDyn},
    },
    scan::{KeyRange, RangeSet},
};

/// Iterator over typed rows in key order using the latest-location index.
pub(crate) struct RowScan<'t, 's, R: Record>
where
    R::Key: Ord,
{
    index: &'t BTreeMap<R::Key, usize>,
    rows: &'t [R],
    ranges: &'s [KeyRange<R::Key>],
    range_idx: usize,
    cursor: Option<BTreeRange<'t, R::Key, usize>>,
}

impl<'t, 's, R> RowScan<'t, 's, R>
where
    R: Record,
    R::Key: Ord,
{
    fn new(
        index: &'t BTreeMap<R::Key, usize>,
        rows: &'t [R],
        ranges: &'s RangeSet<R::Key>,
    ) -> Self {
        Self {
            index,
            rows,
            ranges: ranges.as_slice(),
            range_idx: 0,
            cursor: None,
        }
    }
}

impl<'t, 's, R> Iterator for RowScan<'t, 's, R>
where
    R: Record,
    R::Key: Ord,
{
    type Item = &'t R;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cursor.is_none() {
                if self.range_idx >= self.ranges.len() {
                    return None;
                }
                let (start, end) = self.ranges[self.range_idx].as_borrowed_bounds();
                self.cursor = Some(self.index.range((start, end)));
                self.range_idx += 1;
            }
            if let Some(cur) = &mut self.cursor {
                if let Some((_k, idx)) = cur.next() {
                    return self.rows.get(*idx);
                }
                self.cursor = None;
                continue;
            }
        }
    }
}

/// Columnar-style mutable table for typed mode.
///
/// - Appends typed rows to an open buffer; index maps key to the last writer.
/// - Sealing converts the open buffer into an immutable typed chunk.
pub(crate) struct TypedLayout<R: Record>
where
    R::Key: Ord,
{
    /// Latest location per key in the open buffer (index into `active_rows`).
    loc_index: BTreeMap<R::Key, usize>,
    /// Active in-memory row buffer (unsorted, append-only until seal).
    active_rows: Vec<R>,
    metrics: MutableMemTableMetrics,
}

impl<R> TypedLayout<R>
where
    R: Record,
    R::Key: Ord + KeyHeapSize,
{
    /// Create an empty columnar mutable table for typed records.
    pub(crate) fn new() -> Self {
        Self {
            loc_index: BTreeMap::new(),
            active_rows: Vec::new(),
            metrics: MutableMemTableMetrics {
                entry_overhead: 32,
                ..Default::default()
            },
        }
    }

    /// Insert a typed row: append to the open buffer and update last-writer index.
    pub(crate) fn insert(&mut self, row: R) {
        let key = row.key();
        let key_size = key.key_heap_size();
        let is_new = !self.loc_index.contains_key(&key);
        self.active_rows.push(row);
        let idx = self.active_rows.len() - 1;
        if is_new {
            self.metrics.entries += 1;
            self.metrics.inserts += 1;
            self.metrics.approx_key_bytes += key_size;
        } else {
            self.metrics.replaces += 1;
            self.metrics.inserts += 1;
        }
        self.loc_index.insert(key, idx);
    }

    /// Seal the current open buffer and return drained rows (if any).
    /// Keeps the last-writer index intact. Conversion to immutable arrays
    /// happens at the DB boundary.
    pub(crate) fn seal_open(&mut self) -> Option<Vec<R>> {
        if self.active_rows.is_empty() {
            return None;
        }
        let rows = std::mem::take(&mut self.active_rows);
        // Offsets into the open buffer are now invalid.
        self.loc_index.clear();
        Some(rows)
    }

    // Key-only scan helper removed.

    /// Approximate memory usage for keys stored in the mutable table.
    pub(crate) fn approx_bytes(&self) -> usize {
        self.metrics.approx_key_bytes + self.metrics.entries * self.metrics.entry_overhead
    }

    // No sealed rows queue: sealing returns drained rows directly.

    /// Scan rows in range order from the open buffer. Returns references
    /// to the latest row per key as seen in `active_rows`.
    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<R::Key>) -> RowScan<'t, 's, R>
    where
        't: 's,
    {
        RowScan::new(&self.loc_index, &self.active_rows, ranges)
    }
}

impl<R> MutableLayout<R::Key> for TypedLayout<R>
where
    R: Record,
    R::Key: Ord + KeyHeapSize,
{
    fn approx_bytes(&self) -> usize {
        self.approx_bytes()
    }
}

impl<R> Default for TypedLayout<R>
where
    R: Record,
    R::Key: Ord + KeyHeapSize,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Columnar-style mutable table for dynamic mode.
///
/// - Accepts `RecordBatch` inserts; each batch is stored as a sealed chunk.
/// - Index maps key to the last writer among attached chunks.
pub(crate) struct DynLayout {
    /// Latest location per key across attached batches: (batch_idx, row_idx).
    loc_index: BTreeMap<KeyDyn, (usize, usize)>,
    /// Attached batches held until compaction.
    batches_attached: Vec<RecordBatch>,
    metrics: MutableMemTableMetrics,
}

impl DynLayout {
    /// Create an empty columnar mutable table for dynamic batches.
    pub(crate) fn new() -> Self {
        Self {
            loc_index: BTreeMap::new(),
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
    ) -> Result<(), crate::record::extract::KeyExtractError> {
        extractor.validate_schema(&batch.schema())?;
        let batch_id = self.batches_attached.len();
        for row_idx in 0..batch.num_rows() {
            let k = extractor.key_at(&batch, row_idx)?;
            let key_size = k.key_heap_size();
            let is_new = !self.loc_index.contains_key(&k);
            if is_new {
                self.metrics.entries += 1;
                self.metrics.approx_key_bytes += key_size;
            } else {
                self.metrics.replaces += 1;
            }
            self.metrics.inserts += 1;
            // Update latest location for this key
            let loc = (batch_id, row_idx);
            self.loc_index.insert(k, loc);
        }
        self.batches_attached.push(batch);
        Ok(())
    }

    // Key-only scan helper removed.

    /// Scan dynamic rows in key order returning owned `DynRow`s for each key's
    /// latest location across attached batches.
    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<KeyDyn>) -> DynRowScan<'t, 's> {
        DynRowScan::new(&self.loc_index, &self.batches_attached, ranges)
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
}

impl Default for DynLayout {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableLayout<KeyDyn> for DynLayout {
    fn approx_bytes(&self) -> usize {
        self.approx_bytes()
    }
}

// ---- StatsProvider implementations ----

impl<R> StatsProvider for TypedLayout<R>
where
    R: Record,
    R::Key: Ord + KeyHeapSize,
{
    fn build_stats(&self, since_last_seal: Option<Duration>) -> MemStats {
        MemStats {
            entries: self.metrics.entries,
            inserts: self.metrics.inserts,
            replaces: self.metrics.replaces,
            approx_key_bytes: self.metrics.approx_key_bytes,
            entry_overhead: self.metrics.entry_overhead,
            typed_open_rows: Some(self.active_rows.len()),
            dyn_batches: None,
            dyn_approx_batch_bytes: None,
            since_last_seal,
        }
    }
}

impl StatsProvider for DynLayout {
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
    locs: &'t BTreeMap<KeyDyn, (usize, usize)>,
    batches: &'t [RecordBatch],
    ranges: &'s [KeyRange<KeyDyn>],
    range_idx: usize,
    cursor: Option<BTreeRange<'t, KeyDyn, (usize, usize)>>,
}

impl<'t, 's> DynRowScan<'t, 's> {
    fn new(
        locs: &'t BTreeMap<KeyDyn, (usize, usize)>,
        batches: &'t [RecordBatch],
        ranges: &'s RangeSet<KeyDyn>,
    ) -> Self {
        Self {
            locs,
            batches,
            ranges: ranges.as_slice(),
            range_idx: 0,
            cursor: None,
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
                self.cursor = Some(self.locs.range((start, end)));
                self.range_idx += 1;
            }
            if let Some(cur) = &mut self.cursor {
                if let Some((_k, (batch_idx, row_idx))) = cur.next() {
                    let b = &self.batches[*batch_idx];
                    // Safe: loc_index constructed from these batches
                    let row = crate::record::extract::row_from_batch(b, *row_idx).unwrap();
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
    use typed_arrow::arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};
    use typed_arrow_unified::SchemaLike;

    use super::*;
    use crate::inmem::policy::StatsProvider;

    #[derive(typed_arrow::Record, Clone)]
    #[record(field_macro = crate::key_field)]
    struct RowT {
        #[record(ext(key))]
        id: u32,
        v: i32,
    }

    #[test]
    fn typed_stats_and_scan() {
        let mut m = TypedLayout::<RowT>::new();
        m.insert(RowT { id: 2, v: 1 });
        m.insert(RowT { id: 1, v: 2 });
        m.insert(RowT { id: 2, v: 3 }); // replace

        // Stats reflect inserts/replaces and distinct entries
        let s = m.build_stats(None);
        assert_eq!(s.inserts, 3);
        assert_eq!(s.replaces, 1);
        assert_eq!(s.entries, 2);
        assert_eq!(s.typed_open_rows, Some(3));

        // Scan over [1,2] inclusive -> rows for keys 1 and 2 (latest)
        use std::ops::Bound as B;
        let rs = RangeSet::from_ranges(vec![KeyRange::new(B::Included(1u32), B::Included(2u32))]);
        let got: Vec<u32> = m.scan_rows(&rs).map(|r| r.id).collect();
        assert_eq!(got, vec![1, 2]);

        // Seal and ensure open rows drained are returned
        let drained = m.seal_open();
        let s2 = m.build_stats(None);
        assert_eq!(s2.typed_open_rows, Some(0));
        let drained = drained.expect("rows drained");
        assert_eq!(drained.len(), 3);
    }

    #[test]
    fn dyn_stats_and_scan() {
        let mut m = DynLayout::new();
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
        let batch: RecordBatch = schema.build_batch(rows).expect("ok");
        let extractor =
            crate::record::extract::dyn_extractor_for_field(0, &DataType::Utf8).expect("extractor");
        m.insert_batch(extractor.as_ref(), batch).expect("insert");

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
}
