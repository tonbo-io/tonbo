use std::{
    collections::{BTreeMap, btree_map::Range as BTreeRange},
    time::Duration,
};

use typed_arrow::arrow_array::RecordBatch;

use super::{KeyHeapSize, MutableLayout, MutableMemTableMetrics};
use crate::{
    inmem::policy::{MemStats, StatsProvider},
    record::extract::{DynKeyExtractor, KeyDyn},
    scan::{KeyRange, RangeSet},
};

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
