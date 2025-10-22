#![allow(dead_code)]
use std::collections::{BTreeMap, btree_map::Range as BTreeRange};

use arrow_array::RecordBatch;

use crate::{
    mvcc::Timestamp,
    record::extract::{DynKeyExtractor, KeyDyn, KeyExtractError, dyn_extractor_for_field},
    scan::{KeyRange, RangeSet},
};

/// Generic, read-only immutable memtable with a key index and arbitrary storage `S`.
pub(crate) struct ImmutableMemTable<K: Ord, S> {
    storage: S,
    index: BTreeMap<K, VersionSlice>,
    mvcc: MvccColumns,
}

impl<K: Ord, S> ImmutableMemTable<K, S> {
    pub(crate) fn new(storage: S, index: BTreeMap<K, VersionSlice>, mvcc: MvccColumns) -> Self {
        Self {
            storage,
            index,
            mvcc,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.mvcc.begin_ts.len()
    }

    pub(crate) fn storage(&self) -> &S {
        &self.storage
    }

    fn mvcc_slice(&self, slice: VersionSlice) -> (&[Timestamp], &[Timestamp], &[bool]) {
        let start = slice.start as usize;
        let end = start + slice.len as usize;
        (
            &self.mvcc.begin_ts[start..end],
            &self.mvcc.end_ts[start..end],
            &self.mvcc.tombstone[start..end],
        )
    }

    pub(crate) fn scan_ranges<'t, 's>(
        &'t self,
        ranges: &'s RangeSet<K>,
    ) -> ImmutableScan<'t, 's, K> {
        ImmutableScan::new(&self.index, ranges)
    }

    pub(crate) fn scan_visible<'t, 's>(
        &'t self,
        ranges: &'s RangeSet<K>,
        read_ts: Timestamp,
    ) -> ImmutableVisibleScan<'t, 's, K, S> {
        ImmutableVisibleScan::new(self, ranges, read_ts)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VersionSlice {
    start: u32,
    len: u32,
}

impl VersionSlice {
    pub(crate) fn new(start: u32, len: u32) -> Self {
        Self { start, len }
    }
}

/// Build a dynamic immutable segment from a batch using a provided extractor.
pub(crate) fn segment_from_batch_with_extractor(
    batch: RecordBatch,
    extractor: &dyn DynKeyExtractor,
) -> Result<ImmutableMemTable<KeyDyn, RecordBatch>, KeyExtractError> {
    extractor.validate_schema(&batch.schema())?;
    let len = batch.num_rows();
    let mut index: BTreeMap<KeyDyn, VersionSlice> = BTreeMap::new();
    for row in 0..len {
        let k = extractor.key_at(&batch, row)?;
        index.insert(k, VersionSlice::new(row as u32, 1));
    }
    let mvcc = MvccColumns::new(
        vec![Timestamp::MIN; len],
        vec![Timestamp::MAX; len],
        vec![false; len],
    );
    Ok(ImmutableMemTable::new(batch, index, mvcc))
}

/// Build a dynamic immutable segment given a key column index.
pub(crate) fn segment_from_batch_with_key_col(
    batch: RecordBatch,
    key_col: usize,
) -> Result<ImmutableMemTable<KeyDyn, RecordBatch>, KeyExtractError> {
    let schema = batch.schema();
    let fields = schema.fields();
    if key_col >= fields.len() {
        return Err(KeyExtractError::ColumnOutOfBounds(key_col, fields.len()));
    }
    let dt = fields[key_col].data_type();
    let extractor = dyn_extractor_for_field(key_col, dt)?;
    segment_from_batch_with_extractor(batch, extractor.as_ref())
}

/// Build a dynamic immutable segment given a key field name.
pub(crate) fn segment_from_batch_with_key_name(
    batch: RecordBatch,
    key_field: &str,
) -> Result<ImmutableMemTable<KeyDyn, RecordBatch>, KeyExtractError> {
    let schema = batch.schema();
    let fields = schema.fields();
    let Some((idx, _)) = fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name() == key_field)
    else {
        return Err(KeyExtractError::NoSuchField {
            name: key_field.to_string(),
        });
    };
    segment_from_batch_with_key_col(batch, idx)
}

/// Iterator for immutable scans over key ranges.
pub(crate) struct ImmutableScan<'t, 's, K: Ord> {
    index: &'t BTreeMap<K, VersionSlice>,
    ranges: &'s [KeyRange<K>],
    range_idx: usize,
    cursor: Option<BTreeRange<'t, K, VersionSlice>>,
}

impl<'t, 's, K: Ord> ImmutableScan<'t, 's, K> {
    fn new(index: &'t BTreeMap<K, VersionSlice>, ranges: &'s RangeSet<K>) -> Self {
        Self {
            index,
            ranges: ranges.as_slice(),
            range_idx: 0,
            cursor: None,
        }
    }
}

impl<'t, 's, K: Ord> Iterator for ImmutableScan<'t, 's, K> {
    type Item = (&'t K, VersionSlice);
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
                if let Some((k, slice)) = cur.next() {
                    return Some((k, *slice));
                }
                self.cursor = None;
                continue;
            }
        }
    }
}

pub(crate) struct ImmutableVisibleScan<'t, 's, K: Ord, S> {
    table: &'t ImmutableMemTable<K, S>,
    ranges: &'s [KeyRange<K>],
    range_idx: usize,
    cursor: Option<BTreeRange<'t, K, VersionSlice>>,
    read_ts: Timestamp,
}

impl<'t, 's, K: Ord, S> ImmutableVisibleScan<'t, 's, K, S> {
    fn new(
        table: &'t ImmutableMemTable<K, S>,
        ranges: &'s RangeSet<K>,
        read_ts: Timestamp,
    ) -> Self {
        Self {
            table,
            ranges: ranges.as_slice(),
            range_idx: 0,
            cursor: None,
            read_ts,
        }
    }
}

impl<'t, 's, K: Ord, S> Iterator for ImmutableVisibleScan<'t, 's, K, S> {
    type Item = (&'t K, u32);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cursor.is_none() {
                if self.range_idx >= self.ranges.len() {
                    return None;
                }
                let (start, end) = self.ranges[self.range_idx].as_borrowed_bounds();
                self.cursor = Some(self.table.index.range((start, end)));
                self.range_idx += 1;
            }

            if let Some(cur) = &mut self.cursor {
                while let Some((key, slice)) = cur.next() {
                    let start = slice.start as usize;
                    let len = slice.len as usize;
                    let (begin, end_ts, tomb) = self.table.mvcc_slice(*slice);
                    for idx in (0..len).rev() {
                        if begin[idx] <= self.read_ts && self.read_ts < end_ts[idx] && !tomb[idx] {
                            return Some((key, (start + idx) as u32));
                        }
                    }
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
    use typed_arrow_unified::SchemaLike;

    use super::*;

    #[test]
    fn scan_ranges_dynamic_key_name() {
        // Schema: id Utf8 (key), v Int32
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("c".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(3))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("ok");
        let seg = segment_from_batch_with_key_name(batch, "id").expect("seg");
        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyDyn::from("b")),
            B::Unbounded,
        )]);
        let got: Vec<String> = seg
            .scan_ranges(&ranges)
            .map(|(k, _)| match k {
                KeyDyn::Str(s) => s.as_str().to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(got, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn scan_visible_filters_by_timestamp() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("batch");
        let mut index = BTreeMap::new();
        index.insert(KeyDyn::from("k"), VersionSlice::new(0, 2));
        let mvcc = MvccColumns::new(
            vec![Timestamp::new(10), Timestamp::new(20)],
            vec![Timestamp::new(20), Timestamp::MAX],
            vec![false, false],
        );
        let seg = ImmutableMemTable::new(batch, index, mvcc);

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyDyn::from("k")),
            B::Included(KeyDyn::from("k")),
        )]);

        let mid: Vec<u32> = seg
            .scan_visible(&ranges, Timestamp::new(15))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(mid, vec![0]);

        let latest: Vec<u32> = seg
            .scan_visible(&ranges, Timestamp::new(25))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(latest, vec![1]);
    }
}
pub(crate) struct MvccColumns {
    pub begin_ts: Vec<Timestamp>,
    pub end_ts: Vec<Timestamp>,
    pub tombstone: Vec<bool>,
}

impl MvccColumns {
    pub fn new(begin_ts: Vec<Timestamp>, end_ts: Vec<Timestamp>, tombstone: Vec<bool>) -> Self {
        debug_assert_eq!(begin_ts.len(), end_ts.len());
        debug_assert_eq!(begin_ts.len(), tombstone.len());
        Self {
            begin_ts,
            end_ts,
            tombstone,
        }
    }
}
