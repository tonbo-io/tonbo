use std::collections::{
    BTreeMap,
    btree_map::{Iter as BTreeIter, Range as BTreeRange},
};

use arrow_array::RecordBatch;

use crate::{
    mvcc::Timestamp,
    record::extract::{DynKeyExtractor, KeyDyn, KeyExtractError, dyn_extractor_for_field},
    scan::{KeyRange, RangeSet},
};

pub(crate) const MVCC_COMMIT_COL: &str = "_commit_ts";
pub(crate) const MVCC_TOMBSTONE_COL: &str = "_tombstone";

/// Generic, read-only immutable memtable with a key index and arbitrary storage `S`.
pub(crate) struct ImmutableMemTable<K: Ord, S> {
    _storage: S,
    index: BTreeMap<K, VersionSlice>,
    mvcc: MvccColumns,
}

impl<K: Ord, S> ImmutableMemTable<K, S> {
    pub(crate) fn new(storage: S, index: BTreeMap<K, VersionSlice>, mvcc: MvccColumns) -> Self {
        Self {
            _storage: storage,
            index,
            mvcc,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.mvcc.commit_ts.len()
    }

    #[allow(unused)]
    pub(crate) fn storage(&self) -> &S {
        &self._storage
    }

    pub(crate) fn row_iter(&self) -> ImmutableRowIter<'_, K, S> {
        ImmutableRowIter::new(self)
    }

    pub(crate) fn min_key(&self) -> Option<&K> {
        self.index.keys().next()
    }

    pub(crate) fn max_key(&self) -> Option<&K> {
        self.index.keys().next_back()
    }

    #[allow(unused)]
    pub(crate) fn mvcc_columns(&self) -> &MvccColumns {
        &self.mvcc
    }

    fn mvcc_slice(&self, slice: VersionSlice) -> (&[Timestamp], &[bool]) {
        let start = slice.start as usize;
        let end = start + slice.len as usize;
        (
            &self.mvcc.commit_ts[start..end],
            &self.mvcc.tombstone[start..end],
        )
    }

    #[allow(unused)]
    pub(crate) fn scan_ranges<'t, 's>(
        &'t self,
        ranges: &'s RangeSet<K>,
    ) -> ImmutableScan<'t, 's, K> {
        ImmutableScan::new(&self.index, ranges)
    }

    #[allow(unused)]
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
#[allow(unused)]
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
    let commit_ts = vec![Timestamp::MIN; len];
    let tombstone = vec![false; len];
    let (batch, mvcc) =
        bundle_mvcc_sidecar(batch, commit_ts, tombstone).map_err(KeyExtractError::from)?;
    Ok(ImmutableMemTable::new(batch, index, mvcc))
}

/// Build a dynamic immutable segment given a key column index.
#[allow(unused)]
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
#[allow(unused)]
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

pub(crate) fn bundle_mvcc_sidecar(
    batch: RecordBatch,
    commit_ts: Vec<Timestamp>,
    tombstone: Vec<bool>,
) -> Result<(RecordBatch, MvccColumns), arrow_schema::ArrowError> {
    use arrow_schema::ArrowError;

    if commit_ts.len() != tombstone.len() {
        return Err(ArrowError::ComputeError(
            "commit_ts and tombstone length mismatch".to_string(),
        ));
    }
    if commit_ts.len() != batch.num_rows() {
        return Err(ArrowError::ComputeError(
            "mvcc metadata length mismatch record batch".to_string(),
        ));
    }

    let mvcc = MvccColumns::new(commit_ts, tombstone);
    Ok((batch, mvcc))
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
                for (key, slice) in cur.by_ref() {
                    let start = slice.start as usize;
                    let len = slice.len as usize;
                    let (commit_ts, tomb) = self.table.mvcc_slice(*slice);
                    for idx in 0..len {
                        if commit_ts[idx] <= self.read_ts && !tomb[idx] {
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

pub(crate) struct ImmutableRowIter<'t, K: Ord, S> {
    table: &'t ImmutableMemTable<K, S>,
    iter: BTreeIter<'t, K, VersionSlice>,
    current: Option<ImmutableSliceCursor<'t, K>>,
}

struct ImmutableSliceCursor<'t, K> {
    key: &'t K,
    slice: VersionSlice,
    commit_ts: &'t [Timestamp],
    tombstone: &'t [bool],
    offset: usize,
}

pub(crate) struct ImmutableRowEntry<'t, K> {
    pub _key: &'t K,
    pub _row: u32,
    pub commit_ts: Timestamp,
    pub tombstone: bool,
}

impl<'t, K: Ord, S> ImmutableRowIter<'t, K, S> {
    fn new(table: &'t ImmutableMemTable<K, S>) -> Self {
        Self {
            table,
            iter: table.index.iter(),
            current: None,
        }
    }

    fn advance(&mut self) -> Option<&mut ImmutableSliceCursor<'t, K>> {
        if let Some(cursor) = &mut self.current
            && cursor.offset < cursor.commit_ts.len()
        {
            return self.current.as_mut();
        }
        let (key, slice) = self.iter.next()?;
        let (commit_ts, tombstone) = self.table.mvcc_slice(*slice);
        self.current = Some(ImmutableSliceCursor {
            key,
            slice: *slice,
            commit_ts,
            tombstone,
            offset: 0,
        });
        self.current.as_mut()
    }
}

impl<'t, K: Ord, S> Iterator for ImmutableRowIter<'t, K, S> {
    type Item = ImmutableRowEntry<'t, K>;

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = self.advance()?;
        let idx = cursor.offset;
        cursor.offset += 1;
        Some(ImmutableRowEntry {
            _key: cursor.key,
            _row: cursor.slice.start + idx as u32,
            commit_ts: cursor.commit_ts[idx],
            tombstone: cursor.tombstone[idx],
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::test_util::build_batch;

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
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("ok");
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
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(4))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(3))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let mut index = BTreeMap::new();
        index.insert(KeyDyn::from("k"), VersionSlice::new(0, 4));
        let (batch, mvcc) = bundle_mvcc_sidecar(
            batch,
            vec![
                Timestamp::new(40),
                Timestamp::new(30),
                Timestamp::new(20),
                Timestamp::new(10),
            ],
            vec![false, false, false, false],
        )
        .expect("mvcc columns");
        let seg = ImmutableMemTable::new(batch, index, mvcc);

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyDyn::from("k")),
            B::Included(KeyDyn::from("k")),
        )]);

        let first_visible: Vec<u32> = seg
            .scan_visible(&ranges, Timestamp::new(15))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(first_visible, vec![3]);

        let latest: Vec<u32> = seg
            .scan_visible(&ranges, Timestamp::new(45))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(latest, vec![0]);

        let batch = seg.storage();
        let value_at = |idx: u32| match &crate::record::extract::row_from_batch(batch, idx as usize)
            .expect("row")
            .0[1]
        {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };

        assert_eq!(value_at(first_visible[0]), 1);
        assert_eq!(value_at(latest[0]), 4);
    }

    #[test]
    fn scan_visible_skips_tombstones() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // Newest → oldest rows to align with commit timestamp ordering.
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(3))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let mut index = BTreeMap::new();
        index.insert(KeyDyn::from("k"), VersionSlice::new(0, 3));
        let (batch, mvcc) = bundle_mvcc_sidecar(
            batch,
            vec![Timestamp::new(30), Timestamp::new(20), Timestamp::new(10)],
            vec![false, true, false],
        )
        .expect("mvcc columns");
        let seg = ImmutableMemTable::new(batch, index, mvcc);
        let schema = seg.storage().schema();
        assert!(
            !schema
                .fields()
                .iter()
                .any(|f| f.name() == MVCC_COMMIT_COL || f.name() == MVCC_TOMBSTONE_COL),
            "unexpected _commit_ts column in immutable storage"
        );

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyDyn::from("k")),
            B::Included(KeyDyn::from("k")),
        )]);

        // Timestamp past the tombstoned version should return the next older live row.
        let visible: Vec<u32> = seg
            .scan_visible(&ranges, Timestamp::new(21))
            .map(|(_, row)| row)
            .collect();
        assert_eq!(visible, vec![2]);

        let batch = seg.storage();
        let value = match &crate::record::extract::row_from_batch(batch, visible[0] as usize)
            .expect("row")
            .0[1]
        {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };
        assert_eq!(value, 1);
    }

    #[test]
    fn row_iter_exposes_mvcc_and_bounds() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // rows stored newest → oldest
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(10))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(8))]),
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(9))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let mut index = BTreeMap::new();
        index.insert(KeyDyn::from("a"), VersionSlice::new(0, 2));
        index.insert(KeyDyn::from("b"), VersionSlice::new(2, 1));
        let (batch, mvcc) = bundle_mvcc_sidecar(
            batch,
            vec![Timestamp::new(30), Timestamp::new(20), Timestamp::new(10)],
            vec![false, true, false],
        )
        .expect("mvcc columns");
        let seg = ImmutableMemTable::new(batch, index, mvcc);

        let got: Vec<(String, u32, u64, bool)> = seg
            .row_iter()
            .map(|entry| {
                let key = match entry._key {
                    KeyDyn::Str(s) => s.as_str().to_string(),
                    _ => unreachable!("unexpected key type"),
                };
                (key, entry._row, entry.commit_ts.get(), entry.tombstone)
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), 0, 30, false),
                ("a".to_string(), 1, 20, true),
                ("b".to_string(), 2, 10, false)
            ]
        );

        let min_key = seg.min_key().and_then(|k| match k {
            KeyDyn::Str(s) => Some(s.as_str()),
            _ => None,
        });
        let max_key = seg.max_key().and_then(|k| match k {
            KeyDyn::Str(s) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(min_key, Some("a"));
        assert_eq!(max_key, Some("b"));
        assert_eq!(seg.len(), 3);
    }
}
#[derive(Debug)]
pub(crate) struct MvccColumns {
    pub commit_ts: Vec<Timestamp>,
    pub tombstone: Vec<bool>,
}

impl MvccColumns {
    pub fn new(commit_ts: Vec<Timestamp>, tombstone: Vec<bool>) -> Self {
        debug_assert_eq!(commit_ts.len(), tombstone.len());
        Self {
            commit_ts,
            tombstone,
        }
    }
}
