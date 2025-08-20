#![allow(dead_code)]
use std::{
    borrow::Borrow,
    collections::{BTreeMap, btree_map::Range as BTreeRange},
};

use typed_arrow::arrow_array::RecordBatch;

use super::arrays::{ImmutableArrays, ImmutableArraysBuilder};
use crate::{
    record::{
        Record,
        extract::{DynKeyExtractor, KeyDyn, KeyExtractError, dyn_extractor_for_field},
    },
    scan::{KeyRange, RangeSet},
};

/// Generic, read-only immutable memtable with a key index and arbitrary storage `S`.
pub(crate) struct ImmutableMemTable<K: Ord, S> {
    storage: S,
    index: BTreeMap<K, u32>,
    len: usize,
}

impl<K: Ord, S> ImmutableMemTable<K, S> {
    pub(crate) fn new(storage: S, index: BTreeMap<K, u32>, len: usize) -> Self {
        Self {
            storage,
            index,
            len,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn get_offset<Q>(&self, key: &Q) -> Option<u32>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.index.get(key).copied()
    }

    pub(crate) fn scan_ranges<'t, 's>(
        &'t self,
        ranges: &'s RangeSet<K>,
    ) -> ImmutableScan<'t, 's, K> {
        ImmutableScan::new(&self.index, ranges)
    }
}

// ---------- Typed helpers ----------

/// Build a typed immutable segment from typed rows.
pub(crate) fn segment_from_rows<R, I>(
    rows: I,
) -> ImmutableMemTable<<R as Record>::Key, ImmutableArrays<R>>
where
    R: Record,
    I: IntoIterator<Item = R>,
{
    let iter = rows.into_iter();
    let (lb, ub) = iter.size_hint();
    let cap = ub.unwrap_or(lb);
    let mut builder = ImmutableArraysBuilder::<R>::new(cap);
    let mut len = 0usize;
    for row in iter {
        builder.push_row(row);
        len += 1;
    }
    let arrays = builder.finish();

    let mut index = BTreeMap::new();
    for i in 0..len {
        let key = R::key_at(&arrays.arrays, i);
        index.insert(key, i as u32);
    }
    ImmutableMemTable::new(arrays, index, len)
}

/// Build a typed immutable segment from already-built typed arrays.
pub(crate) fn segment_from_arrays<R>(
    arrays: ImmutableArrays<R>,
) -> ImmutableMemTable<<R as Record>::Key, ImmutableArrays<R>>
where
    R: Record,
{
    let len = arrays.len();
    let mut index = BTreeMap::new();
    for i in 0..len {
        let key = R::key_at(&arrays.arrays, i);
        index.insert(key, i as u32);
    }
    ImmutableMemTable::new(arrays, index, len)
}

// ---------- Dynamic helpers ----------

/// Build a dynamic immutable segment from a batch using a provided extractor.
pub(crate) fn segment_from_batch_with_extractor(
    batch: RecordBatch,
    extractor: &dyn DynKeyExtractor,
) -> Result<ImmutableMemTable<KeyDyn, RecordBatch>, KeyExtractError> {
    extractor.validate_schema(&batch.schema())?;
    let len = batch.num_rows();
    let mut index = BTreeMap::new();
    for row in 0..len {
        let k = extractor.key_at(&batch, row)?;
        index.insert(k, row as u32);
    }
    Ok(ImmutableMemTable::new(batch, index, len))
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
    index: &'t BTreeMap<K, u32>,
    ranges: &'s [KeyRange<K>],
    range_idx: usize,
    cursor: Option<BTreeRange<'t, K, u32>>,
}

impl<'t, 's, K: Ord> ImmutableScan<'t, 's, K> {
    fn new(index: &'t BTreeMap<K, u32>, ranges: &'s RangeSet<K>) -> Self {
        Self {
            index,
            ranges: ranges.as_slice(),
            range_idx: 0,
            cursor: None,
        }
    }
}

impl<'t, 's, K: Ord> Iterator for ImmutableScan<'t, 's, K> {
    type Item = (&'t K, u32);
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
                if let Some((k, off)) = cur.next() {
                    return Some((k, *off));
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

    #[derive(typed_arrow::Record)]
    #[record(field_macro = crate::key_field)]
    struct R {
        #[record(ext(key))]
        k: i32,
        v: i32,
    }

    #[test]
    fn scan_ranges_typed_inclusive_exclusive() {
        // Create a small segment with out-of-order keys
        let seg = segment_from_rows::<R, _>(vec![
            R { k: 3, v: 1 },
            R { k: 1, v: 2 },
            R { k: 5, v: 3 },
            R { k: 7, v: 4 },
        ]);
        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(B::Included(3), B::Excluded(7))]);
        let got: Vec<(i32, u32)> = seg.scan_ranges(&ranges).map(|(k, off)| (*k, off)).collect();
        assert_eq!(got, vec![(3, 0), (5, 2)]);
    }

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
}
