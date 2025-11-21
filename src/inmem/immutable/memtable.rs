use std::{collections::BTreeMap, fmt, marker::PhantomData, ops::Bound, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Schema, SchemaRef};
use typed_arrow_dyn::{DynProjection, DynRowRaw, DynSchema, DynViewError};

pub(crate) use crate::mvcc::MVCC_COMMIT_COL;
use crate::{
    extractor::{KeyExtractError, KeyProjection, map_view_err, projection_for_field},
    key::{KeyOwned, KeyRange, KeyRow, KeyTsViewRaw, RangeSet},
    mvcc::Timestamp,
};
pub(crate) const MVCC_TOMBSTONE_COL: &str = "_tombstone";

/// Read-only immutable memtable backed by Arrow storage and MVCC metadata.
#[derive(Debug)]
pub(crate) struct ImmutableMemTable<S> {
    storage: S,
    index: BTreeMap<KeyTsViewRaw, ImmutableIndexEntry>,
    mvcc: MvccColumns,
    deletes: DeleteSidecar,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ImmutableIndexEntry {
    Row(u32),
    Delete,
}

#[derive(Clone, Debug)]
pub(crate) struct DeleteSidecar {
    keys: RecordBatch,
    commit_ts: Vec<Timestamp>,
}

impl DeleteSidecar {
    pub fn new(keys: RecordBatch, commit_ts: Vec<Timestamp>) -> Self {
        debug_assert_eq!(keys.num_rows(), commit_ts.len());
        Self { keys, commit_ts }
    }

    pub fn empty(schema: &SchemaRef) -> Self {
        Self {
            keys: RecordBatch::new_empty(schema.clone()),
            commit_ts: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.commit_ts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commit_ts.is_empty()
    }

    #[allow(dead_code)]
    pub fn commit_ts(&self, idx: usize) -> Timestamp {
        self.commit_ts[idx]
    }

    #[allow(dead_code)]
    pub fn key_batch(&self) -> &RecordBatch {
        &self.keys
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch, arrow_schema::ArrowError> {
        use arrow_schema::{Field, Schema};

        let mut columns: Vec<ArrayRef> = self.keys.columns().to_vec();
        let commit_array = UInt64Array::from_iter_values(self.commit_ts.iter().map(|ts| ts.get()));
        columns.push(Arc::new(commit_array) as ArrayRef);
        let mut fields: Vec<Field> = self
            .keys
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(
            MVCC_COMMIT_COL,
            arrow_schema::DataType::UInt64,
            false,
        ));
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
    }
}

impl<S> ImmutableMemTable<S> {
    pub(crate) fn new(
        storage: S,
        index: BTreeMap<KeyTsViewRaw, ImmutableIndexEntry>,
        mvcc: MvccColumns,
        deletes: DeleteSidecar,
    ) -> Self {
        Self {
            storage,
            index,
            mvcc,
            deletes,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.index.len()
    }

    #[allow(unused)]
    pub(crate) fn storage(&self) -> &S {
        &self.storage
    }

    pub(crate) fn delete_sidecar(&self) -> &DeleteSidecar {
        &self.deletes
    }

    /// Return `true` if a committed version newer than `snapshot_ts` exists for `key`.
    pub(crate) fn has_conflict(&self, key: &KeyOwned, snapshot_ts: Timestamp) -> bool {
        let upper = KeyTsViewRaw::from_owned(key, Timestamp::MAX);
        let lower = KeyTsViewRaw::from_owned(key, Timestamp::MIN);
        self.index
            .range(upper..=lower)
            .any(|(view, _)| view.timestamp() > snapshot_ts)
    }

    pub(crate) fn row_iter(&self) -> ImmutableRowIter<'_, S> {
        ImmutableRowIter::new(self)
    }

    pub(crate) fn min_key(&self) -> Option<KeyOwned> {
        self.index.keys().next().map(|view| view.key().to_owned())
    }

    pub(crate) fn max_key(&self) -> Option<KeyOwned> {
        self.index
            .keys()
            .next_back()
            .map(|view| view.key().to_owned())
    }

    #[allow(unused)]
    pub(crate) fn mvcc_columns(&self) -> &MvccColumns {
        &self.mvcc
    }

    fn mvcc_row(&self, row: u32) -> (Timestamp, bool) {
        let idx = row as usize;
        (self.mvcc.commit_ts[idx], self.mvcc.tombstone[idx])
    }

    #[allow(unused)]
    pub(crate) fn scan_visible<'t>(
        &'t self,
        ranges: &RangeSet<KeyOwned>,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<ImmutableVisibleScan<'t, S>, KeyExtractError>
    where
        S: RecordBatchStorage,
    {
        ImmutableVisibleScan::new(self, ranges, projection_schema, read_ts)
    }
}

/// Build a dynamic immutable segment from a batch using a provided extractor.
#[allow(unused)]
pub(crate) fn segment_from_batch_with_extractor(
    batch: RecordBatch,
    extractor: &dyn KeyProjection,
) -> Result<ImmutableMemTable<RecordBatch>, KeyExtractError> {
    extractor.validate_schema(&batch.schema())?;
    let len = batch.num_rows();
    let commit_ts = vec![Timestamp::MIN; len];
    let tombstone = vec![false; len];
    let (batch, mvcc) =
        bundle_mvcc_sidecar(batch, commit_ts, tombstone).map_err(KeyExtractError::from)?;

    let mut index: BTreeMap<KeyTsViewRaw, ImmutableIndexEntry> = BTreeMap::new();
    let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
    let key_rows = extractor.project_view(&batch, &row_indices)?;
    for (row, key_row) in key_rows.into_iter().enumerate() {
        index.insert(
            KeyTsViewRaw::new(key_row, mvcc.commit_ts[row]),
            ImmutableIndexEntry::Row(row as u32),
        );
    }

    Ok(ImmutableMemTable::new(
        batch,
        index,
        mvcc,
        DeleteSidecar::empty(&extractor.key_schema()),
    ))
}

/// Build a dynamic immutable segment given a key column index.
#[allow(unused)]
pub(crate) fn segment_from_batch_with_key_col(
    batch: RecordBatch,
    key_col: usize,
) -> Result<ImmutableMemTable<RecordBatch>, KeyExtractError> {
    let schema = batch.schema();
    let fields = schema.fields();
    if key_col >= fields.len() {
        return Err(KeyExtractError::ColumnOutOfBounds(key_col, fields.len()));
    }
    let extractor = projection_for_field(schema.clone(), key_col)?;
    segment_from_batch_with_extractor(batch, extractor.as_ref())
}

/// Build a dynamic immutable segment given a key field name.
#[allow(unused)]
pub(crate) fn segment_from_batch_with_key_name(
    batch: RecordBatch,
    key_field: &str,
) -> Result<ImmutableMemTable<RecordBatch>, KeyExtractError> {
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

pub(crate) struct ImmutableVisibleScan<'t, S> {
    table: &'t ImmutableMemTable<S>,
    converted: ConvertedRanges,
    range_idx: usize,
    cursor: Option<std::collections::btree_map::Range<'t, KeyTsViewRaw, ImmutableIndexEntry>>,
    read_ts: Timestamp,
    current_key: Option<KeyRow>,
    emitted_for_key: bool,
    dyn_schema: DynSchema,
    projection: DynProjection,
}

pub(crate) enum ImmutableVisibleEntry<'t> {
    Row(&'t KeyTsViewRaw, DynRowRaw),
    Tombstone(&'t KeyTsViewRaw),
}

impl<'t> ImmutableVisibleEntry<'t> {
    #[allow(dead_code)]
    pub(crate) fn into_row(self) -> Option<(&'t KeyTsViewRaw, DynRowRaw)> {
        match self {
            ImmutableVisibleEntry::Row(key, row) => Some((key, row)),
            ImmutableVisibleEntry::Tombstone(_) => None,
        }
    }
}

impl<'t, S> fmt::Debug for ImmutableVisibleScan<'t, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImmutableVisibleScan")
            .field("range_idx", &self.range_idx)
            .field("read_ts", &self.read_ts)
            .field("emitted_for_key", &self.emitted_for_key)
            .finish()
    }
}

impl<'t, S> ImmutableVisibleScan<'t, S>
where
    S: RecordBatchStorage,
{
    fn new(
        table: &'t ImmutableMemTable<S>,
        ranges: &RangeSet<KeyOwned>,
        projection_schema: Option<SchemaRef>,
        read_ts: Timestamp,
    ) -> Result<Self, KeyExtractError> {
        let base_schema = table.storage.as_record_batch().schema();
        let dyn_schema = DynSchema::from_ref(base_schema.clone());
        let projection = build_projection(&base_schema, projection_schema.as_ref())?;
        Ok(Self {
            table,
            converted: convert_ranges(ranges),
            range_idx: 0,
            cursor: None,
            read_ts,
            current_key: None,
            emitted_for_key: false,
            dyn_schema,
            projection,
        })
    }
}

impl<'t, S> Iterator for ImmutableVisibleScan<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<ImmutableVisibleEntry<'t>, DynViewError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cursor.is_none() {
                if self.range_idx >= self.converted.ranges().as_slice().len() {
                    return None;
                }
                let (start, end) =
                    self.converted.ranges().as_slice()[self.range_idx].as_borrowed_bounds();
                self.cursor = Some(self.table.index.range((start, end)));
                self.range_idx += 1;
                self.current_key = None;
                self.emitted_for_key = false;
            }
            if let Some(cur) = &mut self.cursor {
                for (view, entry) in cur.by_ref() {
                    let key_view = view.key();
                    if self
                        .current_key
                        .as_ref()
                        .map(|existing| existing == key_view)
                        .unwrap_or(false)
                    {
                        if self.emitted_for_key {
                            continue;
                        }
                    } else {
                        self.current_key = Some(key_view.clone());
                        self.emitted_for_key = false;
                    }

                    let entry_commit = view.timestamp();
                    if entry_commit > self.read_ts {
                        continue;
                    }

                    match entry {
                        ImmutableIndexEntry::Delete => {
                            self.emitted_for_key = true;
                            return Some(Ok(ImmutableVisibleEntry::Tombstone(view)));
                        }
                        ImmutableIndexEntry::Row(row_idx) => {
                            let (commit_ts, tombstone) = self.table.mvcc_row(*row_idx);
                            if commit_ts > self.read_ts {
                                continue;
                            }
                            if tombstone {
                                self.emitted_for_key = true;
                                return Some(Ok(ImmutableVisibleEntry::Tombstone(view)));
                            }
                            let batch = self.table.storage.as_record_batch();
                            let row_idx = *row_idx as usize;
                            let row = match self.projection.project_row_raw(
                                &self.dyn_schema,
                                batch,
                                row_idx,
                            ) {
                                Ok(row) => row,
                                Err(err) => return Some(Err(err)),
                            };
                            self.emitted_for_key = true;
                            return Some(Ok(ImmutableVisibleEntry::Row(view, row)));
                        }
                    }
                }
                self.cursor = None;
                continue;
            }
        }
    }
}

#[derive(Debug)]
struct ConvertedRanges {
    _key_backing: Vec<KeyOwned>,
    ranges: RangeSet<KeyTsViewRaw>,
}

impl ConvertedRanges {
    fn new(key_backing: Vec<KeyOwned>, ranges: RangeSet<KeyTsViewRaw>) -> Self {
        Self {
            _key_backing: key_backing,
            ranges,
        }
    }

    fn ranges(&self) -> &RangeSet<KeyTsViewRaw> {
        &self.ranges
    }
}

fn convert_ranges(ranges: &RangeSet<KeyOwned>) -> ConvertedRanges {
    let mut key_backing: Vec<KeyOwned> = Vec::new();
    let converted = ranges
        .as_slice()
        .iter()
        .map(|range| {
            KeyRange::new(
                convert_lower_bound(&range.start, &mut key_backing),
                convert_upper_bound(&range.end, &mut key_backing),
            )
        })
        .collect();
    ConvertedRanges::new(key_backing, RangeSet::from_ranges(converted))
}

fn push_backing_key(storage: &mut Vec<KeyOwned>, key: KeyOwned) -> &KeyOwned {
    storage.push(key);
    storage
        .last()
        .expect("backing storage should contain the pushed key")
}

fn convert_lower_bound(
    bound: &Bound<KeyOwned>,
    storage: &mut Vec<KeyOwned>,
) -> Bound<KeyTsViewRaw> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => {
            let owned = push_backing_key(storage, key.clone());
            Bound::Included(KeyTsViewRaw::from_owned(owned, Timestamp::MAX))
        }
        Bound::Excluded(key) => {
            let owned = push_backing_key(storage, key.clone());
            Bound::Excluded(KeyTsViewRaw::from_owned(owned, Timestamp::MIN))
        }
    }
}

fn convert_upper_bound(
    bound: &Bound<KeyOwned>,
    storage: &mut Vec<KeyOwned>,
) -> Bound<KeyTsViewRaw> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => {
            let owned = push_backing_key(storage, key.clone());
            Bound::Included(KeyTsViewRaw::from_owned(owned, Timestamp::MIN))
        }
        Bound::Excluded(key) => {
            let owned = push_backing_key(storage, key.clone());
            Bound::Excluded(KeyTsViewRaw::from_owned(owned, Timestamp::MAX))
        }
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
        return DynProjection::from_schema(schema.as_ref(), projected.as_ref())
            .map_err(map_view_err);
    }

    let logical_fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| {
            let name = field.name();
            name != MVCC_COMMIT_COL && name != MVCC_TOMBSTONE_COL
        })
        .map(|field| field.as_ref().clone())
        .collect();

    if logical_fields.is_empty() {
        return Err(KeyExtractError::Arrow(
            arrow_schema::ArrowError::ComputeError(
                "projection requires at least one column".to_string(),
            ),
        ));
    }

    let logical_schema = SchemaRef::new(Schema::new(logical_fields));
    DynProjection::from_schema(schema.as_ref(), logical_schema.as_ref()).map_err(map_view_err)
}

pub(crate) struct ImmutableRowIter<'t, S> {
    iter: std::vec::IntoIter<ImmutableRowEntry>,
    _marker: PhantomData<&'t S>,
}

pub(crate) struct ImmutableRowEntry {
    pub key: KeyOwned,
    pub commit_ts: Timestamp,
    pub tombstone: bool,
}

impl<'t, S> ImmutableRowIter<'t, S> {
    fn new(table: &'t ImmutableMemTable<S>) -> Self {
        let mut rows: Vec<ImmutableRowEntry> = table
            .index
            .iter()
            .map(|(view, entry)| ImmutableRowEntry {
                key: view.key().to_owned(),
                commit_ts: view.timestamp(),
                tombstone: matches!(entry, ImmutableIndexEntry::Delete),
            })
            .collect();
        rows.sort_by(|a, b| match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => b.commit_ts.get().cmp(&a.commit_ts.get()),
            other => other,
        });
        Self {
            iter: rows.into_iter(),
            _marker: PhantomData,
        }
    }
}

impl<'t, S> Iterator for ImmutableRowIter<'t, S> {
    type Item = ImmutableRowEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Debug, Clone)]
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

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::{projection_for_columns, projection_for_field},
        inmem::mutable::memtable::DynMem,
        test_util::build_batch,
    };

    fn push_view(storage: &mut Vec<KeyOwned>, key: &str, ts: Timestamp) -> KeyTsViewRaw {
        let owned = push_backing_key(storage, KeyOwned::from(key));
        KeyTsViewRaw::from_owned(owned, ts)
    }

    #[test]
    fn scan_ranges_dynamic_key_name() {
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
            B::Included(KeyOwned::from("b")),
            B::Unbounded,
        )]);
        let got: Vec<String> = seg
            .scan_visible(&ranges, None, Timestamp::MAX)
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry.into_row().map(|(view, _)| view.key().to_owned())
            })
            .map(|k| k.as_utf8().expect("utf8 key").to_string())
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
        let mut key_storage = Vec::new();
        let mut composite = BTreeMap::new();
        let commits = [40u64, 30, 20, 10];
        for (row, ts) in commits.into_iter().enumerate() {
            let view = push_view(&mut key_storage, "k", Timestamp::new(ts));
            composite.insert(view, ImmutableIndexEntry::Row(row as u32));
        }
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
        let delete_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let delete_sidecar = DeleteSidecar::empty(&delete_schema);
        let seg = ImmutableMemTable::new(batch, composite, mvcc, delete_sidecar);

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyOwned::from("k")),
            B::Included(KeyOwned::from("k")),
        )]);
        let first_visible: Vec<_> = seg
            .scan_visible(&ranges, None, Timestamp::new(15))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(first_visible.len(), 1);

        let latest: Vec<_> = seg
            .scan_visible(&ranges, None, Timestamp::new(45))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(latest.len(), 1);

        let value_after_first = match first_visible[0].0[1].as_ref() {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };
        let value_latest = match latest[0].0[1].as_ref() {
            Some(DynCell::I32(v)) => *v,
            _ => panic!("unexpected cell"),
        };

        assert_eq!(value_after_first, 1);
        assert_eq!(value_latest, 4);
    }

    #[test]
    fn scan_visible_skips_tombstones() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(3))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(2))]),
            DynRow(vec![Some(DynCell::Str("k".into())), Some(DynCell::I32(1))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let mut key_storage = Vec::new();
        let mut composite = BTreeMap::new();
        for (row, (ts, tombstone)) in [(30u64, false), (20, true), (10, false)]
            .into_iter()
            .enumerate()
        {
            let view = push_view(&mut key_storage, "k", Timestamp::new(ts));
            let entry = if tombstone {
                ImmutableIndexEntry::Delete
            } else {
                ImmutableIndexEntry::Row(row as u32)
            };
            composite.insert(view, entry);
        }
        let (batch, mvcc) = bundle_mvcc_sidecar(
            batch,
            vec![Timestamp::new(30), Timestamp::new(20), Timestamp::new(10)],
            vec![false, false, false],
        )
        .expect("mvcc columns");
        let delete_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let delete_rows = vec![DynRow(vec![Some(DynCell::Str("k".into()))])];
        let delete_batch: RecordBatch = build_batch(delete_schema, delete_rows).expect("delete");
        let delete_sidecar = DeleteSidecar::new(delete_batch, vec![Timestamp::new(20)]);
        let seg = ImmutableMemTable::new(batch, composite, mvcc, delete_sidecar);
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
            B::Included(KeyOwned::from("k")),
            B::Included(KeyOwned::from("k")),
        )]);
        let visible: Vec<_> = seg
            .scan_visible(&ranges, None, Timestamp::new(21))
            .expect("scan visible")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(
            visible.len(),
            0,
            "tombstoned rows should be hidden once delete is visible"
        );
    }

    #[test]
    fn scan_visible_handles_delete_then_reinsert() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let mut mutable = DynMem::new(schema.clone());
        let extractor =
            projection_for_field(schema.clone(), 0).expect("mutable delete test extractor");

        let insert_value = |mem: &mut DynMem, value: i32, ts: u64| {
            let rows = vec![DynRow(vec![
                Some(DynCell::Str("k".into())),
                Some(DynCell::I32(value)),
            ])];
            let batch: RecordBatch = build_batch(schema.clone(), rows).expect("insert batch");
            mem.insert_batch(extractor.as_ref(), batch, Timestamp::new(ts))
                .expect("insert value");
        };

        insert_value(&mut mutable, 1, 10);

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
        let delete_batch: RecordBatch =
            build_batch(delete_schema.clone(), delete_rows).expect("delete batch");
        mutable
            .insert_delete_batch(delete_projection.as_ref(), delete_batch)
            .expect("delete");

        insert_value(&mut mutable, 3, 30);

        let segment = mutable
            .seal_into_immutable(&schema, extractor.as_ref())
            .expect("seal immutable")
            .expect("segment");

        use std::ops::Bound as B;
        let ranges = RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(KeyOwned::from("k")),
            B::Included(KeyOwned::from("k")),
        )]);

        let rows_after_delete: Vec<DynRow> = segment
            .scan_visible(&ranges, None, Timestamp::new(25))
            .expect("scan after delete")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert!(
            rows_after_delete.is_empty(),
            "rows at or below delete_ts should be hidden"
        );

        let rows_after_reinsert: Vec<DynRow> = segment
            .scan_visible(&ranges, None, Timestamp::MAX)
            .expect("scan after reinsert")
            .filter_map(|res| {
                let entry = res.expect("row projection");
                entry
                    .into_row()
                    .map(|(_, row)| row.into_owned().expect("row"))
            })
            .collect();
        assert_eq!(rows_after_reinsert.len(), 1);
        match rows_after_reinsert[0].0[1].as_ref() {
            Some(DynCell::I32(v)) => assert_eq!(*v, 3),
            other => panic!("unexpected cell {other:?}"),
        }
    }

    #[test]
    fn default_projection_omits_mvcc_columns() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
            Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
        ]));
        let rows = vec![DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
            Some(DynCell::U64(10)),
            Some(DynCell::Bool(false)),
        ])];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let mut key_storage = Vec::new();
        let view = push_view(&mut key_storage, "k", Timestamp::new(10));
        let mut composite = BTreeMap::new();
        composite.insert(view, ImmutableIndexEntry::Row(0));
        let mvcc = MvccColumns::new(vec![Timestamp::new(10)], vec![false]);
        let key_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let delete_sidecar = DeleteSidecar::empty(&key_schema);
        let seg = ImmutableMemTable::new(batch, composite, mvcc, delete_sidecar);

        let ranges = RangeSet::<KeyOwned>::all();
        let mut scan = seg
            .scan_visible(&ranges, None, Timestamp::MAX)
            .expect("scan visible");
        let entry = scan.next().expect("row present").expect("row projection");
        let row = match entry {
            ImmutableVisibleEntry::Row(_, row) => row,
            ImmutableVisibleEntry::Tombstone(_) => panic!("expected row entry"),
        };
        let owned = row.into_owned().expect("row owned");
        assert_eq!(
            owned.0.len(),
            2,
            "mvcc columns leaked into default projection"
        );
    }

    #[test]
    fn row_iter_exposes_mvcc_and_bounds() {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(10))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(8))]),
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(9))]),
        ];
        let batch: RecordBatch = build_batch(schema.clone(), rows).expect("batch");
        let mut key_storage = Vec::new();
        let mut composite = BTreeMap::new();
        for (row, (key, ts)) in [("a", 30u64), ("b", 20), ("a", 10)].into_iter().enumerate() {
            let view = push_view(&mut key_storage, key, Timestamp::new(ts));
            let entry = if key == "b" {
                ImmutableIndexEntry::Delete
            } else {
                ImmutableIndexEntry::Row(row as u32)
            };
            composite.insert(view, entry);
        }
        let (batch, mvcc) = bundle_mvcc_sidecar(
            batch,
            vec![Timestamp::new(30), Timestamp::new(20), Timestamp::new(10)],
            vec![false, false, false],
        )
        .expect("mvcc columns");

        let delete_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let delete_rows = vec![DynRow(vec![Some(DynCell::Str("b".into()))])];
        let delete_batch: RecordBatch = build_batch(delete_schema, delete_rows).expect("delete");
        let delete_sidecar = DeleteSidecar::new(delete_batch, vec![Timestamp::new(20)]);
        let seg = ImmutableMemTable::new(batch, composite, mvcc, delete_sidecar);

        let got: Vec<(String, u64, bool)> = seg
            .row_iter()
            .map(|entry| {
                let key = entry.key.as_utf8().expect("utf8 key").to_string();
                (key, entry.commit_ts.get(), entry.tombstone)
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), 30, false),
                ("a".to_string(), 10, false),
                ("b".to_string(), 20, true),
            ]
        );

        let min_key = seg
            .min_key()
            .map(|k| k.as_utf8().expect("utf8 key").to_string());
        let max_key = seg
            .max_key()
            .map(|k| k.as_utf8().expect("utf8 key").to_string());
        assert_eq!(min_key.as_deref(), Some("a"));
        assert_eq!(max_key.as_deref(), Some("b"));
        assert_eq!(seg.len(), 3);
    }
}
pub(crate) trait RecordBatchStorage {
    fn as_record_batch(&self) -> &RecordBatch;
}

impl RecordBatchStorage for RecordBatch {
    fn as_record_batch(&self) -> &RecordBatch {
        self
    }
}
