//! Adapters and helpers for integrating typed-arrow generated schemas/rows
//! with Tonbo's database semantics (sentinels, PK metadata, sorting).

use std::{marker::PhantomData, mem::transmute, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};
#[allow(unused_imports)]
use typed_arrow::schema::{BuildRows as _BuildRows, RowBuilder, SchemaMeta as _SchemaMeta};

use crate::{
    magic,
    record::{ArrowArrays, ArrowArraysBuilder, Key, KeyRef, Record, RecordRef, Schema},
};

/// Helper for composing a Tonbo Arrow schema from user fields.
///
/// This injects Tonbo sentinels (`_null`, `_ts`) in front of user columns.
pub fn compose_arrow_schema_with_sentinels(user_fields: &[Field]) -> Arc<ArrowSchema> {
    let mut fields = Vec::with_capacity(user_fields.len() + magic::USER_COLUMN_OFFSET);
    fields.push(Field::new(magic::NULL, DataType::Boolean, false));
    fields.push(Field::new(magic::TS, DataType::UInt32, false));
    fields.extend(user_fields.iter().cloned());
    Arc::new(ArrowSchema::new(fields))
}

/// Compose Arrow schema and attach metadata computed from `user_fields` and `pk_user_indices`.
fn compose_with_metadata(
    user_fields: &[Field],
    pk_user_indices: &[usize],
    base_meta: &std::collections::HashMap<String, String>,
) -> Arc<ArrowSchema> {
    use std::collections::HashMap;
    let mut fields = Vec::with_capacity(user_fields.len() + magic::USER_COLUMN_OFFSET);
    fields.push(Field::new(magic::NULL, DataType::Boolean, false));
    fields.push(Field::new(magic::TS, DataType::UInt32, false));
    fields.extend(user_fields.iter().cloned());

    let mut meta: HashMap<String, String> = base_meta.clone();
    // Store user and full PK indices, plus sorting columns as a compact CSV.
    let user_idx_csv = pk_user_indices
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    meta.insert(
        crate::typed::meta::PK_USER_INDICES.to_string(),
        user_idx_csv.clone(),
    );
    let full_idx_csv = pk_user_indices
        .iter()
        .map(|i| (i + magic::USER_COLUMN_OFFSET).to_string())
        .collect::<Vec<_>>()
        .join(",");
    meta.insert("tonbo.primary_key_full_indices".to_string(), full_idx_csv);
    // Sorting string: first `_ts`: "1:desc:1", then each PK: "<idx>:asc:1"
    let mut parts = vec![format!("1:desc:1")];
    for i in pk_user_indices {
        let full = i + magic::USER_COLUMN_OFFSET;
        parts.push(format!("{}:asc:1", full));
    }
    meta.insert("tonbo.sorting_columns".to_string(), parts.join(";"));

    Arc::new(ArrowSchema::new_with_metadata(fields, meta))
}

/// Compute Parquet PK paths and sorting columns using user field names and PK indices.
///
/// - Sorting: `_ts` ascending, then each PK column ascending, all nulls first to support
///   tombstones.
/// - Paths: `[ _ts, <pk_name> ]` for each PK column.
pub fn compute_pk_paths_and_sorting(
    user_fields: &[Field],
    pk_user_indices: &[usize],
) -> (Vec<ColumnPath>, Vec<SortingColumn>) {
    let mut sorting = Vec::with_capacity(1 + pk_user_indices.len());
    // `_ts` is the second column in the full schema (index 1)
    sorting.push(SortingColumn::new(1_i32, true, true));

    let mut paths = Vec::with_capacity(pk_user_indices.len());
    for &pk_idx in pk_user_indices {
        let arrow_idx = (pk_idx + magic::USER_COLUMN_OFFSET) as i32;
        sorting.push(SortingColumn::new(arrow_idx, false, true));

        let name = user_fields
            .get(pk_idx)
            .map(|f| f.name().to_string())
            .unwrap_or_else(|| format!("col_{pk_idx}"));
        paths.push(ColumnPath::new(vec![magic::TS.to_string(), name]));
    }
    (paths, sorting)
}

/// Phantom wrapper to mark a typed-arrow record type `R` for Tonbo adapters.
///
/// This is a placeholder to facilitate incremental integration without altering
/// existing dynamic/legacy paths. Concrete Arrow arrays/builders and the Schema
/// trait implementation will be added as the refactor proceeds.
#[derive(Debug, Clone, Copy)]
pub struct TonboTypedSchema<R, K> {
    _p: PhantomData<(R, K)>,
}

impl<R, K> Default for TonboTypedSchema<R, K> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

/// Build Tonbo's full Arrow schema + PK metadata from a typed-arrow `SchemaMeta` implementor.
///
/// Returns `(full_schema, pk_paths, sorting_columns, pk_indices_in_full_schema)`.
pub fn tonbo_schema_from_typed<R>() -> (
    Arc<ArrowSchema>,
    Vec<ColumnPath>,
    Vec<SortingColumn>,
    Vec<usize>,
)
where
    R: typed_arrow::schema::SchemaMeta,
{
    // typed-arrow exposes `fields()` as `arrow_schema::Field` which is re-exported by `arrow`.
    let user_fields: Vec<Field> = R::fields();
    // Pull PK user indices from typed schema metadata if present.
    let meta = R::metadata();
    let pk_user_indices: Vec<usize> = meta
        .get(crate::typed::meta::PK_USER_INDICES)
        .map(|s| {
            s.split(',')
                .filter(|p| !p.is_empty())
                .filter_map(|p| p.parse::<usize>().ok())
                .collect()
        })
        .unwrap_or_default();

    let full = compose_with_metadata(&user_fields, &pk_user_indices, &meta);
    let (paths, sorting) = compute_pk_paths_and_sorting(&user_fields, &pk_user_indices);
    let pk_full_indices: Vec<usize> = pk_user_indices
        .iter()
        .map(|i| i + magic::USER_COLUMN_OFFSET)
        .collect();
    (full, paths, sorting, pk_full_indices)
}

/// Same as `tonbo_schema_from_typed`, but accepts explicit user-column PK indices.
pub fn tonbo_schema_from_typed_with_pk<R>(
    pk_user_indices: &[usize],
) -> (
    Arc<ArrowSchema>,
    Vec<ColumnPath>,
    Vec<SortingColumn>,
    Vec<usize>,
)
where
    R: typed_arrow::schema::SchemaMeta,
{
    let user_fields: Vec<Field> = R::fields();
    let meta = R::metadata();
    let full = compose_with_metadata(&user_fields, pk_user_indices, &meta);
    let (paths, sorting) = compute_pk_paths_and_sorting(&user_fields, pk_user_indices);
    let pk_full_indices: Vec<usize> = pk_user_indices
        .iter()
        .map(|i| i + magic::USER_COLUMN_OFFSET)
        .collect();
    (full, paths, sorting, pk_full_indices)
}

// -----------------------------
// Builders and arrays scaffolding
// -----------------------------

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, GenericByteBuilder,
        PrimitiveBuilder, RecordBatch, StringBuilder, UInt32Builder,
    },
    datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

/// Builders wrapper that appends Tonbo sentinels and delegates user columns to typed builders.
pub struct TonboTypedBuilders<R>
where
    R: typed_arrow::schema::BuildRows + Default + crate::record::Record,
{
    inner: <R as typed_arrow::schema::BuildRows>::Builders,
    nulls: BooleanBufferBuilder,
    ts: UInt32Builder,
    schema: Arc<ArrowSchema>,
    user_bytes: usize,
}

impl<R> TonboTypedBuilders<R>
where
    R: typed_arrow::schema::BuildRows
        + typed_arrow::schema::SchemaMeta
        + Default
        + crate::record::Record,
{
    /// Create builders with a capacity hint and a composed full schema.
    pub fn new(capacity: usize) -> Self {
        let inner = <R as typed_arrow::schema::BuildRows>::new_builders(capacity);
        // Fill metadata from typed schema if any.
        let meta = <R as typed_arrow::schema::SchemaMeta>::metadata();
        let pk_user_indices: Vec<usize> = meta
            .get(crate::typed::meta::PK_USER_INDICES)
            .map(|s| {
                s.split(',')
                    .filter(|p| !p.is_empty())
                    .filter_map(|p| p.parse::<usize>().ok())
                    .collect()
            })
            .unwrap_or_default();
        let schema = compose_with_metadata(&R::fields(), &pk_user_indices, &meta);
        Self {
            inner,
            nulls: BooleanBufferBuilder::new(capacity),
            ts: UInt32Builder::with_capacity(capacity),
            schema,
            user_bytes: 0,
        }
    }

    /// Append an optional row with timestamp.
    pub fn append_option_row(&mut self, ts: u32, row: Option<R>) {
        match row {
            Some(r) => {
                self.nulls.append(false);
                self.ts.append_value(ts);
                // Accumulate size before moving the row into inner builders
                let row_sz = r.size();
                RowBuilder::append_row(&mut self.inner, r);
                self.user_bytes += row_sz;
            }
            None => {
                // Append a placeholder row for non-nullable user columns.
                // Tombstone semantics are carried by `_null = true`.
                self.nulls.append(true);
                self.ts.append_value(ts);
                // Accumulate bytes for the default row (placeholder values)
                let d = R::default();
                let row_sz = d.size();
                RowBuilder::append_row(&mut self.inner, d);
                self.user_bytes += row_sz;
            }
        }
    }

    /// Finish builders and produce arrays assembled into a full RecordBatch.
    pub fn finish(mut self) -> TonboTypedBatch {
        let _null: ArrayRef = Arc::new(BooleanArray::new(self.nulls.finish(), None));
        let _ts: ArrayRef = Arc::new(self.ts.finish());
        let user = RowBuilder::finish(self.inner);
        let user_batch = typed_arrow::schema::IntoRecordBatch::into_record_batch(user);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(user_batch.num_columns() + 2);
        cols.push(_null);
        cols.push(_ts);
        cols.extend(user_batch.columns().iter().cloned());
        let batch = RecordBatch::try_new(self.schema.clone(), cols)
            .expect("schema and columns must be consistent");
        TonboTypedBatch { batch }
    }

    /// Bytes written so far (ts-only; user bytes are tracked by typed builders internally).
    pub fn written_size(&self) -> usize {
        // Size of `_null` bit buffer + `_ts` values buffer + user payload bytes
        self.nulls.as_slice().len()
            + std::mem::size_of_val(self.ts.values_slice())
            + self.user_bytes
    }
}

/// Arrays wrapper exposing a full RecordBatch with Tonbo sentinels.
pub struct TonboTypedBatch {
    batch: RecordBatch,
}

impl TonboTypedBatch {
    pub fn as_record_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

// -----------------------------
// Schema + ArrowArrays integration with Tonbo traits
// -----------------------------

impl<R, K> Schema for TonboTypedSchema<R, K>
where
    R: Record<Schema = Self>
        + typed_arrow::schema::SchemaMeta
        + typed_arrow::schema::BuildRows
        + Default
        + for<'a> From<<R as Record>::Ref<'a>>,
    K: Key + PkArray,
    <R as typed_arrow::schema::BuildRows>::Builders: Send,
{
    type Record = R;
    type Columns = TonboTypedArrays<R, K>;
    type Key = K;

    fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        static SCHEMA_PTRS: crate::once_cell::sync::OnceCell<Arc<ArrowSchema>> =
            crate::once_cell::sync::OnceCell::new();
        SCHEMA_PTRS.get_or_init(|| {
            let (schema, _, _, _) = tonbo_schema_from_typed::<R>();
            schema
        })
    }

    fn primary_key_paths_and_sorting(&self) -> (&[ColumnPath], &[SortingColumn]) {
        static PATHS_PTRS: crate::once_cell::sync::OnceCell<Vec<ColumnPath>> =
            crate::once_cell::sync::OnceCell::new();
        static SORTING_PTRS: crate::once_cell::sync::OnceCell<Vec<SortingColumn>> =
            crate::once_cell::sync::OnceCell::new();

        let paths = PATHS_PTRS.get_or_init(|| {
            let user_fields: Vec<Field> = R::fields();
            let meta = R::metadata();
            let pk_user_indices: Vec<usize> = meta
                .get(crate::typed::meta::PK_USER_INDICES)
                .map(|s| {
                    s.split(',')
                        .filter(|p| !p.is_empty())
                        .filter_map(|p| p.parse().ok())
                        .collect()
                })
                .unwrap_or_default();
            let (p, s) = compute_pk_paths_and_sorting(&user_fields, &pk_user_indices);
            SORTING_PTRS.get_or_init(|| s);
            p
        });
        let sorting = SORTING_PTRS.get().expect("sorting initialized with paths");
        (&paths[..], &sorting[..])
    }

    fn primary_key_indices(&self) -> &[usize] {
        static INDICES_PTRS: crate::once_cell::sync::OnceCell<Vec<usize>> =
            crate::once_cell::sync::OnceCell::new();
        let indices = INDICES_PTRS.get_or_init(|| {
            let meta = R::metadata();
            let pk_user_indices: Vec<usize> = meta
                .get(crate::typed::meta::PK_USER_INDICES)
                .map(|s| {
                    s.split(',')
                        .filter(|p| !p.is_empty())
                        .filter_map(|p| p.parse().ok())
                        .collect()
                })
                .unwrap_or_default();
            pk_user_indices
                .into_iter()
                .map(|i| i + magic::USER_COLUMN_OFFSET)
                .collect()
        });
        &indices[..]
    }
}

pub struct TonboTypedArrays<R, K> {
    batch: RecordBatch,
    _p: PhantomData<(R, K)>,
}

pub struct TonboTypedArraysBuilder<R, K>
where
    R: typed_arrow::schema::BuildRows
        + typed_arrow::schema::SchemaMeta
        + Default
        + crate::record::Record,
    K: Key,
{
    inner: TonboTypedBuilders<R>,
    // Track per-row tombstone flags and owned primary keys for later rewrite of PK columns.
    tombstones: Vec<bool>,
    keys: Vec<K>,
}

impl<R, K> ArrowArrays for TonboTypedArrays<R, K>
where
    R: Record<Schema = TonboTypedSchema<R, K>>
        + typed_arrow::schema::SchemaMeta
        + typed_arrow::schema::BuildRows
        + Default
        + for<'a> From<<R as Record>::Ref<'a>>,
    K: Key + PkArray,
    <R as typed_arrow::schema::BuildRows>::Builders: Send,
{
    type Record = R;
    type Builder = TonboTypedArraysBuilder<R, K>;

    fn builder(_schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
        TonboTypedArraysBuilder {
            inner: TonboTypedBuilders::<R>::new(capacity),
            tombstones: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity),
        }
    }

    fn get(
        &self,
        offset: u32,
        projection_mask: &ProjectionMask,
    ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
        let offset = offset as usize;
        if offset >= self.batch.num_rows() {
            return None;
        }
        // Check tombstone by _null column (index 0)
        let nulls = self.batch.column(0).as_boolean();
        if nulls.value(offset) {
            return Some(None);
        }
        let schema = self.batch.schema();
        let rr =
            <R as Record>::Ref::from_record_batch(&self.batch, offset, projection_mask, &schema);
        // from_record_batch returns an OptionRecordRef; .get() yields Option<Ref>
        let val = rr.get();
        let val: Option<<R as Record>::Ref<'_>> = unsafe { transmute(val) };
        Some(val)
    }

    fn as_record_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

impl<R, K> ArrowArraysBuilder<TonboTypedArrays<R, K>> for TonboTypedArraysBuilder<R, K>
where
    R: Record<Schema = TonboTypedSchema<R, K>>
        + typed_arrow::schema::BuildRows
        + typed_arrow::schema::SchemaMeta
        + Default,
    K: Key + PkArray,
    <R as typed_arrow::schema::BuildRows>::Builders: Send,
    for<'a> R: From<<R as Record>::Ref<'a>>,
{
    fn push(
        &mut self,
        key: crate::version::timestamp::Ts<<K as Key>::Ref<'_>>,
        row: Option<<R as Record>::Ref<'_>>,
    ) {
        let ts: u32 = key.ts().into();
        // Capture owned key for possible tombstone row rewrite
        let key_owned: K = key.value().clone().to_key();
        match row {
            Some(rref) => {
                // Convert typed record ref into owned Record using a From impl generated by macros
                let owned: R = <R as From<<R as Record>::Ref<'_>>>::from(rref);
                self.inner.append_option_row(ts, Some(owned));
                self.tombstones.push(false);
                self.keys.push(key_owned);
            }
            None => {
                self.inner.append_option_row(ts, None);
                self.tombstones.push(true);
                self.keys.push(key_owned);
            }
        }
    }

    fn written_size(&self) -> usize {
        self.inner.written_size()
    }

    fn finish(&mut self, indices: Option<&[usize]>) -> TonboTypedArrays<R, K> {
        let arrays = std::mem::replace(&mut self.inner, TonboTypedBuilders::<R>::new(0)).finish();
        let mut batch = arrays.batch;

        // Rewrite PK columns for tombstone rows to hold the key values.
        // Determine PK full indices from typed metadata
        let meta = <R as typed_arrow::schema::SchemaMeta>::metadata();
        let pk_user_indices: Vec<usize> = meta
            .get(crate::typed::meta::PK_USER_INDICES)
            .map(|s| {
                s.split(',')
                    .filter(|p| !p.is_empty())
                    .filter_map(|p| p.parse().ok())
                    .collect()
            })
            .unwrap_or_default();
        let pk_full_indices: Vec<usize> = pk_user_indices
            .into_iter()
            .map(|i| i + crate::magic::USER_COLUMN_OFFSET)
            .collect();

        if !pk_full_indices.is_empty() && self.tombstones.iter().any(|&t| t) {
            let schema = batch.schema();
            let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
            let nulls = batch.column(0).as_boolean();
            // For now, support single-column primary keys.
            if pk_full_indices.len() == 1 {
                let full_idx = pk_full_indices[0];
                let col = batch.column(full_idx).clone();
                let replaced = K::rewrite_pk_column(&col, nulls, &self.tombstones, &self.keys);
                cols[full_idx] = replaced;
                batch = RecordBatch::try_new(schema.clone(), cols)
                    .expect("schema and columns must be consistent");
            }
        }

        // Apply column projection if provided
        if let Some(idxs) = indices {
            let proj_schema = batch
                .schema()
                .project(idxs)
                .expect("projection indices should be valid");
            let proj_cols: Vec<ArrayRef> = idxs.iter().map(|&i| batch.column(i).clone()).collect();
            batch = RecordBatch::try_new(proj_schema.into(), proj_cols)
                .expect("schema and columns must be consistent after projection");
        }

        TonboTypedArrays {
            batch,
            _p: PhantomData,
        }
    }
}

// Trait implemented for supported PK base types to rewrite PK column values.
pub trait PkArray {
    fn rewrite_pk_column(
        col: &ArrayRef,
        nulls: &BooleanArray,
        tombstones: &[bool],
        keys: &[Self],
    ) -> ArrayRef
    where
        Self: Sized;
}

macro_rules! impl_pk_prim {
    ($arrow_ty:ty, $native:ty) => {
        impl PkArray for $native {
            fn rewrite_pk_column(
                col: &ArrayRef,
                nulls: &BooleanArray,
                tombstones: &[bool],
                keys: &[Self],
            ) -> ArrayRef {
                let orig = col.as_primitive::<$arrow_ty>();
                let n = orig.len();
                let mut b = PrimitiveBuilder::<$arrow_ty>::with_capacity(n);
                for i in 0..n {
                    if tombstones[i] && nulls.value(i) {
                        b.append_value(keys[i]);
                    } else {
                        b.append_value(orig.value(i));
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }
        }
    };
}

impl PkArray for String {
    fn rewrite_pk_column(
        col: &ArrayRef,
        nulls: &BooleanArray,
        tombstones: &[bool],
        keys: &[Self],
    ) -> ArrayRef {
        let orig = col.as_string::<i32>();
        let n = orig.len();
        let mut b = StringBuilder::with_capacity(n, 0);
        for i in 0..n {
            if tombstones[i] && nulls.value(i) {
                b.append_value(keys[i].as_str());
            } else {
                b.append_value(orig.value(i));
            }
        }
        Arc::new(b.finish()) as ArrayRef
    }
}

impl PkArray for Vec<u8> {
    fn rewrite_pk_column(
        col: &ArrayRef,
        nulls: &BooleanArray,
        tombstones: &[bool],
        keys: &[Self],
    ) -> ArrayRef {
        type Bin = arrow::datatypes::GenericBinaryType<i32>;
        let orig = col.as_bytes::<Bin>();
        let n = orig.len();
        let mut b = GenericByteBuilder::<Bin>::with_capacity(n, 0);
        for i in 0..n {
            if tombstones[i] && nulls.value(i) {
                b.append_value(&keys[i]);
            } else {
                b.append_value(orig.value(i));
            }
        }
        Arc::new(b.finish()) as ArrayRef
    }
}

impl PkArray for bool {
    fn rewrite_pk_column(
        col: &ArrayRef,
        nulls: &BooleanArray,
        tombstones: &[bool],
        keys: &[Self],
    ) -> ArrayRef {
        let orig = col.as_boolean();
        let n = orig.len();
        let mut b = arrow::array::BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if tombstones[i] && nulls.value(i) {
                b.append_value(keys[i]);
            } else {
                b.append_value(orig.value(i));
            }
        }
        Arc::new(b.finish()) as ArrayRef
    }
}

impl_pk_prim!(UInt8Type, u8);
impl_pk_prim!(UInt16Type, u16);
impl_pk_prim!(UInt32Type, u32);
impl_pk_prim!(UInt64Type, u64);
impl_pk_prim!(Int8Type, i8);
impl_pk_prim!(Int16Type, i16);
impl_pk_prim!(Int32Type, i32);
impl_pk_prim!(Int64Type, i64);
impl_pk_prim!(Float32Type, f32);
impl_pk_prim!(Float64Type, f64);
