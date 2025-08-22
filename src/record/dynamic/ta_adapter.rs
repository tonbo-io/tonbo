use std::sync::Arc;

use arrow::{
    array as arr,
    array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch},
    datatypes::{DataType, Field, Schema as ArrowSchema},
};
use fusio_log::Encode;

use crate::{
    magic::USER_COLUMN_OFFSET,
    record::{
        dynamic::{value::AsValue, DynRecordRef, Value, ValueRef},
        key::KeyRef,
    },
};

/// Thin adapter over typed-arrow-dyn to build dynamic user columns while Tonbo
/// manages sentinels and primary-key semantics.
pub struct TaDynBuilders {
    /// Arrow schema for the full Tonbo record: `[_null, _ts] + user`.
    full_schema: Arc<ArrowSchema>,
    /// Arrow schema for only the user columns (skips `_null` and `_ts`).
    user_schema: Arc<ArrowSchema>,
    /// typed-arrow-dyn builders for the user columns.
    inner: typed_arrow_dyn::DynBuilders,
    /// `_null` sentinel bitset buffer.
    nulls: BooleanBufferBuilder,
    /// `_ts` sentinel values.
    ts: arr::UInt32Builder,
    /// Track tombstone rows to drive PK rewrite.
    tombstones: Vec<bool>,
    /// Owned primary key per appended row.
    keys: Vec<Value>,
    /// Full-column index of the primary key (including sentinels offset).
    pk_full_index: usize,
    /// Accumulated size of user payloads (best-effort accounting).
    user_bytes: usize,
}

impl TaDynBuilders {
    /// Create a new adapter, deriving user schema from the provided full schema.
    pub fn new(full_schema: Arc<ArrowSchema>, capacity: usize) -> Self {
        // Derive PK index from metadata if available. Prefer new key, fallback to legacy.
        let meta = full_schema.metadata();
        let pk_user_index = meta
            .get(crate::typed::meta::PK_USER_INDICES)
            .and_then(|csv| csv.split(',').next())
            .and_then(|s| s.parse::<usize>().ok())
            .or_else(|| {
                meta.get("primary_key_index")
                    .and_then(|s| s.parse::<usize>().ok())
            })
            .unwrap_or(0);
        let pk_full_index = pk_user_index + USER_COLUMN_OFFSET;

        // Construct user-only schema for typed-arrow-dyn builders
        let user_fields: Vec<Field> = full_schema
            .fields()
            .iter()
            .skip(USER_COLUMN_OFFSET)
            .map(|f| (**f).clone())
            .collect();
        let user_schema = Arc::new(ArrowSchema::new(user_fields));

        let inner = typed_arrow_dyn::DynBuilders::new(user_schema.clone(), capacity);

        Self {
            full_schema,
            user_schema,
            inner,
            nulls: BooleanBufferBuilder::new(capacity),
            ts: arr::UInt32Builder::with_capacity(capacity),
            tombstones: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity),
            pk_full_index,
            user_bytes: 0,
        }
    }

    /// Append a row (or tombstone) with timestamp and primary key.
    pub fn append(&mut self, ts: u32, key: ValueRef<'_>, row: Option<DynRecordRef<'_>>) {
        let is_tomb = row.is_none();
        self.nulls.append(is_tomb);
        self.ts.append_value(ts);

        // Accumulate user bytes for accounting
        if let Some(rref) = &row {
            self.user_bytes += rref.size();
        }

        // Convert to DynRow for user columns; for tombstones build placeholders.
        let dyn_row = if let Some(rref) = row.as_ref() {
            let cells = rref
                .columns
                .iter()
                .map(|v| value_ref_to_dyn_cell(v))
                .collect::<Vec<_>>();
            Some(typed_arrow_dyn::DynRow(cells))
        } else {
            // Tombstone: keep PK populated, default others if non-nullable, null if nullable
            let pk_user_index = self.pk_full_index - USER_COLUMN_OFFSET;
            let mut cells: Vec<Option<typed_arrow_dyn::DynCell>> =
                Vec::with_capacity(self.user_schema.fields().len());
            for (j, f) in self.user_schema.fields().iter().enumerate() {
                if j == pk_user_index {
                    let cell = value_ref_to_dyn_cell(&key).expect("pk must be non-null");
                    cells.push(Some(cell));
                } else if f.is_nullable() {
                    cells.push(None);
                } else {
                    let cell = default_cell_for_field(f);
                    cells.push(Some(cell));
                }
            }
            Some(typed_arrow_dyn::DynRow(cells))
        };

        // Append into dynamic builders
        self.inner.append_option_row(dyn_row).expect("append row");

        // Track tombstone flag and owned key
        self.tombstones.push(is_tomb);
        self.keys.push(key.to_key());
    }

    /// Finish and return a complete RecordBatch with sentinels, including PK rewrite
    /// for tombstone rows.
    pub fn finish(mut self) -> RecordBatch {
        // 1) Finish user columns
        let user_batch = self.inner.finish_into_batch();

        // 2) Build sentinel arrays
        let null_arr: ArrayRef = Arc::new(BooleanArray::new(self.nulls.finish(), None));
        let ts_arr: ArrayRef = Arc::new(self.ts.finish());

        // 3) Assemble columns: [_null, _ts] + user arrays
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(user_batch.num_columns() + 2);
        cols.push(null_arr.clone());
        cols.push(ts_arr.clone());
        cols.extend(user_batch.columns().iter().cloned());

        let mut batch = RecordBatch::try_new(self.full_schema.clone(), cols)
            .expect("schema and columns must be consistent");

        // 4) Rewrite PK column values for tombstone rows (single PK for now)
        if self.tombstones.iter().any(|&t| t) {
            let nulls = batch.column(0).as_boolean();
            if self.pk_full_index < batch.num_columns() {
                let col = batch.column(self.pk_full_index).clone();
                let replaced = rewrite_pk_column(&col, nulls, &self.tombstones, &self.keys);
                let mut cols2: Vec<ArrayRef> = batch.columns().to_vec();
                cols2[self.pk_full_index] = replaced;
                batch = RecordBatch::try_new(batch.schema().clone(), cols2)
                    .expect("rebuild batch after pk rewrite");
            }
        }

        batch
    }

    /// Best-effort byte accounting compatible with Tonbo’s expectations.
    pub fn written_size(&self) -> usize {
        self.nulls.as_slice().len()
            + std::mem::size_of_val(self.ts.values_slice())
            + self.user_bytes
    }
}

fn value_ref_to_dyn_cell(v: &ValueRef<'_>) -> Option<typed_arrow_dyn::DynCell> {
    use typed_arrow_dyn::DynCell as C;
    Some(match v {
        ValueRef::Null => C::Null,
        ValueRef::Boolean(b) => C::Bool(*b),
        ValueRef::Int8(x) => C::I8(*x),
        ValueRef::Int16(x) => C::I16(*x),
        ValueRef::Int32(x) => C::I32(*x),
        ValueRef::Int64(x) => C::I64(*x),
        ValueRef::UInt8(x) => C::U8(*x),
        ValueRef::UInt16(x) => C::U16(*x),
        ValueRef::UInt32(x) => C::U32(*x),
        ValueRef::UInt64(x) => C::U64(*x),
        ValueRef::Float32(x) => C::F32(*x),
        ValueRef::Float64(x) => C::F64(*x),
        ValueRef::String(s) => C::Str(s.to_string()),
        ValueRef::Binary(b) => C::Bin(b.to_vec()),
        ValueRef::FixedSizeBinary(b, _w) => C::Bin(b.to_vec()),
        ValueRef::Date32(x) => C::I32(*x),
        ValueRef::Date64(x) => C::I64(*x),
        ValueRef::Timestamp(x, _u) => C::I64(*x),
        ValueRef::Time32(x, _u) => C::I32(*x),
        ValueRef::Time64(x, _u) => C::I64(*x),
        ValueRef::List(_dt, items) => {
            let vals = items
                .iter()
                .map(|a| {
                    let r = ValueRef::from(a.as_ref());
                    if matches!(r, ValueRef::Null) {
                        None
                    } else {
                        value_ref_to_dyn_cell(&r)
                    }
                })
                .collect::<Vec<_>>();
            C::List(vals)
        }
    })
}

fn default_cell_for_field(f: &Field) -> typed_arrow_dyn::DynCell {
    use typed_arrow_dyn::DynCell as C;
    match f.data_type() {
        DataType::Boolean => C::Bool(false),
        DataType::Int8 => C::I8(0),
        DataType::Int16 => C::I16(0),
        DataType::Int32 => C::I32(0),
        DataType::Int64 => C::I64(0),
        DataType::UInt8 => C::U8(0),
        DataType::UInt16 => C::U16(0),
        DataType::UInt32 => C::U32(0),
        DataType::UInt64 => C::U64(0),
        DataType::Float32 => C::F32(0.0),
        DataType::Float64 => C::F64(0.0),
        DataType::Utf8 | DataType::LargeUtf8 => C::Str(String::new()),
        DataType::Binary | DataType::LargeBinary => C::Bin(Vec::new()),
        DataType::FixedSizeBinary(w) => C::Bin(vec![0; *w as usize]),
        DataType::Date32 => C::I32(0),
        DataType::Date64 => C::I64(0),
        DataType::Timestamp(_, _) => C::I64(0),
        DataType::Time32(_) => C::I32(0),
        DataType::Time64(_) => C::I64(0),
        DataType::Duration(_) => C::I64(0),
        DataType::List(_child) | DataType::LargeList(_child) => C::List(Vec::new()),
        DataType::FixedSizeList(child, len) => {
            let mut items = Vec::with_capacity(*len as usize);
            for _ in 0..*len {
                items.push(Some(default_cell_for_field(child.as_ref())));
            }
            C::FixedSizeList(items)
        }
        DataType::Struct(fields) => {
            let mut vals = Vec::with_capacity(fields.len());
            for ch in fields.iter() {
                if ch.is_nullable() {
                    vals.push(None);
                } else {
                    vals.push(Some(default_cell_for_field(ch.as_ref())));
                }
            }
            C::Struct(vals)
        }
        DataType::Dictionary(_, value) => match &**value {
            DataType::Utf8 | DataType::LargeUtf8 => C::Str(String::new()),
            DataType::Binary | DataType::LargeBinary => C::Bin(Vec::new()),
            DataType::FixedSizeBinary(w) => C::Bin(vec![0; *w as usize]),
            _ => C::Null,
        },
        _ => C::Null,
    }
}

fn rewrite_pk_column(
    col: &ArrayRef,
    nulls: &BooleanArray,
    tombstones: &[bool],
    keys: &[Value],
) -> ArrayRef {
    match col.data_type() {
        DataType::Boolean => {
            let orig = col.as_boolean();
            let mut b = arr::BooleanBuilder::with_capacity(orig.len());
            for i in 0..orig.len() {
                if tombstones[i] && nulls.value(i) {
                    b.append_value(*keys[i].as_bool_opt().unwrap_or(&false));
                } else {
                    b.append_value(orig.value(i));
                }
            }
            Arc::new(b.finish())
        }
        DataType::UInt8 => {
            rewrite_prim::<arrow::datatypes::UInt8Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_u8_opt().unwrap_or(&0u8)
            })
        }
        DataType::UInt16 => {
            rewrite_prim::<arrow::datatypes::UInt16Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_u16_opt().unwrap_or(&0u16)
            })
        }
        DataType::UInt32 => {
            rewrite_prim::<arrow::datatypes::UInt32Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_u32_opt().unwrap_or(&0u32)
            })
        }
        DataType::UInt64 => {
            rewrite_prim::<arrow::datatypes::UInt64Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_u64_opt().unwrap_or(&0u64)
            })
        }
        DataType::Int8 => {
            rewrite_prim::<arrow::datatypes::Int8Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_i8_opt().unwrap_or(&0i8)
            })
        }
        DataType::Int16 => {
            rewrite_prim::<arrow::datatypes::Int16Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_i16_opt().unwrap_or(&0i16)
            })
        }
        DataType::Int32 => {
            rewrite_prim::<arrow::datatypes::Int32Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_i32_opt().unwrap_or(&0i32)
            })
        }
        DataType::Int64 => {
            rewrite_prim::<arrow::datatypes::Int64Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_i64_opt().unwrap_or(&0i64)
            })
        }
        DataType::Float32 => {
            rewrite_prim::<arrow::datatypes::Float32Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_f32_opt().unwrap_or(&0.0f32)
            })
        }
        DataType::Float64 => {
            rewrite_prim::<arrow::datatypes::Float64Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_f64_opt().unwrap_or(&0.0f64)
            })
        }
        DataType::Utf8 => {
            let orig = col.as_string::<i32>();
            let mut b = arr::StringBuilder::with_capacity(orig.len(), 0);
            for i in 0..orig.len() {
                if tombstones[i] && nulls.value(i) {
                    b.append_value(keys[i].as_string_opt().unwrap_or(""));
                } else {
                    b.append_value(orig.value(i));
                }
            }
            Arc::new(b.finish())
        }
        DataType::LargeUtf8 => {
            let orig = col.as_string::<i64>();
            let mut b = arr::LargeStringBuilder::with_capacity(orig.len(), 0);
            for i in 0..orig.len() {
                if tombstones[i] && nulls.value(i) {
                    b.append_value(keys[i].as_string_opt().unwrap_or(""));
                } else {
                    b.append_value(orig.value(i));
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            type Bin = arrow::datatypes::GenericBinaryType<i32>;
            let orig = col.as_bytes::<Bin>();
            let mut b = arr::GenericByteBuilder::<Bin>::with_capacity(orig.len(), 0);
            for i in 0..orig.len() {
                if tombstones[i] && nulls.value(i) {
                    b.append_value(keys[i].as_bytes_opt().unwrap_or(&[]));
                } else {
                    b.append_value(orig.value(i));
                }
            }
            Arc::new(b.finish())
        }
        DataType::LargeBinary => {
            type Bin = arrow::datatypes::GenericBinaryType<i64>;
            let orig = col.as_bytes::<Bin>();
            let mut b = arr::GenericByteBuilder::<Bin>::with_capacity(orig.len(), 0);
            for i in 0..orig.len() {
                if tombstones[i] && nulls.value(i) {
                    b.append_value(keys[i].as_bytes_opt().unwrap_or(&[]));
                } else {
                    b.append_value(orig.value(i));
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(w) => {
            let orig = col.as_fixed_size_binary();
            let mut b = arr::FixedSizeBinaryBuilder::with_capacity(orig.len(), *w);
            for i in 0..orig.len() {
                if tombstones[i] && nulls.value(i) {
                    b.append_value(keys[i].as_bytes_opt().unwrap_or(&vec![0; *w as usize]))
                        .unwrap();
                } else {
                    b.append_value(orig.value(i)).unwrap();
                }
            }
            Arc::new(b.finish())
        }
        // Timestamps/Time/Date numerics use I32/I64 paths
        DataType::Date32 => {
            rewrite_prim::<arrow::datatypes::Date32Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_i32_opt().unwrap_or(&0i32)
            })
        }
        DataType::Date64 => {
            rewrite_prim::<arrow::datatypes::Date64Type, _>(col, nulls, tombstones, keys, |v| {
                *v.as_i64_opt().unwrap_or(&0i64)
            })
        }
        DataType::Timestamp(_, _) => rewrite_prim::<arrow::datatypes::TimestampMillisecondType, _>(
            col,
            nulls,
            tombstones,
            keys,
            |v| *v.as_i64_opt().unwrap_or(&0i64),
        ),
        DataType::Time32(_) => rewrite_prim::<arrow::datatypes::Time32MillisecondType, _>(
            col,
            nulls,
            tombstones,
            keys,
            |v| *v.as_i32_opt().unwrap_or(&0i32),
        ),
        DataType::Time64(_) => rewrite_prim::<arrow::datatypes::Time64NanosecondType, _>(
            col,
            nulls,
            tombstones,
            keys,
            |v| *v.as_i64_opt().unwrap_or(&0i64),
        ),
        other => panic!("unsupported PK rewrite datatype: {other:?}"),
    }
}

fn rewrite_prim<T, F>(
    col: &ArrayRef,
    nulls: &BooleanArray,
    tombstones: &[bool],
    keys: &[Value],
    to_base: F,
) -> ArrayRef
where
    T: arrow::datatypes::ArrowPrimitiveType,
    F: Fn(&Value) -> T::Native,
{
    let orig = col.as_primitive::<T>();
    let mut b = arr::PrimitiveBuilder::<T>::with_capacity(orig.len());
    for i in 0..orig.len() {
        if tombstones[i] && nulls.value(i) {
            b.append_value(to_base(&keys[i]));
        } else {
            b.append_value(orig.value(i));
        }
    }
    Arc::new(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dyn_schema,
        record::{DynRecord, Record, RecordRef, Schema},
    };

    #[test]
    fn test_ta_adapter_tombstone_keeps_pk() {
        let schema = dyn_schema!(("id", UInt64, false), 0);
        let full = schema.arrow_schema().clone();
        let mut b = TaDynBuilders::new(full.clone(), 3);

        // Live row
        let rec = DynRecord::new(vec![Value::UInt64(42)], 0);
        let rref = rec.as_record_ref();
        b.append(1u32, rref.clone().key(), Some(rref.clone()));
        // Tombstone
        b.append(2u32, rref.clone().key(), None);
        // Live row again
        b.append(3u32, rref.clone().key(), Some(rref));

        let batch = b.finish();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 3);

        // Check sentinels
        let nulls = batch.column(0).as_boolean();
        assert!(!nulls.value(0));
        assert!(nulls.value(1));
        assert!(!nulls.value(2));

        let ts = batch
            .column(1)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        assert_eq!(ts.value(0), 1);
        assert_eq!(ts.value(1), 2);
        assert_eq!(ts.value(2), 3);

        // PK column should keep key at tombstone row
        let ids = batch
            .column(2)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        assert_eq!(ids.value(0), 42);
        assert_eq!(ids.value(1), 42); // rewritten
        assert_eq!(ids.value(2), 42);
    }

    #[test]
    fn test_ta_adapter_tombstone_keeps_pk_string() {
        let schema = dyn_schema!(("id", Utf8, false), 0);
        let full = schema.arrow_schema().clone();
        let mut b = TaDynBuilders::new(full.clone(), 3);

        let rec = DynRecord::new(vec![Value::String("key-1".into())], 0);
        let rref = rec.as_record_ref();
        b.append(1u32, rref.clone().key(), Some(rref.clone()));
        b.append(2u32, rref.clone().key(), None);
        b.append(3u32, rref.clone().key(), Some(rref));

        let batch = b.finish();
        let nulls = batch.column(0).as_boolean();
        assert_eq!(nulls.len(), 3);
        let ids = batch.column(2).as_string::<i32>();
        assert_eq!(ids.value(0), "key-1");
        assert_eq!(ids.value(1), "key-1");
        assert_eq!(ids.value(2), "key-1");
    }

    #[test]
    fn test_ta_adapter_tombstone_keeps_pk_binary() {
        let schema = dyn_schema!(("id", Binary, false), 0);
        let full = schema.arrow_schema().clone();
        let mut b = TaDynBuilders::new(full.clone(), 3);

        let rec = DynRecord::new(vec![Value::Binary(vec![1, 2, 3, 4])], 0);
        let rref = rec.as_record_ref();
        b.append(1u32, rref.clone().key(), Some(rref.clone()));
        b.append(2u32, rref.clone().key(), None);
        b.append(3u32, rref.clone().key(), Some(rref));

        let batch = b.finish();
        let bin = batch
            .column(2)
            .as_bytes::<arrow::datatypes::GenericBinaryType<i32>>();
        assert_eq!(bin.value(0), &[1, 2, 3, 4]);
        assert_eq!(bin.value(1), &[1, 2, 3, 4]);
        assert_eq!(bin.value(2), &[1, 2, 3, 4]);
    }
}
