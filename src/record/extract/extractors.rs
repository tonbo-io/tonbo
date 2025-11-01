use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef};

use super::{
    errors::KeyExtractError,
    traits::{BatchKeyExtractor, DynKeyExtractor},
};
use crate::{
    inmem::immutable::{
        keys::{BinKey, StrKey},
        memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
    },
    key::{KeyComponentRaw, KeyOwned, KeyViewRaw, SlicePtr},
};

fn check_bounds(batch: &RecordBatch, col: usize, row: usize) -> Result<(), KeyExtractError> {
    if col >= batch.num_columns() {
        return Err(KeyExtractError::ColumnOutOfBounds(col, batch.num_columns()));
    }
    if row >= batch.num_rows() {
        return Err(KeyExtractError::RowOutOfBounds(row, batch.num_rows()));
    }
    Ok(())
}

/// Build a boxed dynamic extractor from a schema field's data type and column index.
pub fn dyn_extractor_for_field(
    col: usize,
    dt: &DataType,
) -> Result<Box<dyn DynKeyExtractor>, KeyExtractError> {
    let ex: Box<dyn DynKeyExtractor> = match dt {
        DataType::Utf8 => Box::new(Utf8KeyExtractor { col }),
        DataType::Binary => Box::new(BinaryKeyExtractor { col }),
        DataType::UInt64 => Box::new(U64KeyExtractor { col }),
        DataType::UInt32 => Box::new(U32KeyExtractor { col }),
        DataType::Int64 => Box::new(I64KeyExtractor { col }),
        DataType::Int32 => Box::new(I32KeyExtractor { col }),
        DataType::Float64 => Box::new(F64KeyExtractor { col }),
        DataType::Float32 => Box::new(F32KeyExtractor { col }),
        DataType::Boolean => Box::new(BoolKeyExtractor { col }),
        other => {
            return Err(KeyExtractError::UnsupportedType {
                col,
                data_type: other.clone(),
            });
        }
    };
    Ok(ex)
}

/// Build a row of dynamic cells by reading a single row from a `RecordBatch`.
pub fn row_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<Option<typed_arrow_dyn::DynCell>>, KeyExtractError> {
    use typed_arrow_dyn::DynCell as C;
    if row >= batch.num_rows() {
        return Err(KeyExtractError::RowOutOfBounds(row, batch.num_rows()));
    }
    let schema = batch.schema();
    let mut cells = Vec::with_capacity(batch.num_columns());
    for (col_idx, arr) in batch.columns().iter().enumerate() {
        let field = schema.field(col_idx);
        if field.name() == MVCC_COMMIT_COL || field.name() == MVCC_TOMBSTONE_COL {
            continue;
        }
        if arr.is_null(row) {
            cells.push(None);
            continue;
        }
        let dt = arr.data_type();
        let cell = match dt {
            DataType::Boolean => Some(C::Bool(
                arr.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int32 => Some(C::I32(
                arr.as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64 => Some(C::I64(
                arr.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::UInt32 => Some(C::U32(
                arr.as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::UInt64 => Some(C::U64(
                arr.as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Float32 => Some(C::F32(
                arr.as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Float64 => Some(C::F64(
                arr.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Utf8 => Some(C::Str(
                arr.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .to_owned(),
            )),
            DataType::Binary => Some(C::Bin(
                arr.as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row)
                    .to_vec(),
            )),
            other => {
                return Err(KeyExtractError::UnsupportedType {
                    col: col_idx,
                    data_type: other.clone(),
                });
            }
        };
        cells.push(cell);
    }
    Ok(cells)
}

/// Utf8 key from a single column.
#[derive(Clone, Copy, Debug)]
/// Extracts a `StrKey` from an `Utf8` column at `col`.
pub struct Utf8KeyExtractor {
    /// Zero-based column index of the key field.
    pub col: usize,
}

impl BatchKeyExtractor<StrKey> for Utf8KeyExtractor {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        let fields = schema.fields();
        if self.col >= fields.len() {
            return Err(KeyExtractError::ColumnOutOfBounds(self.col, fields.len()));
        }
        let f = &fields[self.col];
        let actual = f.data_type();
        if !matches!(actual, DataType::Utf8) {
            return Err(KeyExtractError::WrongType {
                col: self.col,
                expected: DataType::Utf8,
                actual: actual.clone(),
            });
        }
        Ok(())
    }
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<StrKey, KeyExtractError> {
        check_bounds(batch, self.col, row)?;
        let arr = batch
            .column(self.col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema validated");
        Ok(StrKey::from_string_array(arr, row))
    }
}

impl DynKeyExtractor for Utf8KeyExtractor {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        <Self as BatchKeyExtractor<StrKey>>::validate_schema(self, schema)
    }

    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyOwned, KeyExtractError> {
        Ok(KeyOwned::from(<Self as BatchKeyExtractor<StrKey>>::key_at(
            self, batch, row,
        )?))
    }

    fn key_view_at(
        &self,
        batch: &RecordBatch,
        row: usize,
        out: &mut KeyViewRaw,
    ) -> Result<(), KeyExtractError> {
        check_bounds(batch, self.col, row)?;
        let arr = batch
            .column(self.col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema validated");
        let offsets = arr.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let len = end - start;
        let data = arr.value_data();
        let ptr = unsafe { SlicePtr::from_raw_parts(data.as_ptr().add(start), len) };
        out.clear();
        out.push(KeyComponentRaw::Utf8(ptr));
        Ok(())
    }
}

/// Binary key from a single column.
#[derive(Clone, Copy, Debug)]
/// Extracts a `BinKey` from a `Binary` column at `col`.
pub struct BinaryKeyExtractor {
    /// Zero-based column index of the key field.
    pub col: usize,
}

impl BatchKeyExtractor<BinKey> for BinaryKeyExtractor {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        let fields = schema.fields();
        if self.col >= fields.len() {
            return Err(KeyExtractError::ColumnOutOfBounds(self.col, fields.len()));
        }
        let f = &fields[self.col];
        let actual = f.data_type();
        if !matches!(actual, DataType::Binary) {
            return Err(KeyExtractError::WrongType {
                col: self.col,
                expected: DataType::Binary,
                actual: actual.clone(),
            });
        }
        Ok(())
    }
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<BinKey, KeyExtractError> {
        check_bounds(batch, self.col, row)?;
        let arr = batch
            .column(self.col)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("schema validated");
        Ok(BinKey::from_binary_array(arr, row))
    }
}

impl DynKeyExtractor for BinaryKeyExtractor {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        <Self as BatchKeyExtractor<BinKey>>::validate_schema(self, schema)
    }

    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyOwned, KeyExtractError> {
        Ok(KeyOwned::from(<Self as BatchKeyExtractor<BinKey>>::key_at(
            self, batch, row,
        )?))
    }

    fn key_view_at(
        &self,
        batch: &RecordBatch,
        row: usize,
        out: &mut KeyViewRaw,
    ) -> Result<(), KeyExtractError> {
        check_bounds(batch, self.col, row)?;
        let arr = batch
            .column(self.col)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("schema validated");
        let offsets = arr.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let len = end - start;
        let data = arr.value_data();
        let ptr = unsafe { SlicePtr::from_raw_parts(data.as_ptr().add(start), len) };
        out.clear();
        out.push(KeyComponentRaw::Binary(ptr));
        Ok(())
    }
}

macro_rules! impl_prim_extractor {
    ($name:ident, $t:ty, $arr:ty, $dt:expr, $component:expr) => {
        #[derive(Clone, Copy, Debug)]
        /// Extracts a primitive key from a column with the expected Arrow data type.
        pub struct $name {
            /// Zero-based column index of the key field.
            pub col: usize,
        }
        impl BatchKeyExtractor<$t> for $name {
            fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
                let fields = schema.fields();
                if self.col >= fields.len() {
                    return Err(KeyExtractError::ColumnOutOfBounds(self.col, fields.len()));
                }
                let f = &fields[self.col];
                let actual = f.data_type();
                if *actual != $dt {
                    return Err(KeyExtractError::WrongType {
                        col: self.col,
                        expected: $dt,
                        actual: actual.clone(),
                    });
                }
                Ok(())
            }
            fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<$t, KeyExtractError> {
                check_bounds(batch, self.col, row)?;
                let arr = batch
                    .column(self.col)
                    .as_any()
                    .downcast_ref::<$arr>()
                    .expect("schema validated");
                Ok(arr.value(row))
            }
        }

        impl DynKeyExtractor for $name {
            fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
                <Self as BatchKeyExtractor<$t>>::validate_schema(self, schema)
            }

            fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyOwned, KeyExtractError> {
                let value = <Self as BatchKeyExtractor<$t>>::key_at(self, batch, row)?;
                Ok(KeyOwned::from(value))
            }

            fn key_view_at(
                &self,
                batch: &RecordBatch,
                row: usize,
                out: &mut KeyViewRaw,
            ) -> Result<(), KeyExtractError> {
                check_bounds(batch, self.col, row)?;
                let arr = batch
                    .column(self.col)
                    .as_any()
                    .downcast_ref::<$arr>()
                    .expect("schema validated");
                let value = arr.value(row);
                out.clear();
                out.push(($component)(value));
                Ok(())
            }
        }
    };
}

impl_prim_extractor!(
    U64KeyExtractor,
    u64,
    UInt64Array,
    DataType::UInt64,
    |v: u64| KeyComponentRaw::U64(v)
);
impl_prim_extractor!(
    U32KeyExtractor,
    u32,
    UInt32Array,
    DataType::UInt32,
    |v: u32| KeyComponentRaw::U32(v)
);
impl_prim_extractor!(
    I64KeyExtractor,
    i64,
    Int64Array,
    DataType::Int64,
    |v: i64| KeyComponentRaw::I64(v)
);
impl_prim_extractor!(
    I32KeyExtractor,
    i32,
    Int32Array,
    DataType::Int32,
    |v: i32| KeyComponentRaw::I32(v)
);
impl_prim_extractor!(
    F64KeyExtractor,
    f64,
    Float64Array,
    DataType::Float64,
    |v: f64| KeyComponentRaw::F64(v.to_bits())
);
impl_prim_extractor!(
    F32KeyExtractor,
    f32,
    Float32Array,
    DataType::Float32,
    |v: f32| KeyComponentRaw::F32(v.to_bits())
);
impl_prim_extractor!(
    BoolKeyExtractor,
    bool,
    BooleanArray,
    DataType::Boolean,
    |v: bool| KeyComponentRaw::Bool(v)
);

/// Composite dynamic extractor that produces tuple keys by delegating to parts.
pub struct CompositeDynExtractor {
    parts: Vec<Box<dyn DynKeyExtractor>>,
}

impl CompositeDynExtractor {
    /// Construct a composite dynamic key extractor from individual part extractors.
    /// Parts are evaluated in the provided order to build a lexicographic tuple key.
    pub fn new(parts: Vec<Box<dyn DynKeyExtractor>>) -> Self {
        Self { parts }
    }
}

impl DynKeyExtractor for CompositeDynExtractor {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        for p in &self.parts {
            p.validate_schema(schema)?;
        }
        Ok(())
    }
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<KeyOwned, KeyExtractError> {
        let mut out = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            out.push(part.key_at(batch, row)?);
        }
        Ok(KeyOwned::tuple(out))
    }
    fn key_view_at(
        &self,
        batch: &RecordBatch,
        row: usize,
        out: &mut KeyViewRaw,
    ) -> Result<(), KeyExtractError> {
        out.clear();
        let mut components: Vec<KeyComponentRaw> = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            let mut tmp = KeyViewRaw::new();
            part.key_view_at(batch, row, &mut tmp)?;
            components.extend_from_slice(tmp.as_slice());
        }
        out.push(KeyComponentRaw::Struct(components));
        Ok(())
    }
}

/// Compose two extractors into a tuple key.
impl<A, KA, B, KB> BatchKeyExtractor<(KA, KB)> for (A, B)
where
    A: BatchKeyExtractor<KA>,
    B: BatchKeyExtractor<KB>,
{
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        self.0.validate_schema(schema)?;
        self.1.validate_schema(schema)
    }
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<(KA, KB), KeyExtractError> {
        let a = self.0.key_at(batch, row)?;
        let b = self.1.key_at(batch, row)?;
        Ok((a, b))
    }
}

/// Compose three extractors into a triple key.
impl<A, KA, B, KB, C, KC> BatchKeyExtractor<(KA, KB, KC)> for (A, B, C)
where
    A: BatchKeyExtractor<KA>,
    B: BatchKeyExtractor<KB>,
    C: BatchKeyExtractor<KC>,
{
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        self.0.validate_schema(schema)?;
        self.1.validate_schema(schema)?;
        self.2.validate_schema(schema)
    }
    fn key_at(&self, batch: &RecordBatch, row: usize) -> Result<(KA, KB, KC), KeyExtractError> {
        let a = self.0.key_at(batch, row)?;
        let b = self.1.key_at(batch, row)?;
        let c = self.2.key_at(batch, row)?;
        Ok((a, b, c))
    }
}

#[cfg(test)]
mod tests {
    use typed_arrow::schema::BuildRows;

    use super::{BatchKeyExtractor, *};

    #[derive(typed_arrow::Record, Clone)]
    struct User {
        id: String,
        score: i32,
    }

    #[test]
    fn extract_utf8_and_primitive_keys() {
        // Build a small batch via typed-arrow builders
        let mut b = User::new_builders(3);
        <User as BuildRows>::Builders::append_row(
            &mut b,
            User {
                id: "a".into(),
                score: 1,
            },
        );
        <User as BuildRows>::Builders::append_row(
            &mut b,
            User {
                id: "b".into(),
                score: 2,
            },
        );
        let arrays = <User as BuildRows>::Builders::finish(b);
        let batch = arrays.into_record_batch();

        let utf8 = Utf8KeyExtractor { col: 0 };
        let i32k = I32KeyExtractor { col: 1 };

        BatchKeyExtractor::validate_schema(&utf8, &batch.schema()).unwrap();
        BatchKeyExtractor::validate_schema(&i32k, &batch.schema()).unwrap();

        let k0 = BatchKeyExtractor::key_at(&utf8, &batch, 0).unwrap();
        assert_eq!(k0.as_str(), "a");
        let k1 = BatchKeyExtractor::key_at(&i32k, &batch, 1).unwrap();
        assert_eq!(k1, 2);

        // Tuple composition
        let tup = (utf8, i32k);
        let (k_s, k_i) = BatchKeyExtractor::key_at(&tup, &batch, 1).unwrap();
        assert_eq!(k_s.as_str(), "b");
        assert_eq!(k_i, 2);
    }
}
