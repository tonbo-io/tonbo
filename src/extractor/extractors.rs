use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef};

use super::{errors::KeyExtractError, traits::KeyProjection};
use crate::{
    inmem::immutable::memtable::{MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL},
    key::{KeyComponentRaw, KeyViewRaw, SlicePtr},
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

/// Build a boxed key projection from a schema field's data type and column index.
pub fn projection_for_field(
    col: usize,
    dt: &DataType,
) -> Result<Box<dyn KeyProjection>, KeyExtractError> {
    let ex: Box<dyn KeyProjection> = match dt {
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
/// Extracts a UTF-8 key view from column `col`.
pub struct Utf8KeyExtractor {
    /// Zero-based column index of the key field.
    pub col: usize,
}

impl KeyProjection for Utf8KeyExtractor {
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

    fn project_view(
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
/// Extracts a binary key view from column `col`.
pub struct BinaryKeyExtractor {
    /// Zero-based column index of the key field.
    pub col: usize,
}

impl KeyProjection for BinaryKeyExtractor {
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

    fn project_view(
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

macro_rules! impl_prim_projection {
    ($name:ident, $arr:ty, $dt:expr, $component:expr) => {
        #[derive(Clone, Copy, Debug)]
        /// Extracts a primitive key from a column with the expected Arrow data type.
        pub struct $name {
            /// Zero-based column index of the key field.
            pub col: usize,
        }
        impl KeyProjection for $name {
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

            fn project_view(
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

impl_prim_projection!(U64KeyExtractor, UInt64Array, DataType::UInt64, |v: u64| {
    KeyComponentRaw::U64(v)
});
impl_prim_projection!(U32KeyExtractor, UInt32Array, DataType::UInt32, |v: u32| {
    KeyComponentRaw::U32(v)
});
impl_prim_projection!(I64KeyExtractor, Int64Array, DataType::Int64, |v: i64| {
    KeyComponentRaw::I64(v)
});
impl_prim_projection!(I32KeyExtractor, Int32Array, DataType::Int32, |v: i32| {
    KeyComponentRaw::I32(v)
});
impl_prim_projection!(
    F64KeyExtractor,
    Float64Array,
    DataType::Float64,
    |v: f64| KeyComponentRaw::F64(v.to_bits())
);
impl_prim_projection!(
    F32KeyExtractor,
    Float32Array,
    DataType::Float32,
    |v: f32| KeyComponentRaw::F32(v.to_bits())
);
impl_prim_projection!(
    BoolKeyExtractor,
    BooleanArray,
    DataType::Boolean,
    |v: bool| KeyComponentRaw::Bool(v)
);

/// Composite projection that produces tuple keys by delegating to parts.
pub struct CompositeProjection {
    parts: Vec<Box<dyn KeyProjection>>,
}

impl CompositeProjection {
    /// Construct a composite key projection from individual part projections.
    /// Parts are evaluated in the provided order to build a lexicographic tuple key.
    pub fn new(parts: Vec<Box<dyn KeyProjection>>) -> Self {
        Self { parts }
    }
}

impl KeyProjection for CompositeProjection {
    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), KeyExtractError> {
        for p in &self.parts {
            p.validate_schema(schema)?;
        }
        Ok(())
    }

    fn project_view(
        &self,
        batch: &RecordBatch,
        row: usize,
        out: &mut KeyViewRaw,
    ) -> Result<(), KeyExtractError> {
        out.clear();
        let mut components: Vec<KeyComponentRaw> = Vec::with_capacity(self.parts.len());
        let mut tmp = KeyViewRaw::new();
        for part in &self.parts {
            part.project_view(batch, row, &mut tmp)?;
            components.extend_from_slice(tmp.as_slice());
        }
        out.push(KeyComponentRaw::Struct(components));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use typed_arrow::schema::BuildRows;

    use super::{KeyProjection, *};
    use crate::key::{KeyComponentOwned, KeyViewRaw};

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

        KeyProjection::validate_schema(&utf8, &batch.schema()).unwrap();
        KeyProjection::validate_schema(&i32k, &batch.schema()).unwrap();

        let k0 = utf8.project_owned(&batch, 0).unwrap();
        assert_eq!(k0.as_utf8(), Some("a"));
        let k1 = i32k.project_owned(&batch, 1).unwrap();
        assert!(matches!(k1.component(), KeyComponentOwned::I32(2)));

        let mut reuse = KeyViewRaw::new();
        utf8.project_view(&batch, 0, &mut reuse).unwrap();
        let first = reuse.to_owned();
        utf8.project_view(&batch, 1, &mut reuse).unwrap();
        let second = reuse.to_owned();
        assert_eq!(first.as_utf8(), Some("a"));
        assert_eq!(second.as_utf8(), Some("b"));

        // Tuple composition
        let composite = CompositeProjection::new(vec![
            Box::new(Utf8KeyExtractor { col: 0 }),
            Box::new(I32KeyExtractor { col: 1 }),
        ]);
        let owned = composite.project_owned(&batch, 1).unwrap();
        match owned.component() {
            KeyComponentOwned::Struct(parts) => {
                assert_eq!(parts.len(), 2);
                assert!(matches!(parts[0], KeyComponentOwned::Utf8(_)));
                assert!(matches!(parts[1], KeyComponentOwned::I32(2)));
                assert_eq!(parts[0].as_utf8(), Some("b"));
            }
            other => panic!("expected struct component, got {other:?}"),
        }

        let mut view = KeyViewRaw::new();
        composite.project_view(&batch, 1, &mut view).unwrap();
        let roundtrip = view.to_owned();
        assert_eq!(roundtrip.component(), owned.component());
    }
}
