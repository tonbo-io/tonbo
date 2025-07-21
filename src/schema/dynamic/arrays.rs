use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};

use crate::{
    schema::{
        dynamic::{DynamicKeyRef, DynamicRow},
        ArrowArrays, Schema,
    },
    value::{Value, ValueRef},
};

/// Dynamic columnar storage that can handle any schema
pub struct DynamicArrays {
    schema: Arc<crate::schema::dynamic::DynamicSchema>,
    columns: Vec<ArrayRef>,
    len: usize,
}

impl DynamicArrays {
    /// Create new dynamic arrays from columns
    pub fn new(schema: Arc<crate::schema::dynamic::DynamicSchema>, columns: Vec<ArrayRef>) -> Self {
        let len = columns.first().map(|c| c.len()).unwrap_or(0);
        Self {
            schema,
            columns,
            len,
        }
    }

    /// Build Arrow arrays from dynamic rows
    fn build_arrays_from_rows<I>(
        rows: I,
        schema: &crate::schema::dynamic::DynamicSchema,
    ) -> Vec<ArrayRef>
    where
        I: IntoIterator<Item = DynamicRow>,
    {
        let rows: Vec<DynamicRow> = rows.into_iter().collect();

        schema
            .field_names()
            .into_iter()
            .map(|field_name| {
                let field = schema.field(field_name).unwrap();
                let values: Vec<Value> = rows
                    .iter()
                    .map(|row| row.get_field(field_name).cloned().unwrap_or(Value::Null))
                    .collect();

                build_array_from_values(&values, &field.data_type)
            })
            .collect()
    }
}

impl ArrowArrays for DynamicArrays {
    type Schema = crate::schema::dynamic::DynamicSchema;
    type Row = DynamicRow;

    fn as_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(self.schema.arrow_schema(), self.columns.clone())
            .expect("Failed to create RecordBatch")
    }

    fn from_rows<I>(rows: I, schema: &Self::Schema) -> Self
    where
        I: IntoIterator<Item = Self::Row>,
    {
        let columns = Self::build_arrays_from_rows(rows, schema);
        let len = columns.first().map(|c| c.len()).unwrap_or(0);
        Self {
            schema: Arc::new(schema.clone()),
            columns,
            len,
        }
    }

    fn get_array(&self, offset: usize) -> Option<ArrayRef> {
        self.columns.get(offset).cloned()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn key_ref_at(&self, row: usize, schema: &Self::Schema) -> DynamicKeyRef<'_> {
        let indices = schema.primary_key_indices();
        let values: Vec<ValueRef<'_>> = indices
            .iter()
            .map(|&idx| {
                let array = &self.columns[idx];
                extract_value_ref_from_array(array, row)
            })
            .collect();

        DynamicKeyRef::new(values)
    }
}

/// Build an Arrow array from a vector of Values
fn build_array_from_values(values: &[Value], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => {
            let array: BooleanArray = values
                .iter()
                .map(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Boolean"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Int8 => {
            let array: Int8Array = values
                .iter()
                .map(|v| match v {
                    Value::Int8(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Int8"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Int16 => {
            let array: Int16Array = values
                .iter()
                .map(|v| match v {
                    Value::Int16(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Int16"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Int32 => {
            let array: Int32Array = values
                .iter()
                .map(|v| match v {
                    Value::Int32(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Int32"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Int64 => {
            let array: Int64Array = values
                .iter()
                .map(|v| match v {
                    Value::Int64(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Int64"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::UInt8 => {
            let array: UInt8Array = values
                .iter()
                .map(|v| match v {
                    Value::UInt8(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected UInt8"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::UInt16 => {
            let array: UInt16Array = values
                .iter()
                .map(|v| match v {
                    Value::UInt16(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected UInt16"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::UInt32 => {
            let array: UInt32Array = values
                .iter()
                .map(|v| match v {
                    Value::UInt32(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected UInt32"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::UInt64 => {
            let array: UInt64Array = values
                .iter()
                .map(|v| match v {
                    Value::UInt64(i) => Some(*i),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected UInt64"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Float32 => {
            let array: Float32Array = values
                .iter()
                .map(|v| match v {
                    Value::Float32(f) => Some(*f),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Float32"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Float64 => {
            let array: Float64Array = values
                .iter()
                .map(|v| match v {
                    Value::Float64(f) => Some(*f),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Float64"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Utf8 => {
            let array: StringArray = values
                .iter()
                .map(|v| match v {
                    Value::String(s) => Some(s.as_str()),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected String"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Binary => {
            let array: BinaryArray = values
                .iter()
                .map(|v| match v {
                    Value::Binary(b) => Some(b.as_slice()),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Binary"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Date32 => {
            let array: Date32Array = values
                .iter()
                .map(|v| match v {
                    Value::Date32(d) => Some(*d),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Date32"),
                })
                .collect();
            Arc::new(array)
        }
        DataType::Date64 => {
            let array: Date64Array = values
                .iter()
                .map(|v| match v {
                    Value::Date64(d) => Some(*d),
                    Value::Null => None,
                    _ => panic!("Type mismatch: expected Date64"),
                })
                .collect();
            Arc::new(array)
        }
        _ => panic!("Unsupported data type: {data_type:?}"),
    }
}

/// Extract a value reference from an Arrow array at a specific row
/// This avoids allocations by returning references directly to the array data
fn extract_value_ref_from_array(array: &ArrayRef, row: usize) -> ValueRef<'_> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Boolean(arr.value(row))
            }
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Int8(arr.value(row))
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Int16(arr.value(row))
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Int32(arr.value(row))
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Int64(arr.value(row))
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::UInt8(arr.value(row))
            }
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::UInt16(arr.value(row))
            }
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::UInt32(arr.value(row))
            }
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::UInt64(arr.value(row))
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Float32(arr.value(row))
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Float64(arr.value(row))
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::String(arr.value(row))
            }
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Binary(arr.value(row))
            }
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Date32(arr.value(row))
            }
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            if arr.is_null(row) {
                ValueRef::Null
            } else {
                ValueRef::Date64(arr.value(row))
            }
        }
        _ => panic!(
            "Unsupported array type for key extraction: {:?}",
            array.data_type()
        ),
    }
}
