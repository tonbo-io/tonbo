use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, StringArray,
        StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{DataType, TimeUnit as ArrowTimeUnit},
};

use crate::value::{TimeUnit, Value, ValueError};

impl Value {
    #[allow(dead_code)]
    pub(crate) fn from_array_ref(array: &ArrayRef, index: usize) -> Result<Self, ValueError> {
        if index >= array.len() {
            return Err(ValueError::InvalidConversion(
                "Index out of bounds".to_string(),
            ));
        }

        if array.is_null(index) {
            return Ok(Value::Null);
        }

        match array.data_type() {
            DataType::Null => Ok(Value::Null),
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| ValueError::InvalidConversion("Boolean cast failed".into()))?;
                Ok(Value::Boolean(arr.value(index)))
            }
            DataType::Int8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int8 cast failed".into()))?;
                Ok(Value::Int8(arr.value(index)))
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int16 cast failed".into()))?;
                Ok(Value::Int16(arr.value(index)))
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int32 cast failed".into()))?;
                Ok(Value::Int32(arr.value(index)))
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Int64 cast failed".into()))?;
                Ok(Value::Int64(arr.value(index)))
            }
            DataType::UInt8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt8 cast failed".into()))?;
                Ok(Value::UInt8(arr.value(index)))
            }
            DataType::UInt16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt16 cast failed".into()))?;
                Ok(Value::UInt16(arr.value(index)))
            }
            DataType::UInt32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt32 cast failed".into()))?;
                Ok(Value::UInt32(arr.value(index)))
            }
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("UInt64 cast failed".into()))?;
                Ok(Value::UInt64(arr.value(index)))
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Float32 cast failed".into()))?;
                Ok(Value::Float32(arr.value(index)))
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Float64 cast failed".into()))?;
                Ok(Value::Float64(arr.value(index)))
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ValueError::InvalidConversion("Utf8 cast failed".into()))?;
                Ok(Value::String(arr.value(index).to_string()))
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| ValueError::InvalidConversion("Binary cast failed".into()))?;
                Ok(Value::Binary(arr.value(index).to_vec()))
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Date32 cast failed".into()))?;
                Ok(Value::Date32(arr.value(index)))
            }
            DataType::Date64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .ok_or_else(|| ValueError::InvalidConversion("Date64 cast failed".into()))?;
                Ok(Value::Date64(arr.value(index)))
            }
            DataType::Timestamp(unit, _) => match unit {
                ArrowTimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampSecond cast failed".into())
                        })?;
                    Ok(Value::Timestamp(arr.value(index), TimeUnit::Second))
                }
                ArrowTimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampMillisecond cast failed".into())
                        })?;
                    Ok(Value::Timestamp(arr.value(index), TimeUnit::Millisecond))
                }
                ArrowTimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampMicrosecond cast failed".into())
                        })?;
                    Ok(Value::Timestamp(arr.value(index), TimeUnit::Microsecond))
                }
                ArrowTimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            ValueError::InvalidConversion("TimestampNanosecond cast failed".into())
                        })?;
                    Ok(Value::Timestamp(arr.value(index), TimeUnit::Nanosecond))
                }
            },
            DataType::List(_) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| ValueError::InvalidConversion("List cast failed".into()))?;
                let list_values = arr.value(index);
                let mut values = Vec::new();
                for i in 0..list_values.len() {
                    values.push(Value::from_array_ref(&list_values, i)?);
                }
                Ok(Value::List(values))
            }
            DataType::Struct(_) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| ValueError::InvalidConversion("Struct cast failed".into()))?;
                let mut fields = Vec::new();
                for (col_index, field) in arr.fields().iter().enumerate() {
                    let column = arr.column(col_index);
                    let value = Value::from_array_ref(column, index)?;
                    fields.push((field.name().clone(), value));
                }
                Ok(Value::Struct(fields))
            }
            _ => Err(ValueError::InvalidConversion(format!(
                "Unsupported data type: {:?}",
                array.data_type()
            ))),
        }
    }
}

pub fn values_to_arrow_array(values: &[Value]) -> Result<ArrayRef, ValueError> {
    if values.is_empty() {
        return Err(ValueError::InvalidConversion(
            "Cannot create array from empty values".into(),
        ));
    }

    match &values[0] {
        Value::Null => {
            let mut builder = arrow::array::NullBuilder::new();
            for _ in values {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        Value::Boolean(_) => {
            let mut builder = arrow::array::BooleanBuilder::new();
            for v in values {
                match v {
                    Value::Boolean(b) => builder.append_value(*b),
                    Value::Null => builder.append_null(),
                    _ => {
                        return Err(ValueError::TypeMismatch {
                            expected: "Boolean".into(),
                            actual: format!("{v:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Value::Int32(_) => {
            let mut builder = arrow::array::Int32Builder::new();
            for v in values {
                match v {
                    Value::Int32(i) => builder.append_value(*i),
                    Value::Null => builder.append_null(),
                    _ => {
                        return Err(ValueError::TypeMismatch {
                            expected: "Int32".into(),
                            actual: format!("{v:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Value::Int64(_) => {
            let mut builder = arrow::array::Int64Builder::new();
            for v in values {
                match v {
                    Value::Int64(i) => builder.append_value(*i),
                    Value::Null => builder.append_null(),
                    _ => {
                        return Err(ValueError::TypeMismatch {
                            expected: "Int64".into(),
                            actual: format!("{v:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Value::Float64(_) => {
            let mut builder = arrow::array::Float64Builder::new();
            for v in values {
                match v {
                    Value::Float64(f) => builder.append_value(*f),
                    Value::Null => builder.append_null(),
                    _ => {
                        return Err(ValueError::TypeMismatch {
                            expected: "Float64".into(),
                            actual: format!("{v:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Value::String(_) => {
            let mut builder = arrow::array::StringBuilder::new();
            for v in values {
                match v {
                    Value::String(s) => builder.append_value(s),
                    Value::Null => builder.append_null(),
                    _ => {
                        return Err(ValueError::TypeMismatch {
                            expected: "String".into(),
                            actual: format!("{v:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ValueError::InvalidConversion(
            "Conversion not yet implemented for this type".into(),
        )),
    }
}
