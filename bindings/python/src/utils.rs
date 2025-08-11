use std::sync::Arc;

use pyo3::{
    types::{PyDict, PyDictMethods},
    Bound, Py, PyAny, PyResult, Python,
};
use tonbo::record::{Value, ValueRef};

use crate::{column::Column, datatype::DataType, range};

/// Convert a collection of [`Value`] to a dict
pub(crate) fn to_dict(
    py: Python,
    schema: Arc<Vec<Column>>,
    record: Vec<Value>,
) -> PyResult<Bound<PyDict>> {
    let dict = PyDict::new(py);
    for (value, col) in record.into_iter().zip(schema.iter()) {
        let name = col.name.clone();
        match value {
            Value::Null => dict.set_item(name, py.None())?,
            Value::Boolean(v) => dict.set_item(name, v)?,
            Value::Int8(v) => dict.set_item(name, v)?,
            Value::Int16(v) => dict.set_item(name, v)?,
            Value::Int32(v) => dict.set_item(name, v)?,
            Value::Int64(v) => dict.set_item(name, v)?,
            Value::UInt8(v) => dict.set_item(name, v)?,
            Value::UInt16(v) => dict.set_item(name, v)?,
            Value::UInt32(v) => dict.set_item(name, v)?,
            Value::UInt64(v) => dict.set_item(name, v)?,
            Value::Float32(v) => dict.set_item(name, v)?,
            Value::Float64(v) => dict.set_item(name, v)?,
            Value::String(v) => dict.set_item(name, v)?,
            Value::Binary(v) => dict.set_item(name, v)?,
            Value::FixedSizeBinary(v, _) => dict.set_item(name, v)?,
            Value::Date32(_)
            | Value::Date64(_)
            | Value::List(_, _)
            | Value::Timestamp(_, _)
            | Value::Time32(_, _)
            | Value::Time64(_, _)
            | Value::Dictionary(_, _) => unimplemented!(),
        }
    }

    Ok(dict)
}

/// Convert a collection of [`ValueRef`] to a dict
pub(crate) fn to_dict_ref<'py, 'r>(
    py: Python<'py>,
    schema: Arc<Vec<Column>>,
    record: Vec<ValueRef>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (value, col) in record.into_iter().zip(schema.iter()) {
        let name = col.name.clone();
        match value {
            ValueRef::Null => dict.set_item(name, py.None())?,
            ValueRef::Boolean(v) => dict.set_item(name, v)?,
            ValueRef::Int8(v) => dict.set_item(name, v)?,
            ValueRef::Int16(v) => dict.set_item(name, v)?,
            ValueRef::Int32(v) => dict.set_item(name, v)?,
            ValueRef::Int64(v) => dict.set_item(name, v)?,
            ValueRef::UInt8(v) => dict.set_item(name, v)?,
            ValueRef::UInt16(v) => dict.set_item(name, v)?,
            ValueRef::UInt32(v) => dict.set_item(name, v)?,
            ValueRef::UInt64(v) => dict.set_item(name, v)?,
            ValueRef::Float32(v) => dict.set_item(name, v)?,
            ValueRef::Float64(v) => dict.set_item(name, v)?,
            ValueRef::String(v) => dict.set_item(name, v)?,
            ValueRef::Binary(v) => dict.set_item(name, v)?,
            ValueRef::FixedSizeBinary(v, _) => dict.set_item(name, v)?,
            ValueRef::Date32(_)
            | ValueRef::Date64(_)
            | ValueRef::List(_, _)
            | ValueRef::Timestamp(_, _)
            | ValueRef::Time32(_, _)
            | ValueRef::Time64(_, _)
            | ValueRef::Dictionary(_, _) => unimplemented!(),
        }
    }
    Ok(dict)
}

/// convert key to tonbo key
///
/// Note: `f64` will be convert `F64`
pub(crate) fn to_key(py: Python, datatype: &DataType, key: Py<PyAny>) -> Value {
    match datatype {
        DataType::UInt8 => Value::UInt8(key.extract::<u8>(py).unwrap()),
        DataType::UInt16 => Value::UInt16(key.extract::<u16>(py).unwrap()),
        DataType::UInt32 => Value::UInt32(key.extract::<u32>(py).unwrap()),
        DataType::UInt64 => Value::UInt64(key.extract::<u64>(py).unwrap()),
        DataType::Int8 => Value::Int8(key.extract::<i8>(py).unwrap()),
        DataType::Int16 => Value::Int16(key.extract::<i16>(py).unwrap()),
        DataType::Int32 => Value::Int32(key.extract::<i32>(py).unwrap()),
        DataType::Int64 => Value::Int64(key.extract::<i64>(py).unwrap()),
        DataType::String => Value::String(key.extract::<String>(py).unwrap()),
        DataType::Boolean => Value::Boolean(key.extract::<bool>(py).unwrap()),
        DataType::Bytes => Value::Binary(key.extract::<Vec<u8>>(py).unwrap()),
        DataType::Float => Value::Float64(key.extract::<f64>(py).unwrap()),
    }
}

/// convert col to tonbo col
///
/// Note: `f64` will be convert `F64`
pub(crate) fn to_col(py: Python, col: &Column, key: Py<PyAny>) -> Value {
    to_key(py, &col.datatype, key)
}

pub(crate) fn to_bound(
    py: Python,
    col: &Column,
    lower: Option<Py<range::Bound>>,
    high: Option<Py<range::Bound>>,
) -> (std::ops::Bound<Value>, std::ops::Bound<Value>) {
    let lower = match lower {
        Some(bound) => bound.get().to_bound(py, col),
        None => std::ops::Bound::Unbounded,
    };
    let high = match high {
        Some(bound) => bound.get().to_bound(py, col),
        None => std::ops::Bound::Unbounded,
    };

    (lower, high)
}
