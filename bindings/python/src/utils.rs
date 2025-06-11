use std::{any::Any, sync::Arc};

use pyo3::{
    types::{PyBytes, PyDict, PyDictMethods},
    Bound, Py, PyAny, Python,
};
use tonbo::{
    cast_arc_value,
    record::{DataType as TonboDataType, Value, F64},
};

use crate::{column::Column, datatype::DataType, range};

pub(crate) fn to_dict(py: Python, primary_key_index: usize, record: Vec<Value>) -> Bound<PyDict> {
    let dict = PyDict::new(py);
    for (idx, col) in record.iter().enumerate() {
        let name = col.desc.name.clone();
        match &col.datatype() {
            TonboDataType::UInt8 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<u8>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<u8>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::UInt16 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<u16>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<u16>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::UInt32 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<u32>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<u32>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::UInt64 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<u64>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<u64>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Int8 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<i8>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i8>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Int16 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<i16>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Int32 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<i32>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i32>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Int64 => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<i64>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i64>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Float64 => {
                if idx == primary_key_index {
                    let value: f64 = cast_arc_value!(col.value, F64).into();
                    dict.set_item(name, value).unwrap();
                } else {
                    let value = cast_arc_value!(col.value, Option<F64>).map(|v| f64::from(v));
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::String => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<String>())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<String>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Boolean => {
                if idx == primary_key_index {
                    dict.set_item(name, col.value.as_ref().downcast_ref::<bool>().unwrap())
                        .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<bool>>().unwrap();
                    dict.set_item(name, value).unwrap();
                }
            }
            TonboDataType::Bytes => {
                if idx == primary_key_index {
                    let value = col.value.as_ref().downcast_ref::<Vec<u8>>().unwrap();
                    let v = PyBytes::new(py, value);
                    dict.set_item(name, v).unwrap();
                } else {
                    let value = col
                        .value
                        .as_ref()
                        .downcast_ref::<Option<Vec<u8>>>()
                        .unwrap();
                    dict.set_item(name, value.as_ref().map(|v| PyBytes::new(py, v)))
                        .unwrap();
                }
            }
            TonboDataType::Timestamp(_) => {
                unimplemented!()
            }
            TonboDataType::Float32 => {
                unreachable!()
            }
        }
    }
    dict
}

/// convert key to tonbo key
///
/// Note: `f64` will be convert `F64`
pub(crate) fn to_key(
    py: Python,
    datatype: &DataType,
    key: Py<PyAny>,
) -> Arc<dyn Any + Send + Sync> {
    match datatype {
        DataType::UInt8 => Arc::new(key.extract::<u8>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::UInt16 => Arc::new(key.extract::<u16>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::UInt32 => Arc::new(key.extract::<u32>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::UInt64 => Arc::new(key.extract::<u64>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::Int8 => Arc::new(key.extract::<i8>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::Int16 => Arc::new(key.extract::<i16>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::Int32 => Arc::new(key.extract::<i32>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::Int64 => Arc::new(key.extract::<i64>(py).unwrap()) as Arc<dyn Any + Send + Sync>,
        DataType::String => {
            Arc::new(key.extract::<String>(py).unwrap()) as Arc<dyn Any + Send + Sync>
        }
        DataType::Boolean => {
            Arc::new(key.extract::<bool>(py).unwrap()) as Arc<dyn Any + Send + Sync>
        }
        DataType::Bytes => {
            Arc::new(key.extract::<Vec<u8>>(py).unwrap()) as Arc<dyn Any + Send + Sync>
        }
        DataType::Float => {
            Arc::new(F64::from(key.extract::<f64>(py).unwrap())) as Arc<dyn Any + Send + Sync>
        }
    }
}

/// convert col to tonbo col
///
/// Note: `f64` will be convert `F64`
pub(crate) fn to_col(py: Python, col: &Column, key: Py<PyAny>) -> Value {
    Value::new(
        TonboDataType::from(&col.datatype),
        col.name.to_owned(),
        to_key(py, &col.datatype, key),
        col.nullable,
    )
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
