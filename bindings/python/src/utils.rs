use std::{any::Any, sync::Arc};

use pyo3::{
    types::{PyBytes, PyDict, PyDictMethods},
    Bound, Py, PyAny, Python,
};
use tonbo::record::Datatype;

use crate::{column::Column, datatype::DataType, range};

pub(crate) fn to_dict(
    py: Python,
    primary_key_index: usize,
    record: Vec<tonbo::record::Column>,
) -> Bound<PyDict> {
    let dict = PyDict::new_bound(py);
    for (idx, col) in record.iter().enumerate() {
        match &col.datatype {
            Datatype::Int8 => {
                if idx == primary_key_index {
                    dict.set_item(
                        col.name.clone(),
                        col.value.as_ref().downcast_ref::<i8>().unwrap(),
                    )
                    .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i8>>().unwrap();
                    dict.set_item(col.name.clone(), value).unwrap();
                }
            }
            Datatype::Int16 => {
                if idx == primary_key_index {
                    dict.set_item(
                        col.name.clone(),
                        col.value.as_ref().downcast_ref::<i16>().unwrap(),
                    )
                    .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                    dict.set_item(col.name.clone(), value).unwrap();
                }
            }
            Datatype::Int32 => {
                if idx == primary_key_index {
                    dict.set_item(
                        col.name.clone(),
                        col.value.as_ref().downcast_ref::<i32>().unwrap(),
                    )
                    .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i32>>().unwrap();
                    dict.set_item(col.name.clone(), value).unwrap();
                }
            }
            Datatype::Int64 => {
                if idx == primary_key_index {
                    dict.set_item(
                        col.name.clone(),
                        col.value.as_ref().downcast_ref::<i64>().unwrap(),
                    )
                    .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<i64>>().unwrap();
                    dict.set_item(col.name.clone(), value).unwrap();
                }
            }
            Datatype::String => {
                if idx == primary_key_index {
                    dict.set_item(
                        col.name.clone(),
                        col.value.as_ref().downcast_ref::<String>(),
                    )
                    .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<String>>().unwrap();
                    dict.set_item(col.name.clone(), value).unwrap();
                }
            }
            Datatype::Boolean => {
                if idx == primary_key_index {
                    dict.set_item(
                        col.name.clone(),
                        col.value.as_ref().downcast_ref::<bool>().unwrap(),
                    )
                    .unwrap();
                } else {
                    let value = col.value.as_ref().downcast_ref::<Option<bool>>().unwrap();
                    dict.set_item(col.name.clone(), value).unwrap();
                }
            }
            Datatype::Bytes => {
                if idx == primary_key_index {
                    let value = col.value.as_ref().downcast_ref::<Vec<u8>>().unwrap();
                    let v = PyBytes::new_bound(py, value);
                    dict.set_item(col.name.clone(), v).unwrap();
                } else {
                    let value = col
                        .value
                        .as_ref()
                        .downcast_ref::<Option<Vec<u8>>>()
                        .unwrap();
                    dict.set_item(
                        col.name.clone(),
                        value.as_ref().map(|v| PyBytes::new_bound(py, v)),
                    )
                    .unwrap();
                }
            }
        }
    }
    dict
}

pub(crate) fn to_key(py: Python, datatype: &DataType, key: Py<PyAny>) -> Arc<dyn Any> {
    match datatype {
        DataType::Int8 => Arc::new(key.extract::<i8>(py).unwrap()) as Arc<dyn Any>,
        DataType::Int16 => Arc::new(key.extract::<i16>(py).unwrap()) as Arc<dyn Any>,
        DataType::Int32 => Arc::new(key.extract::<i32>(py).unwrap()) as Arc<dyn Any>,
        DataType::Int64 => Arc::new(key.extract::<i64>(py).unwrap()) as Arc<dyn Any>,
        DataType::String => Arc::new(key.extract::<String>(py).unwrap()) as Arc<dyn Any>,
        DataType::Boolean => Arc::new(key.extract::<bool>(py).unwrap()) as Arc<dyn Any>,
        DataType::Bytes => Arc::new(key.extract::<Vec<u8>>(py).unwrap()) as Arc<dyn Any>,
    }
}

pub(crate) fn to_col(py: Python, col: &Column, key: Py<PyAny>) -> tonbo::record::Column {
    tonbo::record::Column::new(
        Datatype::from(&col.datatype),
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
) -> (
    std::ops::Bound<tonbo::record::Column>,
    std::ops::Bound<tonbo::record::Column>,
) {
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
