use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use pyo3::{pyclass, PyObject, Python, ToPyObject};
use tonbo::record::Datatype;

#[pyclass]
#[derive(PartialEq, Clone)]
pub enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Boolean,
    Bytes,
}

impl Debug for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::UInt8 => f.write_str("u8"),
            DataType::UInt16 => f.write_str("u16"),
            DataType::UInt32 => f.write_str("u32"),
            DataType::UInt64 => f.write_str("u64"),
            DataType::Int8 => f.write_str("i8"),
            DataType::Int16 => f.write_str("i16"),
            DataType::Int32 => f.write_str("i32"),
            DataType::Int64 => f.write_str("i64"),
            DataType::String => f.write_str("str"),
            DataType::Boolean => f.write_str("bool"),
            DataType::Bytes => f.write_str("bytes"),
        }
    }
}

impl DataType {
    #[allow(unused)]
    pub(crate) fn default_value(&self, py: Python<'_>) -> PyObject {
        match self {
            DataType::UInt8 => u8::default().to_object(py),
            DataType::UInt16 => u16::default().to_object(py),
            DataType::UInt32 => u32::default().to_object(py),
            DataType::UInt64 => u64::default().to_object(py),
            DataType::Int8 => i8::default().to_object(py),
            DataType::Int16 => i16::default().to_object(py),
            DataType::Int32 => i32::default().to_object(py),
            DataType::Int64 => i64::default().to_object(py),
            DataType::String => String::default().to_object(py),
            DataType::Boolean => bool::default().to_object(py),
            DataType::Bytes => Vec::<u8>::default().to_object(py),
        }
    }
    pub(crate) fn none_value(&self) -> Arc<dyn Any> {
        match self {
            DataType::UInt8 => Arc::new(Option::<u8>::None),
            DataType::UInt16 => Arc::new(Option::<u16>::None),
            DataType::UInt32 => Arc::new(Option::<u32>::None),
            DataType::UInt64 => Arc::new(Option::<u64>::None),
            DataType::Int8 => Arc::new(Option::<i8>::None),
            DataType::Int16 => Arc::new(Option::<i16>::None),
            DataType::Int32 => Arc::new(Option::<i32>::None),
            DataType::Int64 => Arc::new(Option::<i64>::None),
            DataType::String => Arc::new(Option::<String>::None),
            DataType::Boolean => Arc::new(Option::<bool>::None),
            DataType::Bytes => Arc::new(Option::<Vec<u8>>::None),
        }
    }
}

impl From<DataType> for Datatype {
    fn from(datatype: DataType) -> Self {
        match datatype {
            DataType::UInt8 => Datatype::UInt8,
            DataType::UInt16 => Datatype::UInt16,
            DataType::UInt32 => Datatype::UInt32,
            DataType::UInt64 => Datatype::UInt64,
            DataType::Int8 => Datatype::Int8,
            DataType::Int16 => Datatype::Int16,
            DataType::Int32 => Datatype::Int32,
            DataType::Int64 => Datatype::Int64,
            DataType::String => Datatype::String,
            DataType::Boolean => Datatype::Boolean,
            DataType::Bytes => Datatype::Bytes,
        }
    }
}

impl From<&DataType> for Datatype {
    fn from(datatype: &DataType) -> Self {
        match datatype {
            DataType::UInt8 => Datatype::UInt8,
            DataType::UInt16 => Datatype::UInt16,
            DataType::UInt32 => Datatype::UInt32,
            DataType::UInt64 => Datatype::UInt64,
            DataType::Int8 => Datatype::Int8,
            DataType::Int16 => Datatype::Int16,
            DataType::Int32 => Datatype::Int32,
            DataType::Int64 => Datatype::Int64,
            DataType::String => Datatype::String,
            DataType::Boolean => Datatype::Boolean,
            DataType::Bytes => Datatype::Bytes,
        }
    }
}
