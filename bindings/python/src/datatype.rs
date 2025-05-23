use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use pyo3::pyclass;
use tonbo::record::{DataType as TonboDataType, F64};

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
    Float,
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
            DataType::Float => f.write_str("float"),
        }
    }
}

impl DataType {
    pub(crate) fn none_value(&self) -> Arc<dyn Any + Send + Sync> {
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
            DataType::Float => Arc::new(Option::<F64>::None),
        }
    }
}

impl From<DataType> for TonboDataType {
    fn from(datatype: DataType) -> Self {
        match datatype {
            DataType::UInt8 => TonboDataType::UInt8,
            DataType::UInt16 => TonboDataType::UInt16,
            DataType::UInt32 => TonboDataType::UInt32,
            DataType::UInt64 => TonboDataType::UInt64,
            DataType::Int8 => TonboDataType::Int8,
            DataType::Int16 => TonboDataType::Int16,
            DataType::Int32 => TonboDataType::Int32,
            DataType::Int64 => TonboDataType::Int64,
            DataType::String => TonboDataType::String,
            DataType::Boolean => TonboDataType::Boolean,
            DataType::Bytes => TonboDataType::Bytes,
            DataType::Float => TonboDataType::Float64,
        }
    }
}

impl From<&DataType> for TonboDataType {
    fn from(datatype: &DataType) -> Self {
        match datatype {
            DataType::UInt8 => TonboDataType::UInt8,
            DataType::UInt16 => TonboDataType::UInt16,
            DataType::UInt32 => TonboDataType::UInt32,
            DataType::UInt64 => TonboDataType::UInt64,
            DataType::Int8 => TonboDataType::Int8,
            DataType::Int16 => TonboDataType::Int16,
            DataType::Int32 => TonboDataType::Int32,
            DataType::Int64 => TonboDataType::Int64,
            DataType::String => TonboDataType::String,
            DataType::Boolean => TonboDataType::Boolean,
            DataType::Bytes => TonboDataType::Bytes,
            DataType::Float => TonboDataType::Float64,
        }
    }
}
