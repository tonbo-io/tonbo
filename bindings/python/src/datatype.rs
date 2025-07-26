use std::fmt::{Debug, Formatter};

use pyo3::pyclass;
use tonbo::arrow::datatypes::DataType as ArrowDataType;

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

impl From<DataType> for tonbo::arrow::datatypes::DataType {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Bytes => ArrowDataType::Binary,
            DataType::Float => ArrowDataType::Float64,
        }
    }
}
