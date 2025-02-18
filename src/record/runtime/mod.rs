pub(crate) mod array;
mod record;
mod record_ref;
mod schema;
mod value;

pub use array::*;
use arrow::datatypes::DataType as ArrowDataType;
pub use record::*;
pub use record_ref::*;
pub use schema::*;
pub use value::*;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

impl From<&ArrowDataType> for DataType {
    fn from(datatype: &ArrowDataType) -> Self {
        match datatype {
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Binary => DataType::Bytes,
            _ => todo!(),
        }
    }
}
