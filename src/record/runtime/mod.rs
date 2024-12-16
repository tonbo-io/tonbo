pub(crate) mod array;
mod record;
mod record_ref;
mod schema;
mod value;

use arrow::datatypes::DataType;
pub use record::*;
pub use record_ref::*;
pub use schema::*;
pub use value::*;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Datatype {
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
            DataType::Utf8 => Datatype::String,
            DataType::Boolean => Datatype::Boolean,
            DataType::Binary => Datatype::Bytes,
            _ => todo!(),
        }
    }
}
