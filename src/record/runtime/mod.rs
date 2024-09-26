mod array;
mod column;
mod record;
mod record_ref;

use arrow::datatypes::DataType;
pub use column::*;
pub use record::*;
pub use record_ref::*;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Datatype {
    Int8,
    Int16,
    Int32,
    // String,
}

impl From<&DataType> for Datatype {
    fn from(datatype: &DataType) -> Self {
        match datatype {
            DataType::Int8 => Datatype::Int8,
            DataType::Int16 => Datatype::Int16,
            DataType::Int32 => Datatype::Int32,
            // DataType::Utf8 => Datatype::String,
            _ => todo!(),
        }
    }
}

