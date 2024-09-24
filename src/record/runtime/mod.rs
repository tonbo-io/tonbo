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
    INT8,
    INT16,
}

impl From<&DataType> for Datatype {
    fn from(datatype: &DataType) -> Self {
        match datatype {
            DataType::Int8 => Datatype::INT8,
            DataType::Int16 => Datatype::INT16,
            _ => todo!(),
        }
    }
}

impl Datatype {
    pub(crate) fn size(
        &self,
        // is_nullable: bool,
    ) -> usize {
        match self {
            Datatype::INT8 => std::mem::size_of::<i8>(),
            Datatype::INT16 => std::mem::size_of::<i16>(),
        }
    }
}
