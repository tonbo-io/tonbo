pub(crate) mod array;
mod record;
mod record_ref;
mod value;

use std::sync::Arc;

pub use array::*;
use common::{datatype::DataType, Date32, Date64, Time32, Time64, Timestamp, Value, F32, F64};
pub use record::*;
pub use record_ref::*;
pub use value::*;

/// Cast the `Arc<dyn Any>` to the value of given type.
#[macro_export]
macro_rules! cast_arc_value {
    ($value:expr, $type:ty) => {
        $value.as_ref().downcast_ref::<$type>().unwrap()
    };
}

pub(crate) fn null_value(data_type: &DataType) -> Arc<dyn Value> {
    match data_type {
        DataType::Boolean => Arc::new(None::<bool>),
        DataType::Int8 => Arc::new(None::<i8>),
        DataType::Int16 => Arc::new(None::<i16>),
        DataType::Int32 => Arc::new(None::<i32>),
        DataType::Int64 => Arc::new(None::<i64>),
        DataType::UInt8 => Arc::new(None::<u8>),
        DataType::UInt16 => Arc::new(None::<u16>),
        DataType::UInt32 => Arc::new(None::<u32>),
        DataType::UInt64 => Arc::new(None::<u64>),
        DataType::Float32 => Arc::new(None::<F32>),
        DataType::Float64 => Arc::new(None::<F64>),
        DataType::Timestamp(_) => Arc::new(None::<Timestamp>),
        DataType::Date32 => Arc::new(None::<Date32>),
        DataType::Date64 => Arc::new(None::<Date64>),
        DataType::Time32(_) => Arc::new(None::<Time32>),
        DataType::Time64(_) => Arc::new(None::<Time64>),
        DataType::Bytes | DataType::LargeBinary => Arc::new(None::<Vec<u8>>),
        DataType::String | DataType::LargeString => Arc::new(None::<String>),
    }
}
