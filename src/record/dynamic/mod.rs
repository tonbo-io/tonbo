pub(crate) mod array;
mod builder;
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

use crate::record::TimeUnit;

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
    LargeString,
    Boolean,
    Bytes,
    LargeBinary,
    FixedSizeBinary(i32),
    Float32,
    Float64,
    Timestamp(TimeUnit),
    /// representing the elapsed time since midnight in the unit of
    /// `TimeUnit`. Must be either seconds or milliseconds.
    Time32(TimeUnit),
    /// representing the elapsed time since midnight in the unit of `TimeUnit`. Must be either
    /// microseconds or nanoseconds.
    ///
    /// See more details in [`arrow::datatypes::DataType::Time64`].
    Time64(TimeUnit),
    /// representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds.
    ///
    /// See [`arrow::datatypes::DataType::Date64`] for more details.
    Date64,
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
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Binary => DataType::Bytes,
            ArrowDataType::Timestamp(unit, tz) => {
                debug_assert!(tz.is_none(), "expected timezone is none, get {tz:?}");
                DataType::Timestamp(unit.into())
            }
            ArrowDataType::Time32(unit) => DataType::Time32(unit.into()),
            ArrowDataType::Time64(unit) => DataType::Time64(unit.into()),
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,
            ArrowDataType::LargeBinary => DataType::LargeBinary,
            ArrowDataType::LargeUtf8 => DataType::LargeString,
            ArrowDataType::FixedSizeBinary(w) => DataType::FixedSizeBinary(*w),
            _ => todo!(),
        }
    }
}

/// Cast the `Arc<dyn Any>` to the value of given type.
#[macro_export]
macro_rules! cast_arc_value {
    ($value:expr, $type:ty) => {
        $value.as_ref().downcast_ref::<$type>().unwrap()
    };
}
