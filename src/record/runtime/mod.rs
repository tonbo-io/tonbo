pub(crate) mod array;
mod record;
mod record_ref;
mod schema;
mod value;

use std::sync::Arc;

pub use array::*;
use arrow::datatypes::{DataType as ArrowDataType, Field};
use fusio_log::{Decode, Encode};
pub use record::*;
pub use record_ref::*;
pub use schema::*;
pub use value::*;

type ValueDescRef = Arc<ValueDesc>;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
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
    Float32,
    Float64,
    List(ValueDescRef),
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
            ArrowDataType::List(field) => DataType::List(ValueDescRef::new(field.as_ref().into())),
            _ => todo!(),
        }
    }
}

impl From<&DataType> for ArrowDataType {
    fn from(datatype: &DataType) -> Self {
        match datatype {
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Bytes => ArrowDataType::Binary,
            DataType::List(field) => ArrowDataType::List(Arc::new(Field::new(
                &field.name,
                (&field.datatype).into(),
                field.is_nullable,
            ))),
        }
    }
}

impl From<u8> for DataType {
    fn from(value: u8) -> Self {
        match value {
            0 => DataType::UInt8,
            1 => DataType::UInt16,
            2 => DataType::UInt32,
            3 => DataType::UInt64,
            4 => DataType::Int8,
            5 => DataType::Int16,
            6 => DataType::Int32,
            7 => DataType::Int64,
            8 => DataType::String,
            9 => DataType::Boolean,
            10 => DataType::Bytes,
            11 => DataType::Float32,
            12 => DataType::Float64,
            _ => panic!("can not construct `DataType` from {}", value),
        }
    }
}

impl Encode for DataType {
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: fusio::Write,
    {
        match self {
            DataType::UInt8 => 0_u8.encode(writer).await?,
            DataType::UInt16 => 1_u8.encode(writer).await?,
            DataType::UInt32 => 2_u8.encode(writer).await?,
            DataType::UInt64 => 3_u8.encode(writer).await?,
            DataType::Int8 => 4_u8.encode(writer).await?,
            DataType::Int16 => 5_u8.encode(writer).await?,
            DataType::Int32 => 6_u8.encode(writer).await?,
            DataType::Int64 => 7_u8.encode(writer).await?,
            DataType::String => 8_u8.encode(writer).await?,
            DataType::Boolean => 9_u8.encode(writer).await?,
            DataType::Bytes => 10_u8.encode(writer).await?,
            DataType::Float32 => 11_u8.encode(writer).await?,
            DataType::Float64 => 12_u8.encode(writer).await?,
            DataType::List(desc) => {
                13_u8.encode(writer).await?;
                desc.is_nullable.encode(writer).await?;
                match desc.datatype {
                    DataType::UInt8 => 0_u8.encode(writer).await?,
                    DataType::UInt16 => 1_u8.encode(writer).await?,
                    DataType::UInt32 => 2_u8.encode(writer).await?,
                    DataType::UInt64 => 3_u8.encode(writer).await?,
                    DataType::Int8 => 4_u8.encode(writer).await?,
                    DataType::Int16 => 5_u8.encode(writer).await?,
                    DataType::Int32 => 6_u8.encode(writer).await?,
                    DataType::Int64 => 7_u8.encode(writer).await?,
                    DataType::String => 8_u8.encode(writer).await?,
                    DataType::Boolean => 9_u8.encode(writer).await?,
                    DataType::Bytes => 10_u8.encode(writer).await?,
                    DataType::Float32 => 11_u8.encode(writer).await?,
                    DataType::Float64 => 12_u8.encode(writer).await?,
                    DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supporte yet"),
                }
            }
        };
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::String
            | DataType::Boolean
            | DataType::Bytes => 1,
            DataType::List(_) => 3,
        }
    }
}

impl Decode for DataType {
    type Error = fusio::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: fusio::SeqRead,
    {
        let tag = u8::decode(reader).await?;
        Ok(match tag {
            0 => DataType::UInt8,
            1 => DataType::UInt16,
            2 => DataType::UInt32,
            3 => DataType::UInt64,
            4 => DataType::Int8,
            5 => DataType::Int16,
            6 => DataType::Int32,
            7 => DataType::Int64,
            8 => DataType::String,
            9 => DataType::Boolean,
            10 => DataType::Bytes,
            11 => DataType::Float32,
            12 => DataType::Float64,
            13 => {
                let is_nullable = bool::decode(reader).await?;
                let inner_datatype = u8::decode(reader).await?;
                DataType::List(Arc::new(ValueDesc::new(
                    "".into(),
                    inner_datatype.into(),
                    is_nullable,
                )))
            }
            _ => panic!("invalid datatype tag"),
        })
    }
}

/// Cast the `Arc<dyn Array>` to the rust native Vec.
#[macro_export]
macro_rules! arrow_array_to_native {
    ($value:expr, $arrow_ty:expr, $native_ty:expr) => {
        let arrow_array = $value.as_any().downcast_ref::<$arrow_ty>().unwrap();
        arrow_array
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect::<$native_ty>()
    };
}

/// Cast the `Arc<dyn Array>` to the rust native Vec.
#[macro_export]
macro_rules! arrow_array_iter {
    ($value:expr, $arrow_ty:ty) => {
        $value.as_any().downcast_ref::<$arrow_ty>().unwrap().iter()
    };
}

/// Cast the `Arc<dyn Any>` to the value of given type.
#[macro_export]
macro_rules! cast_arc_value {
    ($value:expr, $type:ty) => {
        $value.as_ref().downcast_ref::<$type>().unwrap()
    };
}
