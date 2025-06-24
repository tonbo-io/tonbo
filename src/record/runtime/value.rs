use std::fmt::Debug;

use arrow::datatypes::{DataType as ArrowDataType, Field};
use common::{datatype::DataType, TimeUnit};

#[derive(Debug, Clone)]
pub struct ValueDesc {
    pub datatype: DataType,
    pub is_nullable: bool,
    pub name: String,
}

impl ValueDesc {
    pub fn new(name: String, datatype: DataType, is_nullable: bool) -> Self {
        Self {
            name,
            datatype,
            is_nullable,
        }
    }

    #[allow(unused)]
    pub(crate) fn arrow_field(&self) -> Field {
        let arrow_type = match self.datatype {
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
            DataType::Timestamp(unit) => ArrowDataType::Timestamp(unit.into(), None),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second | TimeUnit::Millisecond => ArrowDataType::Time32(unit.into()),
                TimeUnit::Microsecond | TimeUnit::Nanosecond => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Second | TimeUnit::Millisecond => unreachable!(),
                TimeUnit::Microsecond | TimeUnit::Nanosecond => ArrowDataType::Time64(unit.into()),
            },
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::LargeBinary => ArrowDataType::LargeBinary,
            DataType::LargeString => ArrowDataType::LargeUtf8,
        };
        Field::new(&self.name, arrow_type, self.is_nullable)
    }
}

impl From<Field> for ValueDesc {
    fn from(field: Field) -> Self {
        let datatype = DataType::from(field.data_type());
        ValueDesc::new(field.name().to_owned(), datatype, field.is_nullable())
    }
}

impl From<&Field> for ValueDesc {
    fn from(field: &Field) -> Self {
        let datatype = DataType::from(field.data_type());
        ValueDesc::new(field.name().to_owned(), datatype, field.is_nullable())
    }
}
