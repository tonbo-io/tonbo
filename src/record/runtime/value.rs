use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

use arrow::{
    array::{
        BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, GenericBinaryArray,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
        StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{DataType as ArrowDataType, Field},
};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, DecodeError, Encode};

use super::DataType;
use crate::record::{Key, KeyRef, TimeUnit, Timestamp, F32, F64};

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

#[derive(Clone)]
pub struct Value {
    pub desc: ValueDesc,
    pub value: Arc<dyn Any + Send + Sync>,
}

impl Value {
    pub fn new(
        datatype: DataType,
        name: String,
        value: Arc<dyn Any + Send + Sync>,
        is_nullable: bool,
    ) -> Self {
        Self {
            desc: ValueDesc::new(name, datatype, is_nullable),
            value,
        }
    }

    pub fn datatype(&self) -> DataType {
        self.desc.datatype
    }

    pub fn is_nullable(&self) -> bool {
        self.desc.is_nullable
    }

    pub fn name(&self) -> String {
        self.desc.name.clone()
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

macro_rules! implement_col {
    ([], $({$Type:ty, $DataType:pat}), *) => {
        impl Ord for Value {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                match self.datatype() {
                    $(
                        $DataType => self
                            .value
                            .downcast_ref::<$Type>()
                            .cmp(&other.value.downcast_ref::<$Type>()),
                    )*
                }
            }
        }

        impl PartialEq for Value {
            fn eq(&self, other: &Self) -> bool {
                self.name() == other.name()
                && self.datatype() == other.datatype()
                && self.is_nullable() == other.is_nullable()
                && match self.datatype() {
                        $(
                            $DataType => {
                                if let Some(v) = self
                                    .value
                                    .downcast_ref::<$Type>() {
                                        v.eq(other.value.downcast_ref::<$Type>().unwrap())
                                } else {
                                    self.value
                                        .downcast_ref::<Option<$Type>>()
                                        .unwrap()
                                        .eq(other.value.downcast_ref::<Option<$Type>>().unwrap())
                                }
                            }
                        )*
                    }
                }
        }

        impl Hash for Value {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self.datatype() {
                    $(
                        $DataType => self.value.downcast_ref::<$Type>().hash(state),
                    )*
                }
            }
        }

        impl Debug for Value {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut debug_struct = f.debug_struct("Value");
                debug_struct.field("name", &self.name());
                match self.datatype() {
                    $(
                        $DataType => {
                            debug_struct.field("datatype", &stringify!($Type));
                            if let Some(value) = self.value.as_ref().downcast_ref::<$Type>() {
                                debug_struct.field("value", value);
                            } else {
                                debug_struct.field(
                                    "value",
                                    self.value.as_ref().downcast_ref::<Option<$Type>>().unwrap(),
                                );
                            }
                        }
                    )*
                }
                debug_struct.field("nullable", &self.is_nullable()).finish()
            }
        }


        impl Value {
                /// return the none value of tonbo type
            pub(crate) fn with_none_value(datatype: DataType, name: String, is_nullable: bool) -> Self {
                match datatype {
                    $(
                        $DataType => Self::new(datatype, name, Arc::<Option<$Type>>::new(None), is_nullable),
                    )*
                }
            }
        }
    };
}

macro_rules! implement_key_col {
    (
        { $( { $primitive_ty:ty, $primitive_pat:pat, $primitive_array_ty:ty}), * $(,)? },
        { $( { $native_ty:ty, $native_pat:pat, $native_array_ty:ty}), * $(,)? },
        { $( { $wrapped_ty:ty, $wrapped_pat:pat, $wrapped_array_ty:ty, $value_fn:ident} ),* }
    )
     => {
        impl Key for Value {
            type Ref<'a> = Value;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                self.clone()
            }

            fn to_arrow_datum(&self) -> Arc<dyn arrow::array::Datum> {
                match self.datatype() {
                    $(
                        $primitive_pat => Arc::new(<$primitive_array_ty>::new_scalar(
                            *self
                                .value
                                .as_ref()
                                .downcast_ref::<$primitive_ty>()
                                .expect(stringify!("unexpected datatype, expected " $primitive_ty))
                        )),
                    )*
                    $(
                        $native_pat => Arc::new(<$native_array_ty>::new_scalar(
                            self
                                .value
                                .as_ref()
                                .downcast_ref::<$native_ty>()
                                .expect(stringify!("unexpected datatype, expected " $native_ty))
                        )),
                    )*
                    $(
                        $wrapped_pat => Arc::new(<$wrapped_array_ty>::new_scalar(
                            self
                                .value
                                .as_ref()
                                .downcast_ref::<$wrapped_ty>()
                                .expect(stringify!("unexpected datatype, expected " $ty))
                                .$value_fn()
                        )),
                    )*
                }
            }
        }
    }
}

impl<'r> KeyRef<'r> for Value {
    type Key = Value;

    fn to_key(self) -> Self::Key {
        self
    }
}

macro_rules! implement_decode_col {
    ([], $({$Type:ty, $DataType:pat}), *) => {
        impl Decode for Value {
            type Error = fusio::Error;

            async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
            where
                R: SeqRead,
            {
                let tag = u8::decode(reader).await?;
                let datatype = Self::tag_to_datatype(tag);
                let is_nullable = bool::decode(reader).await?;
                let is_some = !bool::decode(reader).await?;
                let value =
                    match datatype {
                        $(
                            $DataType => match is_some {
                                true => Arc::new(Option::<$Type>::decode(reader).await.map_err(
                                    |err| match err {
                                        DecodeError::Io(error) => fusio::Error::Io(error),
                                        DecodeError::Fusio(error) => error,
                                        DecodeError::Inner(error) => fusio::Error::Other(Box::new(error)),
                                    },
                                )?) as Arc<dyn Any + Send + Sync>,
                                false => Arc::new(<$Type>::decode(reader).await?) as Arc<dyn Any + Send + Sync>,
                            },
                        )*
                    };
                let name = String::decode(reader).await?;
                Ok(Value::new(
                    datatype,
                    name,
                    value,
                    is_nullable,
                ))
            }
        }
    }
}

macro_rules! implement_encode_col {
    ([], $({$Type:ty, $DataType:pat}), *) => {
        impl Encode for Value {
            type Error = fusio::Error;

            async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
            where
                W: Write,
            {
                Self::tag(self.datatype()).encode(writer).await?;
                self.is_nullable().encode(writer).await?;
                match self.datatype() {
                        $(
                            $DataType => {
                                if let Some(value) = self.value.as_ref().downcast_ref::<$Type>() {
                                    true.encode(writer).await?;
                                    value.encode(writer).await?
                                } else {
                                    false.encode(writer).await?;
                                    self.value
                                        .as_ref()
                                        .downcast_ref::<Option<$Type>>()
                                        .unwrap()
                                        .encode(writer)
                                        .await
                                        .map_err(|err| fusio::Error::Other(Box::new(err)))?;
                                }
                            }
                        )*
                };
                self.desc.name.encode(writer).await?;
                Ok(())
            }

            fn size(&self) -> usize {
                3 + self.desc.name.size() + match self.desc.datatype {
                    $(
                        $DataType => {
                            if let Some(value) = self.value.as_ref().downcast_ref::<$Type>() {
                                value.size()
                            } else {
                                self.value
                                    .as_ref()
                                    .downcast_ref::<Option<$Type>>()
                                    .unwrap()
                                    .size()
                            }
                        }
                    )*
                }
            }
        }
    }
}

impl Value {
    fn tag(datatype: DataType) -> u8 {
        match datatype {
            DataType::UInt8 => 0,
            DataType::UInt16 => 1,
            DataType::UInt32 => 2,
            DataType::UInt64 => 3,
            DataType::Int8 => 4,
            DataType::Int16 => 5,
            DataType::Int32 => 6,
            DataType::Int64 => 7,
            DataType::String => 8,
            DataType::Boolean => 9,
            DataType::Bytes => 10,
            DataType::Float32 => 11,
            DataType::Float64 => 12,
            DataType::Timestamp(TimeUnit::Second) => 13,
            DataType::Timestamp(TimeUnit::Millisecond) => 14,
            DataType::Timestamp(TimeUnit::Microsecond) => 15,
            DataType::Timestamp(TimeUnit::Nanosecond) => 16,
        }
    }

    fn tag_to_datatype(tag: u8) -> DataType {
        match tag {
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
            13 => DataType::Timestamp(TimeUnit::Second),
            14 => DataType::Timestamp(TimeUnit::Millisecond),
            15 => DataType::Timestamp(TimeUnit::Millisecond),
            16 => DataType::Timestamp(TimeUnit::Nanosecond),
            _ => panic!("invalid datatype tag"),
        }
    }
}

impl From<&ValueDesc> for Field {
    fn from(col: &ValueDesc) -> Self {
        match col.datatype {
            DataType::UInt8 => Field::new(&col.name, ArrowDataType::UInt8, col.is_nullable),
            DataType::UInt16 => Field::new(&col.name, ArrowDataType::UInt16, col.is_nullable),
            DataType::UInt32 => Field::new(&col.name, ArrowDataType::UInt32, col.is_nullable),
            DataType::UInt64 => Field::new(&col.name, ArrowDataType::UInt64, col.is_nullable),
            DataType::Int8 => Field::new(&col.name, ArrowDataType::Int8, col.is_nullable),
            DataType::Int16 => Field::new(&col.name, ArrowDataType::Int16, col.is_nullable),
            DataType::Int32 => Field::new(&col.name, ArrowDataType::Int32, col.is_nullable),
            DataType::Int64 => Field::new(&col.name, ArrowDataType::Int64, col.is_nullable),
            DataType::Float32 => Field::new(&col.name, ArrowDataType::Float32, col.is_nullable),
            DataType::Float64 => Field::new(&col.name, ArrowDataType::Float64, col.is_nullable),
            DataType::String => Field::new(&col.name, ArrowDataType::Utf8, col.is_nullable),
            DataType::Boolean => Field::new(&col.name, ArrowDataType::Boolean, col.is_nullable),
            DataType::Bytes => Field::new(&col.name, ArrowDataType::Binary, col.is_nullable),
            DataType::Timestamp(unit) => Field::new(
                &col.name,
                ArrowDataType::Timestamp(unit.into(), None),
                col.is_nullable,
            ),
        }
    }
}

macro_rules! for_datatype {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
                { u8, DataType::UInt8 },
                { u16, DataType::UInt16 },
                { u32, DataType::UInt32 },
                { u64, DataType::UInt64 },
                { i8, DataType::Int8 },
                { i16, DataType::Int16 },
                { i32, DataType::Int32 },
                { i64, DataType::Int64 },
                { F32, DataType::Float32 },
                { F64, DataType::Float64 },
                { String, DataType::String },
                { bool, DataType::Boolean },
                { Vec<u8>, DataType::Bytes },
                { Timestamp, DataType::Timestamp(TimeUnit::Second) },
                { Timestamp, DataType::Timestamp(TimeUnit::Millisecond) },
                { Timestamp, DataType::Timestamp(TimeUnit::Microsecond) },
                { Timestamp, DataType::Timestamp(TimeUnit::Nanosecond) }

        }
    };
}

implement_key_col!(
    {
        { u8, DataType::UInt8, UInt8Array }, { u16, DataType::UInt16, UInt16Array }, { u32, DataType::UInt32, UInt32Array }, { u64, DataType::UInt64, UInt64Array },
        { i8, DataType::Int8, Int8Array }, { i16, DataType::Int16, Int16Array }, { i32, DataType::Int32, Int32Array }, { i64, DataType::Int64, Int64Array },
        { bool, DataType::Boolean, BooleanArray }
    },
    {
        // other native type
        { Vec<u8>, DataType::Bytes, GenericBinaryArray<i32> },
        { String, DataType::String, StringArray }
    },
    {
        // wrapped type
        { F32, DataType::Float32, Float32Array, value },
        { F64, DataType::Float64, Float64Array, value },
        { Timestamp, DataType::Timestamp(TimeUnit::Second), TimestampSecondArray, timestamp },
        { Timestamp, DataType::Timestamp(TimeUnit::Millisecond), TimestampMillisecondArray, timestamp_millis },
        { Timestamp, DataType::Timestamp(TimeUnit::Microsecond), TimestampMicrosecondArray, timestamp_micros },
        { Timestamp, DataType::Timestamp(TimeUnit::Nanosecond), TimestampNanosecondArray, timestamp_nanos }

    }
);
for_datatype! { implement_col }
for_datatype! { implement_decode_col }
for_datatype! { implement_encode_col }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Value;
    use crate::record::{DataType, TimeUnit, Timestamp};

    #[test]
    fn test_value_eq() {
        {
            let value1 = Value::new(
                DataType::UInt64,
                "uint64".to_string(),
                Arc::new(123_u64),
                false,
            );
            let value2 = Value::new(
                DataType::UInt64,
                "uint64".to_string(),
                Arc::new(123_u64),
                false,
            );
            let value3 = Value::new(
                DataType::UInt64,
                "uint64".to_string(),
                Arc::new(124_u64),
                false,
            );
            assert_eq!(value1, value2);
            assert_ne!(value1, value3);
        }
        {
            let value1 = Value::new(
                DataType::UInt64,
                "uint64".to_string(),
                Arc::new(Some(123_u64)),
                true,
            );
            let value2 = Value::new(
                DataType::UInt64,
                "uint64".to_string(),
                Arc::new(Some(123_u64)),
                true,
            );
            let value3 = Value::new(
                DataType::UInt64,
                "uint64".to_string(),
                Arc::new(Some(124_u64)),
                true,
            );
            assert_eq!(value1, value2);
            assert_ne!(value1, value3);
        }
        {
            let value1 = Value::new(
                DataType::Timestamp(TimeUnit::Millisecond),
                "ts".to_string(),
                Arc::new(Some(Timestamp::new_millis(1717507203412))),
                true,
            );
            let value2 = Value::new(
                DataType::Timestamp(TimeUnit::Millisecond),
                "ts".to_string(),
                Arc::new(Some(Timestamp::new_millis(1717507203412))),
                true,
            );
            let value3 = Value::new(
                DataType::Timestamp(TimeUnit::Millisecond),
                "ts".to_string(),
                Arc::new(Some(Timestamp::new_millis(2717507203412))),
                true,
            );
            assert_eq!(value1, value2);
            assert_ne!(value1, value3);
        }
    }
}
