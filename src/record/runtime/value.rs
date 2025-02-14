use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

use arrow::{
    array::{
        BooleanArray, GenericBinaryArray, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType as ArrowDataType, Field},
};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, DecodeError, Encode};

use super::DataType;
use crate::record::{Key, KeyRef};

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
            DataType::String => ArrowDataType::Utf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Bytes => ArrowDataType::Binary,
        };
        Field::new(&self.name, arrow_type, self.is_nullable)
    }
}

#[derive(Clone)]
pub struct Value {
    pub datatype: DataType,
    pub is_nullable: bool,
    pub name: String,

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
            datatype,
            name,
            value,
            is_nullable,
        }
    }

    pub(crate) fn with_none_value(datatype: DataType, name: String, is_nullable: bool) -> Self {
        match datatype {
            DataType::UInt8 => Self::new(datatype, name, Arc::<Option<u8>>::new(None), is_nullable),
            DataType::UInt16 => {
                Self::new(datatype, name, Arc::<Option<u16>>::new(None), is_nullable)
            }
            DataType::UInt32 => {
                Self::new(datatype, name, Arc::<Option<u32>>::new(None), is_nullable)
            }
            DataType::UInt64 => {
                Self::new(datatype, name, Arc::<Option<u64>>::new(None), is_nullable)
            }
            DataType::Int8 => Self::new(datatype, name, Arc::<Option<i8>>::new(None), is_nullable),
            DataType::Int16 => {
                Self::new(datatype, name, Arc::<Option<i16>>::new(None), is_nullable)
            }
            DataType::Int32 => {
                Self::new(datatype, name, Arc::<Option<i32>>::new(None), is_nullable)
            }
            DataType::Int64 => {
                Self::new(datatype, name, Arc::<Option<i64>>::new(None), is_nullable)
            }
            DataType::String => Self::new(
                datatype,
                name,
                Arc::<Option<String>>::new(None),
                is_nullable,
            ),
            DataType::Boolean => {
                Self::new(datatype, name, Arc::<Option<bool>>::new(None), is_nullable)
            }
            DataType::Bytes => Self::new(
                datatype,
                name,
                Arc::<Option<Vec<u8>>>::new(None),
                is_nullable,
            ),
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

macro_rules! implement_col {
    ([], $({$Type:ty, $DataType:ident}), *) => {
        impl Ord for Value {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                match self.datatype {
                    $(
                        DataType::$DataType => self
                            .value
                            .downcast_ref::<$Type>()
                            .cmp(&other.value.downcast_ref::<$Type>()),
                    )*
                }
            }
        }

        impl PartialEq for Value {
            fn eq(&self, other: &Self) -> bool {
                self.datatype == other.datatype
                    && self.is_nullable == other.is_nullable
                    && match self.datatype {
                        $(
                            DataType::$DataType => self
                                .value
                                .downcast_ref::<$Type>()
                                .eq(&other.value.downcast_ref::<$Type>()),
                        )*
                    }
            }
        }

        impl Hash for Value {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self.datatype {
                    $(
                        DataType::$DataType => self.value.downcast_ref::<$Type>().hash(state),
                    )*
                }
            }
        }

        impl Debug for Value {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut debug_struct = f.debug_struct("Value");
                match self.datatype {
                    $(
                        DataType::$DataType => {
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
                debug_struct.field("nullable", &self.is_nullable).finish()
            }
        }

    };
}

macro_rules! implement_key_col {
    ($({$Type:ident, $DataType:ident, $Array:ident}), *) => {
        impl Key for Value {
            type Ref<'a> = Value;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                self.clone()
            }

            fn to_arrow_datum(&self) -> Arc<dyn arrow::array::Datum> {
                match self.datatype {
                    $(
                        DataType::$DataType => Arc::new($Array::new_scalar(
                            *self
                                .value
                                .as_ref()
                                .downcast_ref::<$Type>()
                                .expect(stringify!("unexpected datatype, expected " $Type))
                        )),
                    )*
                    DataType::String => Arc::new(StringArray::new_scalar(
                        self
                            .value
                            .as_ref()
                            .downcast_ref::<String>()
                                .expect("unexpected datatype, expected String"),
                    )),
                    DataType::Boolean => Arc::new(BooleanArray::new_scalar(
                        *self
                            .value
                            .as_ref()
                            .downcast_ref::<bool>()
                            .expect("unexpected datatype, expected bool"),
                    )),
                    DataType::Bytes => Arc::new(GenericBinaryArray::<i32>::new_scalar(
                        self
                            .value
                            .as_ref()
                            .downcast_ref::<Vec<u8>>()
                            .expect("unexpected datatype, expected bytes"),
                    )),
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
    ([], $({$Type:ty, $DataType:ident}), *) => {
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
                            DataType::$DataType => match is_some {
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
                Ok(Value {
                    datatype,
                    is_nullable,
                    name,
                    value,
                })
            }
        }
    }
}

macro_rules! implement_encode_col {
    ([], $({$Type:ty, $DataType:ident}), *) => {
        impl Encode for Value {
            type Error = fusio::Error;

            async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
            where
                W: Write,
            {
                Self::tag(self.datatype).encode(writer).await?;
                self.is_nullable.encode(writer).await?;
                match self.datatype {
                        $(
                            DataType::$DataType => {
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
                self.name.encode(writer).await?;
                Ok(())
            }

            fn size(&self) -> usize {
                3 + self.name.size() + match self.datatype {
                    $(
                        DataType::$DataType => {
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
            DataType::String => Field::new(&col.name, ArrowDataType::Utf8, col.is_nullable),
            DataType::Boolean => Field::new(&col.name, ArrowDataType::Boolean, col.is_nullable),
            DataType::Bytes => Field::new(&col.name, ArrowDataType::Binary, col.is_nullable),
        }
    }
}

macro_rules! for_datatype {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
                { u8, UInt8 },
                { u16, UInt16 },
                { u32, UInt32 },
                { u64, UInt64 },
                { i8, Int8 },
                { i16, Int16 },
                { i32, Int32 },
                { i64, Int64 },
                { String, String },
                { bool, Boolean },
                { Vec<u8>, Bytes }
        }
    };
}

implement_key_col!(
    { u8, UInt8, UInt8Array }, { u16, UInt16, UInt16Array }, { u32, UInt32, UInt32Array }, { u64, UInt64, UInt64Array },
    { i8, Int8, Int8Array }, { i16, Int16, Int16Array }, { i32, Int32, Int32Array }, { i64, Int64, Int64Array }
);
for_datatype! { implement_col }
for_datatype! { implement_decode_col }
for_datatype! { implement_encode_col }
