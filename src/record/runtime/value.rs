use core::hash::{Hash, Hasher};
use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{
    array::{
        Array, BooleanArray, Float32Array, Float64Array, GenericBinaryArray, Int16Array,
        Int32Array, Int64Array, Int8Array, ListArray, StringArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType as ArrowDataType, Field, FieldRef},
};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, DecodeError, Encode};

use super::DataType;
use crate::{
    cast_arc_value,
    record::{Key, KeyRef, F32, F64},
};

#[derive(Debug, Clone)]
pub struct ValueDesc {
    pub datatype: DataType,
    pub is_nullable: bool,
    pub name: String,
}

impl PartialEq for ValueDesc {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.datatype == other.datatype
            && self.is_nullable == other.is_nullable
    }
}
impl Eq for ValueDesc {}

impl PartialOrd for ValueDesc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueDesc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.datatype.cmp(&other.datatype))
            .then_with(|| self.is_nullable.cmp(&other.is_nullable))
    }
}

impl Hash for ValueDesc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.datatype.hash(state);
        self.is_nullable.hash(state);
    }
}

impl From<&Field> for ValueDesc {
    fn from(field: &Field) -> Self {
        Self {
            datatype: field.data_type().into(),
            is_nullable: field.is_nullable(),
            name: field.name().to_owned(),
        }
    }
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
        let arrow_type = match &self.datatype {
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
            DataType::List(desc) => {
                let array_ty = match &desc.datatype {
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
                    DataType::List(_) => {
                        unimplemented!("`Vec<Vec<T>> is not supported now")
                    }
                };
                ArrowDataType::List(FieldRef::new(Field::new(
                    &desc.name,
                    array_ty,
                    desc.is_nullable,
                )))
            }
        };
        Field::new(&self.name, arrow_type, self.is_nullable)
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

    pub(crate) fn with_none_value(datatype: DataType, name: String, is_nullable: bool) -> Self {
        match &datatype {
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
            DataType::Float32 => {
                Self::new(datatype, name, Arc::<Option<f32>>::new(None), is_nullable)
            }
            DataType::Float64 => {
                Self::new(datatype, name, Arc::<Option<f64>>::new(None), is_nullable)
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
            DataType::List(desc) => match &desc.datatype {
                DataType::UInt8 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<u8>>>::new(None),
                    is_nullable,
                ),
                DataType::UInt16 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<u16>>>::new(None),
                    is_nullable,
                ),
                DataType::UInt32 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<u32>>>::new(None),
                    is_nullable,
                ),
                DataType::UInt64 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<u64>>>::new(None),
                    is_nullable,
                ),
                DataType::Int8 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<i8>>>::new(None),
                    is_nullable,
                ),
                DataType::Int16 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<i16>>>::new(None),
                    is_nullable,
                ),
                DataType::Int32 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<i32>>>::new(None),
                    is_nullable,
                ),
                DataType::Int64 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<i64>>>::new(None),
                    is_nullable,
                ),
                DataType::Float32 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<f32>>>::new(None),
                    is_nullable,
                ),
                DataType::Float64 => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<f64>>>::new(None),
                    is_nullable,
                ),
                DataType::String => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<String>>>::new(None),
                    is_nullable,
                ),
                DataType::Boolean => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<bool>>>::new(None),
                    is_nullable,
                ),
                DataType::Bytes => Self::new(
                    datatype,
                    name,
                    Arc::<Option<Vec<Vec<u8>>>>::new(None),
                    is_nullable,
                ),
                DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supporte yet"),
            },
        }
    }

    pub fn datatype(&self) -> &DataType {
        &self.desc.datatype
    }

    pub fn is_nullable(&self) -> bool {
        self.desc.is_nullable
    }

    pub fn name(&self) -> String {
        self.desc.name.clone()
    }
}

/// transform `Arc<T>` to `Arc<Option<T>>`
pub(crate) fn wrapped_value(
    datatype: &DataType,
    value: &Arc<dyn Any + Send + Sync>,
) -> Arc<dyn Any + Send + Sync> {
    match datatype {
        DataType::UInt8 => Arc::new(Some(*cast_arc_value!(value, u8))),
        DataType::UInt16 => Arc::new(Some(*cast_arc_value!(value, u16))),
        DataType::UInt32 => Arc::new(Some(*cast_arc_value!(value, u32))),
        DataType::UInt64 => Arc::new(Some(*cast_arc_value!(value, u64))),
        DataType::Int8 => Arc::new(Some(*cast_arc_value!(value, i8))),
        DataType::Int16 => Arc::new(Some(*cast_arc_value!(value, i16))),
        DataType::Int32 => Arc::new(Some(*cast_arc_value!(value, i32))),
        DataType::Int64 => Arc::new(Some(*cast_arc_value!(value, i64))),
        DataType::Float32 => Arc::new(Some(*cast_arc_value!(value, F32))),
        DataType::Float64 => Arc::new(Some(*cast_arc_value!(value, F64))),
        DataType::String => Arc::new(Some(cast_arc_value!(value, String).to_owned())),
        DataType::Boolean => Arc::new(Some(*cast_arc_value!(value, bool))),
        DataType::Bytes => Arc::new(Some(cast_arc_value!(value, Vec<u8>).to_owned())),
        DataType::List(desc) => match desc.datatype {
            DataType::UInt8 => Arc::new(Some(cast_arc_value!(value, Vec<u8>).to_owned())),
            DataType::UInt16 => Arc::new(Some(cast_arc_value!(value, Vec<u16>).to_owned())),
            DataType::UInt32 => Arc::new(Some(cast_arc_value!(value, Vec<u32>).to_owned())),
            DataType::UInt64 => Arc::new(Some(cast_arc_value!(value, Vec<u64>).to_owned())),
            DataType::Int8 => Arc::new(Some(cast_arc_value!(value, Vec<i8>).to_owned())),
            DataType::Int16 => Arc::new(Some(cast_arc_value!(value, Vec<i16>).to_owned())),
            DataType::Int32 => Arc::new(Some(cast_arc_value!(value, Vec<i32>).to_owned())),
            DataType::Int64 => Arc::new(Some(cast_arc_value!(value, Vec<i64>).to_owned())),
            DataType::Float32 => Arc::new(Some(cast_arc_value!(value, Vec<F32>).to_owned())),
            DataType::Float64 => Arc::new(Some(cast_arc_value!(value, Vec<F64>).to_owned())),
            DataType::String => Arc::new(Some(cast_arc_value!(value, Vec<String>).to_owned())),
            DataType::Boolean => Arc::new(Some(cast_arc_value!(value, Vec<bool>).to_owned())),
            DataType::Bytes => Arc::new(Some(cast_arc_value!(value, Vec<Vec<u8>>).to_owned())),
            DataType::List(_) => {
                unimplemented!("Vec<Vec<T>> is not supporte yet")
            }
        },
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
                match self.datatype() {
                    $(
                        DataType::$DataType => self
                            .value
                            .downcast_ref::<$Type>()
                            .cmp(&other.value.downcast_ref::<$Type>()),
                    )*
                    DataType::List(field) => {
                        match &field.datatype {
                            $(
                                DataType::$DataType => self
                                    .value
                                    .downcast_ref::<Vec<$Type>>()
                                    .cmp(&other.value.downcast_ref::<Vec<$Type>>()),
                            )*
                            DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                        }
                    }
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
                            DataType::$DataType => {
                                if let Some(v) = self
                                .value
                                .downcast_ref::<$Type>() {
                                    v.eq(other.value.downcast_ref::<$Type>().unwrap())
                                } else {
                                    self.value.downcast_ref::<Option<$Type>>().unwrap().eq(other.value.downcast_ref::<Option<$Type>>().unwrap())
                                }
                            }
                        )*
                        DataType::List(field) => {
                            match &field.datatype {
                                $(
                                    DataType::$DataType => {
                                        if let Some(v) = self
                                        .value
                                        .downcast_ref::<Vec<$Type>>() {
                                            v.eq(other.value.downcast_ref::<Vec<$Type>>().unwrap())
                                        } else {
                                            self.value
                                                .downcast_ref::<Option<Vec<$Type>>>()
                                                .expect(stringify!("unexpected datatype, can not convert to " Vec<$Type>))
                                                .eq(other.value.downcast_ref::<Option<Vec<$Type>>>()
                                                .expect(stringify!("unexpected datatype, can not convert to " Vec<$Type>)))
                                        }
                                        // self
                                        // .value
                                        // .downcast_ref::<Vec<$Type>>()
                                        // .eq(&other.value.downcast_ref::<Vec<$Type>>()),
                                    }
                                )*
                                DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                            }
                        }
                    }
            }
        }

        impl Hash for Value {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self.datatype() {
                    $(
                        DataType::$DataType => self.value.downcast_ref::<$Type>().hash(state),
                    )*
                    DataType::List(field) => {
                        match &field.datatype {
                            $(
                                DataType::$DataType => self.value.downcast_ref::<Vec<$Type>>().hash(state),
                            )*
                            DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                        }
                    }
                }
            }
        }

        impl Debug for Value {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut debug_struct = f.debug_struct("Value");
                debug_struct.field("name", &self.name());
                match self.datatype() {
                    $(
                        DataType::$DataType => {
                            debug_struct.field("datatype", &stringify!($Type));
                            if let Some(value) = self.value.as_ref().downcast_ref::<$Type>() {
                                debug_struct.field("value", value);
                            } else {
                                debug_struct.field(
                                    "value",
                                    self.value.as_ref().downcast_ref::<Option<$Type>>().expect("can not convert value to {}"),
                                );
                            }
                        }
                    )*
                    DataType::List(field) => {
                        match &field.datatype {
                            $(
                                DataType::$DataType => {
                                    debug_struct.field("datatype", &stringify!(Vec<$Type>));
                                    if let Some(value) = self.value.as_ref().downcast_ref::<Vec<$Type>>() {
                                        debug_struct.field("value", value);
                                    } else {
                                        debug_struct.field(
                                            "value",
                                            self.value.as_ref().downcast_ref::<Option<Vec<$Type>>>().unwrap(),
                                        );
                                    }
                                }
                            )*
                            DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                        }
                    }
                }
                debug_struct.field("nullable", &self.is_nullable()).finish()
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
                match self.datatype() {
                    $(
                        DataType::$DataType => Arc::new($Array::new_scalar(
                            *self
                                .value
                                .as_ref()
                                .downcast_ref::<$Type>()
                                .expect(stringify!("unexpected datatype, expected " $Type))
                        )),
                    )*
                    DataType::Float32 => Arc::new(Float32Array::new_scalar(
                        self
                            .value
                            .as_ref()
                            .downcast_ref::<F32>()
                                .expect("unexpected datatype, expected String").into(),
                    )),
                    DataType::Float64 => Arc::new(Float64Array::new_scalar(
                        self
                            .value
                            .as_ref()
                            .downcast_ref::<F64>()
                                .expect("unexpected datatype, expected String").into(),
                    )),
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
                    DataType::List(desc) => {
                        let arr: Arc<dyn Array> = match &desc.datatype {
                            $(
                                DataType::$DataType => {
                                    Arc::new($Array::from_iter_values(cast_arc_value!(self.value, Vec<$Type>).clone()))
                                }
                            )*
                            DataType::Float32 => {
                                Arc::new(Float32Array::from_iter_values(cast_arc_value!(self.value, Vec<f32>).clone()))
                            },
                            DataType::Float64 => {
                                Arc::new(Float64Array::from_iter_values(cast_arc_value!(self.value, Vec<f64>).clone()))
                            },
                            DataType::Boolean => {
                                Arc::new(BooleanArray::from(cast_arc_value!(self.value, Vec<bool>).clone()))
                            },
                            DataType::String => {
                                Arc::new(StringArray::from(cast_arc_value!(self.value, Vec<String>).clone()))
                            },
                            DataType::Bytes => {
                                Arc::new(GenericBinaryArray::<i32>::from_vec(vec![cast_arc_value!(self.value, Vec<u8>)]))
                            },
                            DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                        };
                        Arc::new(ListArray::new(
                            Arc::new(Field::new("", (&desc.datatype).into(), desc.is_nullable)),
                            OffsetBuffer::from_lengths([arr.len()]),
                            arr,
                            None
                        ))
                    },
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
                let datatype = DataType::decode(reader).await?;
                let is_nullable = bool::decode(reader).await?;
                let is_some = !bool::decode(reader).await?;
                let value =
                    match &datatype {
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
                        DataType::List(field) => {
                            match &field.datatype {
                                $(
                                    DataType::$DataType => match is_some {
                                        true => Arc::new(Option::<Vec<$Type>>::decode(reader).await.map_err(
                                            |err| match err {
                                                DecodeError::Io(error) => fusio::Error::Io(error),
                                                DecodeError::Fusio(error) => error,
                                                DecodeError::Inner(error) => fusio::Error::Other(Box::new(error)),
                                            },
                                        )?) as Arc<dyn Any + Send + Sync>,
                                        false => Arc::new(<Vec<$Type>>::decode(reader).await?) as Arc<dyn Any + Send + Sync>,
                                    },
                                )*
                                DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                            }
                        },
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
    ([], $({$Type:ty, $DataType:ident}), *) => {
        impl Encode for Value {
            type Error = fusio::Error;

            async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
            where
                W: Write,
            {
                self.datatype().encode(writer).await?;
                self.is_nullable().encode(writer).await?;
                match self.datatype() {
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
                        DataType::List(field) => {
                            match &field.datatype {
                                $(
                                    DataType::$DataType =>  {
                                        if let Some(value) = self.value.as_ref().downcast_ref::<Vec<$Type>>() {
                                            true.encode(writer).await?;
                                            value.encode(writer).await?
                                        } else {
                                            false.encode(writer).await?;
                                            self.value
                                                .as_ref()
                                                .downcast_ref::<Option<Vec<$Type>>>()
                                                .unwrap()
                                                .encode(writer)
                                                .await
                                                .map_err(|err| fusio::Error::Other(Box::new(err)))?;
                                        }
                                    }
                                )*
                                DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                            }
                        },
                };
                self.desc.name.encode(writer).await?;
                Ok(())
            }

            fn size(&self) -> usize {
                2 + self.desc.name.size() + self.datatype().size() + match self.datatype() {
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
                    DataType::List(field) => {
                        match &field.datatype {
                            $(
                                DataType::$DataType => {
                                    if let Some(value) = self.value.as_ref().downcast_ref::<Vec<$Type>>() {
                                        value.size()
                                    } else {
                                        self.value
                                            .as_ref()
                                            .downcast_ref::<Option<Vec<$Type>>>()
                                            .unwrap()
                                            .size()
                                    }
                                }
                            )*
                            DataType::List(_) => unimplemented!("Vec<Vec<T>> is not supported yet")
                        }
                    },
                }
            }
        }
    }
}

impl From<&ValueDesc> for Field {
    fn from(col: &ValueDesc) -> Self {
        col.arrow_field()
    }
}

#[macro_export]
macro_rules! value {
    ($name: expr, $ty: expr, $nullable: expr, $value: expr) => {{
        $crate::record::Value::new($ty, $name.into(), std::sync::Arc::new($value), $nullable)
    }};
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
                { F32, Float32 },
                { F64, Float64 },
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

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::{
        array::{AsArray, BooleanArray, PrimitiveArray, StringArray},
        datatypes::UInt64Type,
    };
    use fusio_log::{Decode, Encode};

    use crate::record::{DataType, Key, Value, ValueDesc};

    #[tokio::test]
    async fn test_encode_decode_list() {
        use tokio::io::AsyncSeekExt;
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt64, false))),
                "u64s".into(),
                Arc::new(vec![1_u64, 2, 3, 4]),
                false,
            );

            let mut source = vec![];
            let mut cursor = Cursor::new(&mut source);
            v.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Value::decode(&mut cursor).await.unwrap();
            assert_eq!(v, decoded);
        }
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt64, false))),
                "u64s".into(),
                Arc::new(Some(vec![1_u64, 2, 3, 4])),
                true,
            );

            let mut source = vec![];
            let mut cursor = Cursor::new(&mut source);
            v.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Value::decode(&mut cursor).await.unwrap();
            assert_eq!(v, decoded);
        }
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::Bytes, false))),
                "bytes".into(),
                Arc::new(Some(vec![
                    vec![1_u8, 2, 3, 4],
                    vec![72, 83, 94],
                    vec![112, 113, 124],
                ])),
                true,
            );

            let mut source = vec![];
            let mut cursor = Cursor::new(&mut source);
            v.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Value::decode(&mut cursor).await.unwrap();
            assert_eq!(v, decoded);
        }
    }

    #[test]
    fn test_key_ref_list() {
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt64, false))),
                "u64s".into(),
                Arc::new(vec![1_u64, 2, 3, 4]),
                false,
            );

            let vref = v.as_key_ref();
            assert_eq!(v, vref);
        }
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt64, true))),
                "u64s".into(),
                Arc::new(Some(vec![1_u64, 2, 3, 4])),
                false,
            );

            let vref = v.as_key_ref();
            assert_eq!(v, vref);
        }
    }

    #[test]
    fn test_key_list_datum() {
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::UInt64, false))),
                "u64s".into(),
                Arc::new(vec![1_u64, 2, 3, 4]),
                false,
            );
            let datum = v.to_arrow_datum();
            assert_eq!(
                datum
                    .get()
                    .0
                    .as_list::<i32>()
                    .value(0)
                    .as_primitive::<UInt64Type>(),
                &PrimitiveArray::from_iter([1_u64, 2, 3, 4])
            );
        }
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new("".into(), DataType::String, false))),
                "u64s".into(),
                Arc::new(vec![
                    "1_u64".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                ]),
                false,
            );
            let datum = v.to_arrow_datum();
            assert_eq!(
                datum.get().0.as_list::<i32>().value(0).as_string(),
                &StringArray::from(vec![
                    "1_u64".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string()
                ])
            );
        }
        {
            let v = Value::new(
                DataType::List(Arc::new(ValueDesc::new(
                    "".into(),
                    DataType::Boolean,
                    false,
                ))),
                "u64s".into(),
                Arc::new(vec![true, false, false, true]),
                false,
            );
            let datum = v.to_arrow_datum();
            assert_eq!(
                datum.get().0.as_list::<i32>().value(0).as_boolean(),
                &BooleanArray::from(vec![true, false, false, true])
            );
        }
    }
}
