use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

use arrow::{
    array::{Int16Array, Int32Array, Int8Array},
    datatypes::{DataType, Field},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::Datatype;
use crate::{
    record::{Key, KeyRef},
    serdes::{Decode, Encode},
};

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub datatype: Datatype,
    pub is_nullable: bool,
    pub name: String,
}

impl ColumnDesc {
    pub fn new(name: String, datatype: Datatype, is_nullable: bool) -> Self {
        Self {
            name,
            datatype,
            is_nullable,
        }
    }
}

#[derive(Clone)]
pub struct Column {
    pub datatype: Datatype,
    pub value: Arc<dyn Any>,
    pub is_nullable: bool,
    pub name: String,
}

unsafe impl Send for Column {}
unsafe impl Sync for Column {}

impl Column {
    pub fn new(datatype: Datatype, name: String, value: Arc<dyn Any>, is_nullable: bool) -> Self {
        Self {
            datatype,
            name,
            value,
            is_nullable,
        }
    }

    pub fn value(&self) -> Arc<dyn Any> {
        // TODO: Option case
        match self.datatype {
            Datatype::Int8 if !self.is_nullable => {
                Arc::new(*self.value.downcast_ref::<i8>().unwrap())
            }
            Datatype::Int16 if !self.is_nullable => {
                Arc::new(*self.value.downcast_ref::<i16>().unwrap())
            }
            Datatype::Int32 if !self.is_nullable => {
                Arc::new(*self.value.downcast_ref::<i32>().unwrap())
            }
            Datatype::Int8 => Arc::new(*self.value.downcast_ref::<i8>().unwrap()),
            Datatype::Int16 => Arc::new(*self.value.downcast_ref::<i16>().unwrap()),
            Datatype::Int32 => Arc::new(*self.value.downcast_ref::<i32>().unwrap()),
        }
    }

    pub fn with_none_value(datatype: Datatype, name: String, is_nullable: bool) -> Self {
        match datatype {
            Datatype::Int8 => Self::new(datatype, name, Arc::<Option<i8>>::new(None), is_nullable),
            Datatype::Int16 => {
                Self::new(datatype, name, Arc::<Option<i16>>::new(None), is_nullable)
            }
            Datatype::Int32 => {
                Self::new(datatype, name, Arc::<Option<i32>>::new(None), is_nullable)
            }
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.datatype {
            Datatype::Int8 => self
                .value
                .downcast_ref::<i8>()
                .cmp(&other.value.downcast_ref::<i8>()),
            Datatype::Int16 => self
                .value
                .downcast_ref::<i16>()
                .cmp(&other.value.downcast_ref::<i16>()),
            Datatype::Int32 => self
                .value
                .downcast_ref::<i32>()
                .cmp(&other.value.downcast_ref::<i32>()),
        }
    }
}

impl Eq for Column {}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        match self.datatype {
            Datatype::Int8 => self
                .value
                .downcast_ref::<i8>()
                .eq(&other.value.downcast_ref::<i8>()),
            Datatype::Int16 => self
                .value
                .downcast_ref::<i16>()
                .eq(&other.value.downcast_ref::<i16>()),
            Datatype::Int32 => self
                .value
                .downcast_ref::<i32>()
                .eq(&other.value.downcast_ref::<i32>()),
        }
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("Column");
        match self.datatype {
            Datatype::Int8 => {
                debug_struct.field("datatype", &"i8".to_string());
                if let Some(value) = self.value.as_ref().downcast_ref::<i8>() {
                    debug_struct.field("value", value);
                } else {
                    debug_struct.field(
                        "value",
                        self.value.as_ref().downcast_ref::<Option<i8>>().unwrap(),
                    );
                }
            }
            Datatype::Int16 => {
                debug_struct.field("datatype", &"i16".to_string());
                if let Some(value) = self.value.as_ref().downcast_ref::<i16>() {
                    debug_struct.field("value", value);
                } else {
                    debug_struct.field(
                        "value",
                        self.value.as_ref().downcast_ref::<Option<i16>>().unwrap(),
                    );
                }
            }
            Datatype::Int32 => {
                debug_struct.field("datatype", &"i32".to_string());
                if let Some(value) = self.value.as_ref().downcast_ref::<i32>() {
                    debug_struct.field("value", value);
                } else {
                    debug_struct.field(
                        "value",
                        self.value.as_ref().downcast_ref::<Option<i32>>().unwrap(),
                    );
                }
            }
        }
        debug_struct.field("nullable", &self.is_nullable).finish()
    }
}

impl Hash for Column {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self.datatype {
            Datatype::Int8 => self.value.downcast_ref::<i8>().hash(state),
            Datatype::Int16 => self.value.downcast_ref::<i16>().hash(state),
            Datatype::Int32 => self.value.downcast_ref::<i32>().hash(state),
        }
    }
}

impl Key for Column {
    type Ref<'a> = Column;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self.clone()
    }

    fn to_arrow_datum(&self) -> Arc<dyn arrow::array::Datum> {
        match self.datatype {
            Datatype::Int8 => Arc::new(Int8Array::new_scalar(
                *self
                    .value
                    .as_ref()
                    .downcast_ref::<i8>()
                    .expect("unexpected datatype, expected: i8"),
            )),
            Datatype::Int16 => Arc::new(Int16Array::new_scalar(
                *self
                    .value
                    .as_ref()
                    .downcast_ref::<i16>()
                    .expect("unexpected datatype, expected: i16"),
            )),
            Datatype::Int32 => Arc::new(Int32Array::new_scalar(
                *self
                    .value
                    .as_ref()
                    .downcast_ref::<i32>()
                    .expect("unexpected datatype, expected: i32"),
            )),
        }
    }
}

impl<'r> KeyRef<'r> for Column {
    type Key = Column;

    fn to_key(self) -> Self::Key {
        self
    }
}

impl Decode for Column {
    type Error = std::io::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut tag = [0];
        reader.read_exact(&mut tag).await?;
        let datatype = Self::tag_to_datatype(tag[0]);
        let is_nullable = bool::decode(reader).await?;
        match datatype {
            Datatype::Int8 => {
                let v = reader.read_i8().await?;
                Ok(Column {
                    datatype,
                    is_nullable,
                    name: "".to_owned(),
                    value: Arc::new(v),
                })
            }
            Datatype::Int16 => {
                let v = reader.read_i16().await?;
                Ok(Column {
                    datatype,
                    is_nullable,
                    name: "".to_owned(),
                    value: Arc::new(v),
                })
            }
            Datatype::Int32 => {
                let v = reader.read_i32().await?;
                Ok(Column {
                    datatype,
                    is_nullable,
                    name: "".to_owned(),
                    value: Arc::new(v),
                })
            }
        }
    }
}

impl Encode for Column {
    type Error = std::io::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: tokio::io::AsyncWrite + Unpin + Send,
    {
        writer.write_all(&[Self::tag(self.datatype)]).await?;
        self.is_nullable.encode(writer).await?;
        match self.datatype {
            Datatype::Int8 => {
                if let Some(value) = self.value.as_ref().downcast_ref::<i8>() {
                    value.encode(writer).await
                } else {
                    self.value
                        .as_ref()
                        .downcast_ref::<Option<i8>>()
                        .unwrap()
                        .encode(writer)
                        .await
                        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
                }
            }
            Datatype::Int16 => {
                if let Some(value) = self.value.as_ref().downcast_ref::<i16>() {
                    value.encode(writer).await
                } else {
                    self.value
                        .as_ref()
                        .downcast_ref::<Option<i16>>()
                        .unwrap()
                        .encode(writer)
                        .await
                        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
                }
            }
            Datatype::Int32 => {
                if let Some(value) = self.value.as_ref().downcast_ref::<i32>() {
                    value.encode(writer).await
                } else {
                    self.value
                        .as_ref()
                        .downcast_ref::<Option<i32>>()
                        .unwrap()
                        .encode(writer)
                        .await
                        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
                }
            }
        }
    }

    fn size(&self) -> usize {
        2 + match self.datatype {
            Datatype::Int8 => {
                if let Some(value) = self.value.as_ref().downcast_ref::<i8>() {
                    value.size()
                } else {
                    self.value
                        .as_ref()
                        .downcast_ref::<Option<i8>>()
                        .unwrap()
                        .size()
                }
            }
            Datatype::Int16 => {
                if let Some(value) = self.value.as_ref().downcast_ref::<i16>() {
                    value.size()
                } else {
                    self.value
                        .as_ref()
                        .downcast_ref::<Option<i16>>()
                        .unwrap()
                        .size()
                }
            }
            Datatype::Int32 => {
                if let Some(value) = self.value.as_ref().downcast_ref::<i32>() {
                    value.size()
                } else {
                    self.value
                        .as_ref()
                        .downcast_ref::<Option<i32>>()
                        .unwrap()
                        .size()
                }
            }
        }
    }
}

impl Column {
    fn tag(datatype: Datatype) -> u8 {
        match datatype {
            Datatype::Int8 => 0,
            Datatype::Int16 => 1,
            Datatype::Int32 => 2,
        }
    }

    fn tag_to_datatype(tag: u8) -> Datatype {
        match tag {
            0 => Datatype::Int8,
            1 => Datatype::Int16,
            2 => Datatype::Int32,
            _ => panic!("invalid datatype tag"),
        }
    }
}

impl From<&Column> for Field {
    fn from(col: &Column) -> Self {
        match col.datatype {
            Datatype::Int8 => Field::new(&col.name, DataType::Int8, col.is_nullable),
            Datatype::Int16 => Field::new(&col.name, DataType::Int16, col.is_nullable),
            Datatype::Int32 => Field::new(&col.name, DataType::Int32, col.is_nullable),
        }
    }
}
