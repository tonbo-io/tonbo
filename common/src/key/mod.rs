mod bool;
mod cast;
mod datetime;
mod list;
mod num;
mod pk;
mod str;
mod timestamp;

use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

pub use cast::*;
pub use datetime::*;
use fusio_log::{Decode, Encode};
pub use list::*;
pub use num::*;
pub use pk::*;
pub use str::*;
pub use timestamp::*;

use crate::datatype::DataType;

pub type ValueRef = Arc<dyn Value>;

pub trait Value: 'static + Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;

    fn data_type(&self) -> DataType;

    fn size_of(&self) -> usize;

    fn is_none(&self) -> bool;

    fn is_some(&self) -> bool;

    fn clone_arc(&self) -> ValueRef;
}

pub trait Key:
    'static + Value + Encode + Decode + Ord + Clone + Send + Sync + Hash + std::fmt::Debug
{
    type Ref<'r>: KeyRef<'r, Key = Self>
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    fn as_value(&self) -> &dyn Value;
}

pub trait KeyRef<'r>: Clone + Encode + Send + Sync + Ord + std::fmt::Debug {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(self) -> Self::Key;
}

impl Encode for dyn Value {
    /// Value layout
    /// ```ignore
    /// +-------------------------------------+
    /// | datatype(1) | is_option(1) |  value |
    /// +-------------------------------------+
    /// ```
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        let data_type = self.data_type();
        data_type.encode(writer).await?;
        if self.is_none() || self.is_some() {
            1u8.encode(writer).await?;
            match &data_type {
                DataType::UInt8 => self.as_u8_opt().encode(writer).await?,
                DataType::UInt16 => self.as_u16_opt().encode(writer).await?,
                DataType::UInt32 => self.as_u32_opt().encode(writer).await?,
                DataType::UInt64 => self.as_u64_opt().encode(writer).await?,
                DataType::Int8 => self.as_i8_opt().encode(writer).await?,
                DataType::Int16 => self.as_i16_opt().encode(writer).await?,
                DataType::Int32 => self.as_i32_opt().encode(writer).await?,
                DataType::Int64 => self.as_i64_opt().encode(writer).await?,
                DataType::String | DataType::LargeString => {
                    self.as_string_opt().encode(writer).await?
                }
                DataType::Boolean => self.as_boolean_opt().encode(writer).await?,
                DataType::Bytes | DataType::LargeBinary => {
                    self.as_bytes_opt().encode(writer).await?
                }
                DataType::Float32 => self.as_f32_opt().encode(writer).await?,
                DataType::Float64 => self.as_f64_opt().encode(writer).await?,
                DataType::Timestamp(_) => self.as_timestamp_opt().encode(writer).await?,
                DataType::Time32(_) => self.as_time32_opt().encode(writer).await?,
                DataType::Time64(_) => self.as_time64_opt().encode(writer).await?,
                DataType::Date32 => self.as_date32_opt().encode(writer).await?,
                DataType::Date64 => self.as_date64_opt().encode(writer).await?,
            };
        } else {
            0u8.encode(writer).await?;
            match &data_type {
                DataType::UInt8 => self.as_u8().encode(writer).await?,
                DataType::UInt16 => self.as_u16().encode(writer).await?,
                DataType::UInt32 => self.as_u32().encode(writer).await?,
                DataType::UInt64 => self.as_u64().encode(writer).await?,
                DataType::Int8 => self.as_i8().encode(writer).await?,
                DataType::Int16 => self.as_i16().encode(writer).await?,
                DataType::Int32 => self.as_i32().encode(writer).await?,
                DataType::Int64 => self.as_i64().encode(writer).await?,
                DataType::String | DataType::LargeString => self.as_string().encode(writer).await?,
                DataType::Boolean => self.as_boolean().encode(writer).await?,
                DataType::Bytes | DataType::LargeBinary => self.as_bytes().encode(writer).await?,
                DataType::Float32 => self.as_f32().encode(writer).await?,
                DataType::Float64 => self.as_f64().encode(writer).await?,
                DataType::Timestamp(_) => self.as_timestamp().encode(writer).await?,
                DataType::Time32(_) => self.as_time32().encode(writer).await?,
                DataType::Time64(_) => self.as_time64().encode(writer).await?,
                DataType::Date32 => self.as_date32().encode(writer).await?,
                DataType::Date64 => self.as_date64().encode(writer).await?,
            };
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.size_of()
    }
}

impl Eq for dyn Value {}
impl PartialEq for dyn Value {
    fn eq(&self, other: &Self) -> bool {
        if self.data_type() != other.data_type()
            && !matches!(
                self.data_type(),
                DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_)
            )
        {
            return false;
        }
        if self.is_none() {
            dbg!(self, other);
            return other.is_none() && other.data_type() == self.data_type();
        }
        if self.is_some() {
            if !other.is_some() {
                return false;
            }
            match self.data_type() {
                DataType::UInt8 => self.as_u8_opt().eq(other.as_u8_opt()),
                DataType::UInt16 => self.as_u16_opt().eq(other.as_u16_opt()),
                DataType::UInt32 => self.as_u32_opt().eq(other.as_u32_opt()),
                DataType::UInt64 => self.as_u64_opt().eq(other.as_u64_opt()),
                DataType::Int8 => self.as_i8_opt().eq(other.as_i8_opt()),
                DataType::Int16 => self.as_i16_opt().eq(other.as_i16_opt()),
                DataType::Int32 => self.as_i32_opt().eq(other.as_i32_opt()),
                DataType::Int64 => self.as_i64_opt().eq(other.as_i64_opt()),
                DataType::String => self.as_string_opt().eq(other.as_string_opt()),
                DataType::LargeString => self.as_string_opt().eq(other.as_string_opt()),
                DataType::Boolean => self.as_boolean_opt().eq(other.as_boolean_opt()),
                DataType::Bytes => self.as_bytes_opt().eq(other.as_bytes_opt()),
                DataType::LargeBinary => self.as_bytes_opt().eq(other.as_bytes_opt()),
                DataType::Float32 => self.as_f32_opt().eq(other.as_f32_opt()),
                DataType::Float64 => self.as_f64_opt().eq(other.as_f64_opt()),
                DataType::Timestamp(_) => self.as_timestamp_opt().eq(other.as_timestamp_opt()),
                DataType::Time32(_) => self.as_time32_opt().eq(other.as_time32_opt()),
                DataType::Time64(_) => self.as_time64_opt().eq(other.as_time64_opt()),
                DataType::Date32 => self.as_date32_opt().eq(other.as_date32_opt()),
                DataType::Date64 => self.as_date64_opt().eq(other.as_date64_opt()),
            }
        } else {
            match self.data_type() {
                DataType::UInt8 => self.as_u8().eq(other.as_u8()),
                DataType::UInt16 => self.as_u16().eq(other.as_u16()),
                DataType::UInt32 => self.as_u32().eq(other.as_u32()),
                DataType::UInt64 => self.as_u64().eq(other.as_u64()),
                DataType::Int8 => self.as_i8().eq(other.as_i8()),
                DataType::Int16 => self.as_i16().eq(other.as_i16()),
                DataType::Int32 => self.as_i32().eq(other.as_i32()),
                DataType::Int64 => self.as_i64().eq(other.as_i64()),
                DataType::Boolean => self.as_boolean().eq(other.as_boolean()),
                DataType::String | DataType::LargeString => self.as_string().eq(other.as_string()),
                DataType::Bytes | DataType::LargeBinary => self.as_bytes().eq(other.as_bytes()),
                DataType::Float32 => self.as_f32().eq(other.as_f32()),
                DataType::Float64 => self.as_f64().eq(other.as_f64()),
                DataType::Timestamp(_) => self.as_timestamp().eq(other.as_timestamp()),
                DataType::Time32(_) => self.as_time32().eq(other.as_time32()),
                DataType::Time64(_) => self.as_time64().eq(other.as_time64()),
                DataType::Date32 => self.as_date32().eq(other.as_date32()),
                DataType::Date64 => self.as_date64().eq(other.as_date64()),
            }
        }
    }
}

impl PartialOrd for dyn Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        debug_assert!(
            self.data_type() == other.data_type()
                || matches!(
                    self.data_type(),
                    DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_)
                )
        );

        if self.is_none() {
            // TOOD: handle None value
        }
        if self.is_some() {
            if !other.is_some() {
                // TOOD: handle None value
            }
            match self.data_type() {
                DataType::UInt8 => self.as_u8_opt().cmp(other.as_u8_opt()),
                DataType::UInt16 => self.as_u16_opt().cmp(other.as_u16_opt()),
                DataType::UInt32 => self.as_u32_opt().cmp(other.as_u32_opt()),
                DataType::UInt64 => self.as_u64_opt().cmp(other.as_u64_opt()),
                DataType::Int8 => self.as_i8_opt().cmp(other.as_i8_opt()),
                DataType::Int16 => self.as_i16_opt().cmp(other.as_i16_opt()),
                DataType::Int32 => self.as_i32_opt().cmp(other.as_i32_opt()),
                DataType::Int64 => self.as_i64_opt().cmp(other.as_i64_opt()),
                DataType::Boolean => self.as_boolean_opt().cmp(other.as_boolean_opt()),
                DataType::String | DataType::LargeString => {
                    self.as_string_opt().cmp(other.as_string_opt())
                }
                DataType::Bytes | DataType::LargeBinary => {
                    self.as_bytes_opt().cmp(other.as_bytes_opt())
                }
                DataType::Float32 => self.as_f32_opt().cmp(other.as_f32_opt()),
                DataType::Float64 => self.as_f64_opt().cmp(other.as_f64_opt()),
                DataType::Timestamp(_) => self.as_timestamp_opt().cmp(other.as_timestamp_opt()),
                DataType::Time32(_) => self.as_time32_opt().cmp(other.as_time32_opt()),
                DataType::Time64(_) => self.as_time64_opt().cmp(other.as_time64_opt()),
                DataType::Date32 => self.as_date32_opt().cmp(other.as_date32_opt()),
                DataType::Date64 => self.as_date64_opt().cmp(other.as_date64_opt()),
            }
        } else {
            match self.data_type() {
                DataType::UInt8 => self.as_u8().cmp(other.as_u8()),
                DataType::UInt16 => self.as_u16().cmp(other.as_u16()),
                DataType::UInt32 => self.as_u32().cmp(other.as_u32()),
                DataType::UInt64 => self.as_u64().cmp(other.as_u64()),
                DataType::Int8 => self.as_i8().cmp(other.as_i8()),
                DataType::Int16 => self.as_i16().cmp(other.as_i16()),
                DataType::Int32 => self.as_i32().cmp(other.as_i32()),
                DataType::Int64 => self.as_i64().cmp(other.as_i64()),
                DataType::Boolean => self.as_boolean().cmp(other.as_boolean()),
                DataType::String | DataType::LargeString => self.as_string().cmp(other.as_string()),
                DataType::Bytes | DataType::LargeBinary => self.as_bytes().cmp(other.as_bytes()),
                DataType::Float32 => self.as_f32().cmp(other.as_f32()),
                DataType::Float64 => self.as_f64().cmp(other.as_f64()),
                DataType::Timestamp(_) => self.as_timestamp().cmp(other.as_timestamp()),
                DataType::Time32(_) => self.as_time32().cmp(other.as_time32()),
                DataType::Time64(_) => self.as_time64().cmp(other.as_time64()),
                DataType::Date32 => self.as_date32().cmp(other.as_date32()),
                DataType::Date64 => self.as_date64().cmp(other.as_date64()),
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{cmp::Ordering, sync::Arc};

    use crate::{Date32, Key, Timestamp, Value};

    #[test]
    fn test_value_eq() {
        {
            let val = 123i32.as_value();
            let val2 = 123i32.as_value();
            let val3 = 123i8.as_value();
            let val4 = 124i32.as_value();
            let val5 = Date32::from(123);
            assert_eq!(val, val2);
            assert_ne!(val, val3);
            assert_ne!(val, val4);
            assert_ne!(val, val5.as_value());
        }
        {
            let val = String::from("123");
            let val2 = String::from("123");
            let val3 = String::from("124");
            assert_eq!(val.as_value(), val2.as_value());
            assert_ne!(val.as_value(), val3.as_value());
        }
        {
            let val = Timestamp::new_millis(123);
            let val2 = Timestamp::new_millis(123);
            let val3 = Timestamp::new_seconds(123);
            let val4 = Timestamp::new_micros(123000);
            assert_eq!(val.as_value(), val2.as_value());
            assert_ne!(val.as_value(), val3.as_value());
            assert_eq!(val.as_value(), val4.as_value());
        }
        {
            let val = Arc::new(Some(123i32)) as Arc<dyn Value>;
            let val2 = Arc::new(Some(123i32)) as Arc<dyn Value>;
            let val3 = Arc::new(123i32) as Arc<dyn Value>;
            let val4 = Arc::new(Some(124i32)) as Arc<dyn Value>;
            let val5 = Arc::new(None::<i32>) as Arc<dyn Value>;
            let val6 = Arc::new(None::<i32>) as Arc<dyn Value>;
            assert_eq!(val.as_ref(), val2.as_ref());
            assert_ne!(val.as_ref(), val3.as_ref());
            assert_ne!(val.as_ref(), val4.as_ref());
            assert_ne!(val.as_ref(), val5.as_ref());
            assert_eq!(val5.as_ref(), val6.as_ref());
        }
        {
            let val = Arc::new(Some(Timestamp::new_millis(123))) as Arc<dyn Value>;
            let val2 = Arc::new(Some(Timestamp::new_millis(123))) as Arc<dyn Value>;
            let val3 = Arc::new(Timestamp::new_seconds(123)) as Arc<dyn Value>;
            let val4 = Arc::new(Some(Timestamp::new_nanos(123000000))) as Arc<dyn Value>;
            let val5 = Arc::new(None::<Timestamp>) as Arc<dyn Value>;
            assert_eq!(val.as_ref(), val2.as_ref());
            assert_ne!(val.as_ref(), val3.as_ref());
            assert_eq!(val.as_ref(), val4.as_ref());
            assert_ne!(val.as_ref(), val5.as_ref());
        }
    }

    #[test]
    fn test_value_cmp() {
        {
            let val = 123u32.as_value();
            let val2 = 123u32.as_value();
            let val3 = 124u32.as_value();
            let val4 = 122u32.as_value();
            assert_eq!(val.cmp(val2), std::cmp::Ordering::Equal);
            assert_eq!(val.cmp(val3), std::cmp::Ordering::Less);
            assert_eq!(val.cmp(val4), std::cmp::Ordering::Greater);
        }

        {
            let val = String::from("123");
            let val2 = String::from("123");
            let val3 = String::from("124");
            let val4 = String::from("2");
            let val5 = String::from("119");
            assert_eq!(val.as_value().cmp(val2.as_value()), Ordering::Equal);
            assert_eq!(val.as_value().cmp(val3.as_value()), Ordering::Less);
            assert_eq!(val.as_value().cmp(val4.as_value()), Ordering::Less);
            assert_eq!(val.as_value().cmp(val5.as_value()), Ordering::Greater);
        }
        {
            let val = Timestamp::new_millis(123);
            let val2 = Timestamp::new_millis(123);
            let val3 = Timestamp::new_seconds(123);
            let val4 = Timestamp::new_micros(122999);
            assert_eq!(val.as_value().cmp(val2.as_value()), Ordering::Equal);
            assert_eq!(val.as_value().cmp(val3.as_value()), Ordering::Less);
            assert_eq!(val.as_value().cmp(val4.as_value()), Ordering::Greater);
        }
    }

    #[tokio::test]
    async fn test_encode_value_trait() {
        use std::io::{Cursor, SeekFrom};

        use fusio_log::{Decode, Encode};
        use tokio::io::AsyncSeekExt;

        let value = 123u16;
        let mut bytes = Vec::new();
        let mut buf = Cursor::new(&mut bytes);
        value.encode(&mut buf).await.unwrap();

        buf.seek(SeekFrom::Start(0)).await.unwrap();
        let decoded = u16::decode(&mut buf).await.unwrap();

        assert_eq!(value, decoded);
    }
}
