use fusio::Write;
use fusio_log::{Decode, Encode};

use super::{TimeUnit, Value};
use crate::record::{ValueError, ValueRef};

impl Encode for Value {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        match self {
            Value::Null => {
                0u8.encode(writer).await?;
            }
            Value::Boolean(v) => {
                1u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Int8(v) => {
                2u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Int16(v) => {
                3u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Int32(v) => {
                4u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Int64(v) => {
                5u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::UInt8(v) => {
                6u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::UInt16(v) => {
                7u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::UInt32(v) => {
                8u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::UInt64(v) => {
                9u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Float32(v) => {
                10u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Float64(v) => {
                11u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::String(v) => {
                12u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Binary(items) => {
                13u8.encode(writer).await?;
                items.encode(writer).await?;
            }
            Value::Date32(v) => {
                14u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Date64(v) => {
                15u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            Value::Timestamp(v, time_unit) => {
                16u8.encode(writer).await?;
                v.encode(writer).await?;
                time_unit.encode(writer).await?;
            }
            Value::Time32(v, time_unit) => {
                17u8.encode(writer).await?;
                v.encode(writer).await?;
                time_unit.encode(writer).await?;
            }
            Value::Time64(v, time_unit) => {
                18u8.encode(writer).await?;
                v.encode(writer).await?;
                time_unit.encode(writer).await?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Boolean(v) => 1 + v.size(),
            Value::Int8(v) => 1 + v.size(),
            Value::Int16(v) => 1 + v.size(),
            Value::Int32(v) => 1 + v.size(),
            Value::Int64(v) => 1 + v.size(),
            Value::UInt8(v) => 1 + v.size(),
            Value::UInt16(v) => 1 + v.size(),
            Value::UInt32(v) => 1 + v.size(),
            Value::UInt64(v) => 1 + v.size(),
            Value::Float32(v) => 1 + v.size(),
            Value::Float64(v) => 1 + v.size(),
            Value::String(v) => 1 + v.size(),
            Value::Binary(v) => 1 + v.size(),
            Value::Date32(v) => 1 + v.size(),
            Value::Date64(v) => 1 + v.size(),
            Value::Timestamp(v, time_unit) => 1 + v.size() + time_unit.size(),
            Value::Time32(v, time_unit) => 1 + v.size() + time_unit.size(),
            Value::Time64(v, time_unit) => 1 + v.size() + time_unit.size(),
        }
    }
}

impl Decode for Value {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let data_type = u8::decode(reader).await?;
        match data_type {
            0 => Ok(Value::Null),
            1 => Ok(Value::Boolean(bool::decode(reader).await?)),
            2 => Ok(Value::Int8(i8::decode(reader).await?)),
            3 => Ok(Value::Int16(i16::decode(reader).await?)),
            4 => Ok(Value::Int32(i32::decode(reader).await?)),
            5 => Ok(Value::Int64(i64::decode(reader).await?)),
            6 => Ok(Value::UInt8(u8::decode(reader).await?)),
            7 => Ok(Value::UInt16(u16::decode(reader).await?)),
            8 => Ok(Value::UInt32(u32::decode(reader).await?)),
            9 => Ok(Value::UInt64(u64::decode(reader).await?)),
            10 => Ok(Value::Float32(f32::decode(reader).await?)),
            11 => Ok(Value::Float64(f64::decode(reader).await?)),
            12 => Ok(Value::String(String::decode(reader).await?)),
            13 => Ok(Value::Binary(Vec::<u8>::decode(reader).await?)),
            14 => Ok(Value::Date32(i32::decode(reader).await?)),
            15 => Ok(Value::Date64(i64::decode(reader).await?)),
            16 => Ok(Value::Timestamp(
                i64::decode(reader).await?,
                TimeUnit::decode(reader).await?,
            )),
            17 => Ok(Value::Time32(
                i32::decode(reader).await?,
                TimeUnit::decode(reader).await?,
            )),
            18 => Ok(Value::Time64(
                i64::decode(reader).await?,
                TimeUnit::decode(reader).await?,
            )),
            _ => Err(fusio::Error::Other(Box::new(ValueError::InvalidDataType(
                data_type.to_string(),
            )))),
        }
    }
}

impl Encode for TimeUnit {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        match self {
            TimeUnit::Second => 0u8.encode(writer).await,
            TimeUnit::Millisecond => 1u8.encode(writer).await,
            TimeUnit::Microsecond => 2u8.encode(writer).await,
            TimeUnit::Nanosecond => 3u8.encode(writer).await,
        }
    }

    fn size(&self) -> usize {
        1
    }
}

impl Decode for TimeUnit {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let unit = u8::decode(reader).await?;
        match unit {
            0 => Ok(TimeUnit::Second),
            1 => Ok(TimeUnit::Millisecond),
            2 => Ok(TimeUnit::Microsecond),
            3 => Ok(TimeUnit::Nanosecond),
            _ => panic!("Invalid TimeUnit"),
        }
    }
}

impl<'a> Encode for ValueRef<'a> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        match self {
            ValueRef::Null => {
                0u8.encode(writer).await?;
            }
            ValueRef::Boolean(v) => {
                1u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Int8(v) => {
                2u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Int16(v) => {
                3u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Int32(v) => {
                4u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Int64(v) => {
                5u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::UInt8(v) => {
                6u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::UInt16(v) => {
                7u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::UInt32(v) => {
                8u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::UInt64(v) => {
                9u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Float32(v) => {
                10u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Float64(v) => {
                11u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::String(v) => {
                12u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Binary(items) => {
                13u8.encode(writer).await?;
                items.encode(writer).await?;
            }
            ValueRef::Date32(v) => {
                14u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Date64(v) => {
                15u8.encode(writer).await?;
                v.encode(writer).await?;
            }
            ValueRef::Timestamp(v, time_unit) => {
                16u8.encode(writer).await?;
                v.encode(writer).await?;
                time_unit.encode(writer).await?;
            }
            ValueRef::Time32(v, time_unit) => {
                17u8.encode(writer).await?;
                v.encode(writer).await?;
                time_unit.encode(writer).await?;
            }
            ValueRef::Time64(v, time_unit) => {
                18u8.encode(writer).await?;
                v.encode(writer).await?;
                time_unit.encode(writer).await?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            ValueRef::Null => 1,
            ValueRef::Boolean(v) => 1 + v.size(),
            ValueRef::Int8(v) => 1 + v.size(),
            ValueRef::Int16(v) => 1 + v.size(),
            ValueRef::Int32(v) => 1 + v.size(),
            ValueRef::Int64(v) => 1 + v.size(),
            ValueRef::UInt8(v) => 1 + v.size(),
            ValueRef::UInt16(v) => 1 + v.size(),
            ValueRef::UInt32(v) => 1 + v.size(),
            ValueRef::UInt64(v) => 1 + v.size(),
            ValueRef::Float32(v) => 1 + v.size(),
            ValueRef::Float64(v) => 1 + v.size(),
            ValueRef::String(v) => 1 + v.size(),
            ValueRef::Binary(v) => 1 + v.size(),
            ValueRef::Date32(v) => 1 + v.size(),
            ValueRef::Date64(v) => 1 + v.size(),
            ValueRef::Timestamp(v, time_unit) => 1 + v.size() + time_unit.size(),
            ValueRef::Time32(v, time_unit) => 1 + v.size() + time_unit.size(),
            ValueRef::Time64(v, time_unit) => 1 + v.size() + time_unit.size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::*;
    use crate::record::Key;

    #[tokio::test]
    async fn test_time_unit_encode_decode() {
        let time_unit = TimeUnit::Second;
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        time_unit.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = TimeUnit::decode(&mut cursor).await.unwrap();
        assert_eq!(time_unit, decoded);
    }

    #[tokio::test]
    async fn test_value_ref_encode() {
        let value = ValueRef::String("hello");
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value, decoded.as_key_ref());
    }

    #[tokio::test]
    async fn test_value_encode_decode() {
        let value = Value::String("hello".to_string());
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value, decoded);
    }

    #[tokio::test]
    async fn test_value_encode_decode_fail() {
        {
            let mut buf = Vec::new();
            let mut cursor = Cursor::new(&mut buf);

            10u8.encode(&mut cursor).await.unwrap();
            1.23f32.encode(&mut cursor).await.unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();

            let decoded = Value::decode(&mut cursor).await.unwrap();
            assert_eq!(decoded, Value::Float32(1.23f32));
        }
        {
            let mut buf = Vec::new();
            let mut cursor = Cursor::new(&mut buf);

            11u8.encode(&mut cursor).await.unwrap();
            1.23f32.encode(&mut cursor).await.unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();

            let res = Value::decode(&mut cursor).await;
            assert!(res.is_err());
        }
        {
            let mut buf = Vec::new();
            let mut cursor = Cursor::new(&mut buf);

            19u8.encode(&mut cursor).await.unwrap();
            1.23f32.encode(&mut cursor).await.unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();

            let res = Value::decode(&mut cursor).await;
            assert!(res.is_err());
        }
    }
}
