use std::sync::Arc;

use arrow::datatypes::DataType;
use fusio::Write;
use fusio_log::{Decode, Encode};
#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
use futures_util::FutureExt;

use super::{TimeUnit, Value};
use crate::record::{decode_arrow_datatype, encode_arrow_datatype, ValueError, ValueRef};

#[cfg(not(target_arch = "wasm32"))]
type BoxedFuture<'a, T> = BoxFuture<'a, T>;
#[cfg(target_arch = "wasm32")]
type BoxedFuture<'a, T> = LocalBoxFuture<'a, T>;

impl Value {
    fn encode_inner<'a, 'b, W>(
        &'a self,
        writer: &'b mut W,
    ) -> BoxedFuture<'a, Result<(), fusio::Error>>
    where
        W: Write,
        'b: 'a,
    {
        let fut = async move {
            encode_arrow_datatype(&self.data_type(), writer).await?;
            match self {
                Value::Null => (),
                Value::Boolean(v) => {
                    v.encode(writer).await?;
                }
                Value::Int8(v) => {
                    v.encode(writer).await?;
                }
                Value::Int16(v) => {
                    v.encode(writer).await?;
                }
                Value::Int32(v) => {
                    v.encode(writer).await?;
                }
                Value::Int64(v) => {
                    v.encode(writer).await?;
                }
                Value::UInt8(v) => {
                    v.encode(writer).await?;
                }
                Value::UInt16(v) => {
                    v.encode(writer).await?;
                }
                Value::UInt32(v) => {
                    v.encode(writer).await?;
                }
                Value::UInt64(v) => {
                    v.encode(writer).await?;
                }
                Value::Float32(v) => {
                    v.encode(writer).await?;
                }
                Value::Float64(v) => {
                    v.encode(writer).await?;
                }
                Value::String(v) => {
                    v.encode(writer).await?;
                }
                Value::Binary(items) => {
                    items.encode(writer).await?;
                }
                Value::Date32(v) => {
                    v.encode(writer).await?;
                }
                Value::Date64(v) => {
                    v.encode(writer).await?;
                }
                Value::Timestamp(v, _) => {
                    v.encode(writer).await?;
                }
                Value::Time32(v, _) => {
                    v.encode(writer).await?;
                }
                Value::Time64(v, _) => {
                    v.encode(writer).await?;
                }
                Value::List(_, vec) => {
                    let len = vec.len() as u32;
                    len.encode(writer).await?;
                    for v in vec.iter() {
                        v.encode_inner(writer).await?;
                    }
                }
            }
            Ok(())
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            fut.boxed()
        }
        #[cfg(target_arch = "wasm32")]
        {
            fut.boxed_local()
        }
    }

    fn decode_inner<R>(reader: &mut R) -> BoxedFuture<Result<Self, fusio::Error>>
    where
        R: fusio::SeqRead,
    {
        let fut = async move {
            let data_type = decode_arrow_datatype(reader).await?;
            match &data_type {
                DataType::Null => Ok(Value::Null),
                DataType::Boolean => Ok(Value::Boolean(bool::decode(reader).await?)),
                DataType::Int8 => Ok(Value::Int8(i8::decode(reader).await?)),
                DataType::Int16 => Ok(Value::Int16(i16::decode(reader).await?)),
                DataType::Int32 => Ok(Value::Int32(i32::decode(reader).await?)),
                DataType::Int64 => Ok(Value::Int64(i64::decode(reader).await?)),
                DataType::UInt8 => Ok(Value::UInt8(u8::decode(reader).await?)),
                DataType::UInt16 => Ok(Value::UInt16(u16::decode(reader).await?)),
                DataType::UInt32 => Ok(Value::UInt32(u32::decode(reader).await?)),
                DataType::UInt64 => Ok(Value::UInt64(u64::decode(reader).await?)),
                DataType::Float32 => Ok(Value::Float32(f32::decode(reader).await?)),
                DataType::Float64 => Ok(Value::Float64(f64::decode(reader).await?)),
                DataType::Utf8 => Ok(Value::String(String::decode(reader).await?)),
                DataType::Binary => Ok(Value::Binary(Vec::<u8>::decode(reader).await?)),
                DataType::Date32 => Ok(Value::Date32(i32::decode(reader).await?)),
                DataType::Date64 => Ok(Value::Date64(i64::decode(reader).await?)),
                DataType::Timestamp(time_unit, _) => Ok(Value::Timestamp(
                    i64::decode(reader).await?,
                    time_unit.into(),
                )),
                DataType::Time32(time_unit) => {
                    if matches!(
                        time_unit,
                        arrow::datatypes::TimeUnit::Nanosecond
                            | arrow::datatypes::TimeUnit::Microsecond
                    ) {
                        unreachable!()
                    }
                    Ok(Value::Time32(i32::decode(reader).await?, time_unit.into()))
                }
                DataType::Time64(time_unit) => {
                    if matches!(
                        time_unit,
                        arrow::datatypes::TimeUnit::Second
                            | arrow::datatypes::TimeUnit::Millisecond
                    ) {
                        unreachable!()
                    }
                    Ok(Value::Time64(i64::decode(reader).await?, time_unit.into()))
                }
                DataType::List(field) => {
                    let len = u32::decode(reader).await?;
                    let mut list = Vec::new();
                    for _ in 0..len {
                        list.push(Arc::new(Value::decode_inner(reader).await?));
                    }
                    Ok(Value::List(field.data_type().clone(), list))
                }
                _ => Err(fusio::Error::Other(Box::new(ValueError::InvalidDataType(
                    data_type.to_string(),
                )))),
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            fut.boxed()
        }
        #[cfg(target_arch = "wasm32")]
        {
            fut.boxed_local()
        }
    }
}

impl Encode for Value {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        self.encode_inner(writer).await
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
            Value::List(data_type, vec) => {
                vec.iter().map(|v| v.size()).sum::<usize>()
                    + match data_type {
                        DataType::Timestamp(_, _) => 2,
                        DataType::Time32(_) | DataType::Time64(_) => 2,
                        DataType::List(field) => 1 + field.size(),
                        _ => 1,
                    }
            }
        }
    }
}

impl Decode for Value {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        Self::decode_inner(reader).await
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

impl ValueRef<'_> {
    fn encode_inner<'a, 'b, W>(
        &'a self,
        writer: &'b mut W,
    ) -> BoxedFuture<'a, Result<(), fusio::Error>>
    where
        W: Write,
        'b: 'a,
    {
        let fut = async move {
            encode_arrow_datatype(&self.data_type(), writer).await?;
            match self {
                ValueRef::Null => (),
                ValueRef::Boolean(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Int8(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Int16(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Int32(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Int64(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::UInt8(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::UInt16(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::UInt32(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::UInt64(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Float32(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Float64(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::String(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Binary(items) => {
                    items.encode(writer).await?;
                }
                ValueRef::Date32(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Date64(v) => {
                    v.encode(writer).await?;
                }
                ValueRef::Timestamp(v, _) => {
                    v.encode(writer).await?;
                }
                ValueRef::Time32(v, _) => {
                    v.encode(writer).await?;
                }
                ValueRef::Time64(v, _) => {
                    v.encode(writer).await?;
                }
                ValueRef::List(_, vec) => {
                    let len = vec.len() as u32;
                    len.encode(writer).await?;
                    for v in vec.iter() {
                        v.encode_inner(writer).await?;
                    }
                }
            }
            Ok(())
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            fut.boxed()
        }
        #[cfg(target_arch = "wasm32")]
        {
            fut.boxed_local()
        }
    }
}

impl Encode for ValueRef<'_> {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        self.encode_inner(writer).await
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
            ValueRef::List(data_type, vec) => {
                vec.iter().map(|v| v.size()).sum::<usize>()
                    + match data_type {
                        DataType::Timestamp(_, _) => 2,
                        DataType::Time32(_) | DataType::Time64(_) => 2,
                        DataType::List(field) => 1 + field.size(),
                        _ => 1,
                    }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use arrow::datatypes::Field;
    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::*;
    use crate::record::{encode_arrow_datatype, encode_arrow_timeunit, Key};

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
    async fn test_value_timstamp_encode_decode() {
        let value = Value::Timestamp(1732838400, TimeUnit::Nanosecond);
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

            encode_arrow_datatype(&DataType::Float32, &mut cursor)
                .await
                .unwrap();
            1.23f32.encode(&mut cursor).await.unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();

            let decoded = Value::decode(&mut cursor).await.unwrap();
            assert_eq!(decoded, Value::Float32(1.23f32));
        }
        {
            let mut buf = Vec::new();
            let mut cursor = Cursor::new(&mut buf);

            encode_arrow_datatype(&DataType::Float64, &mut cursor)
                .await
                .unwrap();
            1.23f32.encode(&mut cursor).await.unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();

            let res = Value::decode(&mut cursor).await;
            assert!(res.is_err());
        }
    }

    #[should_panic]
    #[tokio::test]
    async fn test_value_time32_nano_encode_decode_panic() {
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        19u8.encode(&mut cursor).await.unwrap();
        encode_arrow_timeunit(&arrow::datatypes::TimeUnit::Nanosecond, &mut cursor)
            .await
            .unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let _ = Value::decode(&mut cursor).await;
    }

    #[tokio::test]
    async fn test_list_value_encode_decode() {
        let value = Value::List(
            DataType::List(Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ))),
            vec![Arc::new(Value::List(
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                vec![Arc::new(Value::Timestamp(
                    1732838400,
                    TimeUnit::Millisecond,
                ))],
            ))],
        );
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value, decoded);
    }
}
