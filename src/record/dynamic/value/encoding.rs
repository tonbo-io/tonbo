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
use crate::record::{
    decode_arrow_datatype, encode_arrow_datatype, size_of_arrow_datatype, ValueError, ValueRef,
};

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
                Value::FixedSizeBinary(v, _) => {
                    v.encode(writer).await?;
                }
                Value::List(_, vec) => {
                    let len = vec.len() as u32;
                    len.encode(writer).await?;
                    for v in vec.iter() {
                        v.encode_inner(writer).await?;
                    }
                }
                Value::Dictionary(_, value) => {
                    value.encode_inner(writer).await?;
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

    fn decode_inner<R>(reader: &mut R) -> BoxedFuture<'_, Result<Self, fusio::Error>>
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
                DataType::FixedSizeBinary(w) => Ok(Value::FixedSizeBinary(
                    Vec::<u8>::decode(reader).await?,
                    *w as u32,
                )),
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
                DataType::Dictionary(key_type, _) => {
                    let value = Box::new(Value::decode_inner(reader).await?);
                    Ok(Value::Dictionary(key_type.as_ref().into(), value))
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
        size_of_arrow_datatype(&self.data_type())
            + match self {
                Value::Null => 0,
                Value::Boolean(v) => v.size(),
                Value::Int8(v) => v.size(),
                Value::Int16(v) => v.size(),
                Value::Int32(v) => v.size(),
                Value::Int64(v) => v.size(),
                Value::UInt8(v) => v.size(),
                Value::UInt16(v) => v.size(),
                Value::UInt32(v) => v.size(),
                Value::UInt64(v) => v.size(),
                Value::Float32(v) => v.size(),
                Value::Float64(v) => v.size(),
                Value::String(v) => v.size(),
                Value::Binary(v) => v.size(),
                Value::FixedSizeBinary(v, _) => v.size(),
                Value::Date32(v) => v.size(),
                Value::Date64(v) => v.size(),
                Value::Timestamp(v, _) => v.size(),
                Value::Time32(v, _) => v.size(),
                Value::Time64(v, _) => v.size(),
                Value::List(_, vec) => vec.size(),
                Value::Dictionary(_, value) => value.size(),
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
                ValueRef::FixedSizeBinary(v, _) => {
                    v.encode(writer).await?;
                }
                ValueRef::List(_, vec) => {
                    let len = vec.len() as u32;
                    len.encode(writer).await?;
                    for v in vec.iter() {
                        v.encode_inner(writer).await?;
                    }
                }
                ValueRef::Dictionary(_, value_ref) => {
                    value_ref.encode_inner(writer).await?;
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
        size_of_arrow_datatype(&self.data_type())
            + match self {
                ValueRef::Null => 0,
                ValueRef::Boolean(v) => v.size(),
                ValueRef::Int8(v) => v.size(),
                ValueRef::Int16(v) => v.size(),
                ValueRef::Int32(v) => v.size(),
                ValueRef::Int64(v) => v.size(),
                ValueRef::UInt8(v) => v.size(),
                ValueRef::UInt16(v) => v.size(),
                ValueRef::UInt32(v) => v.size(),
                ValueRef::UInt64(v) => v.size(),
                ValueRef::Float32(v) => v.size(),
                ValueRef::Float64(v) => v.size(),
                ValueRef::String(v) => v.size(),
                ValueRef::Binary(v) => v.size(),
                ValueRef::FixedSizeBinary(v, _) => v.size(),
                ValueRef::Date32(v) => v.size(),
                ValueRef::Date64(v) => v.size(),
                ValueRef::Timestamp(v, _) => v.size(),
                ValueRef::Time32(v, _) => v.size(),
                ValueRef::Time64(v, _) => v.size(),
                ValueRef::List(_, vec) => vec.size(),
                ValueRef::Dictionary(_, value_ref) => value_ref.size(),
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
    use crate::record::{encode_arrow_datatype, encode_arrow_timeunit, DictionaryKeyType, Key};

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

    #[tokio::test]
    async fn test_list_value_ref_encode_decode() {
        let data_type = DataType::List(Arc::new(Field::new(
            "timestamp",
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        )));
        let value = ValueRef::List(
            &data_type,
            vec![Arc::new(Value::List(
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                vec![Arc::new(Value::Time64(1732838400, TimeUnit::Microsecond))],
            ))],
        );
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value, decoded.as_key_ref());
    }

    #[tokio::test]
    async fn test_fixed_size_binary_value_encode_decode() {
        let value = Value::FixedSizeBinary(b"hello".to_vec(), 5);
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value, decoded);
    }

    #[tokio::test]
    async fn test_fixed_size_binary_value_ref_encode_decode() {
        let value = ValueRef::FixedSizeBinary(b"tonbo", 5);
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value, decoded.as_key_ref());
    }

    #[tokio::test]
    async fn test_dictionary_value_encode_decode() {
        let value1 = Value::Dictionary(
            DictionaryKeyType::UInt8,
            Box::new(Value::String("value".to_string())),
        );
        let value2 = Value::Dictionary(DictionaryKeyType::Int64, Box::new(Value::Date64(114)));
        let value_ref = value1.as_key_ref();
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        value1.encode(&mut cursor).await.unwrap();
        value_ref.encode(&mut cursor).await.unwrap();
        value2.encode(&mut cursor).await.unwrap();
        value2.as_key_ref().encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value1, decoded);

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value_ref, decoded.as_key_ref());

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value2, decoded);

        let decoded = Value::decode(&mut cursor).await.unwrap();
        assert_eq!(value2.as_key_ref(), decoded.as_key_ref());
    }

    #[tokio::test]
    async fn test_size_matches_encoded_len_value() {
        let values = [
            Value::Null,
            Value::UInt8(1),
            Value::UInt16(1),
            Value::UInt32(1),
            Value::UInt64(1),
            Value::Int8(1),
            Value::Int16(1),
            Value::Int32(1),
            Value::Int64(1),
            Value::Float32(1.0),
            Value::Float64(1.0),
            Value::String("tonbo".to_string()),
            Value::Binary(b"tonbo".to_vec()),
            Value::FixedSizeBinary(b"tonbo".to_vec(), 5),
            Value::Date32(1),
            Value::Date64(1),
            Value::Timestamp(1, TimeUnit::Millisecond),
            Value::Time32(1, TimeUnit::Millisecond),
            Value::Time64(1, TimeUnit::Nanosecond),
            Value::List(
                DataType::List(Arc::new(Field::new(
                    "timestamp",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ))),
                vec![Arc::new(Value::Timestamp(
                    1732838400,
                    TimeUnit::Millisecond,
                ))],
            ),
            Value::Dictionary(DictionaryKeyType::Int64, Box::new(Value::Date64(114))),
            Value::Dictionary(
                DictionaryKeyType::UInt8,
                Box::new(Value::String("fusio".to_string())),
            ),
            Value::Dictionary(
                DictionaryKeyType::Int32,
                Box::new(Value::Binary(b"tonbo".to_vec())),
            ),
            Value::Dictionary(
                DictionaryKeyType::UInt8,
                Box::new(Value::FixedSizeBinary(b"tonbo".to_vec(), 5)),
            ),
        ];

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        for value in values.iter() {
            let before = cursor.position();
            value.encode(&mut cursor).await.unwrap();
            let after = cursor.position();

            assert_eq!(
                value.size(),
                (after - before) as usize,
                "encoded length {} differs from size() {} for {} value. value: {:?}",
                after - before,
                value.size(),
                value.data_type(),
                value
            );
        }
    }

    #[tokio::test]
    async fn test_size_matches_encoded_len_value_ref() {
        let values = [
            Value::Null,
            Value::UInt8(1),
            Value::UInt16(1),
            Value::UInt32(1),
            Value::UInt64(1),
            Value::Int8(1),
            Value::Int16(1),
            Value::Int32(1),
            Value::Int64(1),
            Value::Float32(1.0),
            Value::Float64(1.0),
            Value::String("fusio".to_string()),
            Value::Binary(b"fusio".to_vec()),
            Value::FixedSizeBinary(b"fusio".to_vec(), 5),
            Value::Date32(1),
            Value::Date64(1),
            Value::Timestamp(1, TimeUnit::Millisecond),
            Value::Time32(1, TimeUnit::Millisecond),
            Value::Time64(1, TimeUnit::Nanosecond),
            Value::List(
                DataType::List(Arc::new(Field::new(
                    "timestamp",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ))),
                vec![Arc::new(Value::Timestamp(
                    1732838400,
                    TimeUnit::Millisecond,
                ))],
            ),
            Value::Dictionary(DictionaryKeyType::Int64, Box::new(Value::Date64(114))),
            Value::Dictionary(
                DictionaryKeyType::UInt8,
                Box::new(Value::String("fusio".to_string())),
            ),
            Value::Dictionary(
                DictionaryKeyType::Int32,
                Box::new(Value::Binary(b"tonbo".to_vec())),
            ),
            Value::Dictionary(
                DictionaryKeyType::UInt8,
                Box::new(Value::FixedSizeBinary(b"tonbo".to_vec(), 5)),
            ),
        ];

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        for value in values.iter() {
            let before = cursor.position();
            let value_ref = value.as_key_ref();
            value_ref.encode(&mut cursor).await.unwrap();
            let after = cursor.position();

            assert_eq!(
                value_ref.size(),
                (after - before) as usize,
                "encoded length {} differs from size() {} for {} value_ref. value_ref: {:?}",
                after - before,
                value_ref.size(),
                value_ref.data_type(),
                value_ref
            );
        }
    }
}
