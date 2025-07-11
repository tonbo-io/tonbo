use std::sync::Arc;

use fusio_log::Decode;

use crate::{datatype::DataType, Date32, Date64, Time32, Time64, Timestamp, Value, F32, F64};

pub async fn decode_value<R>(reader: &mut R) -> Result<Arc<dyn Value>, fusio::Error>
where
    R: fusio::SeqRead,
{
    let data_type = DataType::decode(reader).await?;
    let is_opt = u8::decode(reader).await?;
    let v: Arc<dyn Value> = if is_opt == 0 {
        match data_type {
            DataType::UInt8 => Arc::new(u8::decode(reader).await?),
            DataType::UInt16 => Arc::new(u16::decode(reader).await?),
            DataType::UInt32 => Arc::new(u32::decode(reader).await?),
            DataType::UInt64 => Arc::new(u64::decode(reader).await?),
            DataType::Int8 => Arc::new(i8::decode(reader).await?),
            DataType::Int16 => Arc::new(i16::decode(reader).await?),
            DataType::Int32 => Arc::new(i32::decode(reader).await?),
            DataType::Int64 => Arc::new(i64::decode(reader).await?),
            DataType::String | DataType::LargeString => Arc::new(String::decode(reader).await?),
            DataType::Boolean => Arc::new(bool::decode(reader).await?),
            DataType::Bytes | DataType::LargeBinary => Arc::new(Vec::<u8>::decode(reader).await?),
            DataType::Float32 => Arc::new(F32::decode(reader).await?),
            DataType::Float64 => Arc::new(F64::decode(reader).await?),
            DataType::Timestamp(_) => Arc::new(Timestamp::decode(reader).await?),
            DataType::Time32(_) => Arc::new(Time32::decode(reader).await?),
            DataType::Time64(_) => Arc::new(Time64::decode(reader).await?),
            DataType::Date32 => Arc::new(Date32::decode(reader).await?),
            DataType::Date64 => Arc::new(Date64::decode(reader).await?),
        }
    } else {
        match data_type {
            DataType::UInt8 => Arc::new(Option::<u8>::decode(reader).await?),
            DataType::UInt16 => Arc::new(Option::<u16>::decode(reader).await?),
            DataType::UInt32 => Arc::new(Option::<u32>::decode(reader).await?),
            DataType::UInt64 => Arc::new(Option::<u64>::decode(reader).await?),
            DataType::Int8 => Arc::new(Option::<i8>::decode(reader).await?),
            DataType::Int16 => Arc::new(Option::<i16>::decode(reader).await?),
            DataType::Int32 => Arc::new(Option::<i32>::decode(reader).await?),
            DataType::Int64 => Arc::new(Option::<i64>::decode(reader).await?),
            DataType::String | DataType::LargeString => {
                Arc::new(Option::<String>::decode(reader).await?)
            }
            DataType::Boolean => Arc::new(Option::<bool>::decode(reader).await?),
            DataType::Bytes | DataType::LargeBinary => {
                Arc::new(Option::<Vec<u8>>::decode(reader).await?)
            }
            DataType::Float32 => Arc::new(Option::<F32>::decode(reader).await?),
            DataType::Float64 => Arc::new(Option::<F64>::decode(reader).await?),
            DataType::Timestamp(_) => Arc::new(Option::<Timestamp>::decode(reader).await?),
            DataType::Time32(_) => Arc::new(Option::<Time32>::decode(reader).await?),
            DataType::Time64(_) => Arc::new(Option::<Time64>::decode(reader).await?),
            DataType::Date32 => Arc::new(Option::<Date32>::decode(reader).await?),
            DataType::Date64 => Arc::new(Option::<Date64>::decode(reader).await?),
        }
    };

    Ok(v)
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use fusio_log::Encode;
    use tokio::io::AsyncSeekExt;

    use super::decode_value;
    use crate::Key;

    #[tokio::test]
    async fn test_encode_decode_value() {
        let value = 123i64.as_value();
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        value.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let decoded = decode_value(&mut cursor).await.unwrap();
        assert_eq!(value, decoded.as_ref());
    }
}
