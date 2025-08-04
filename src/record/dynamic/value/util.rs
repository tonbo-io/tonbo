use std::sync::Arc;

use arrow::datatypes::Field;
use fusio_log::{Decode, Encode};
#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
use futures_util::FutureExt;

use crate::record::TimeUnit;

#[cfg(not(target_arch = "wasm32"))]
type BoxedFuture<'a, T> = BoxFuture<'a, T>;
#[cfg(target_arch = "wasm32")]
type BoxedFuture<'a, T> = LocalBoxFuture<'a, T>;

/// Split a timestamp value into seconds and nanoseconds
pub(crate) fn split_second_ns(v: i64, unit: TimeUnit) -> (i64, u32) {
    let base = TimeUnit::Second.factor() / unit.factor();
    let sec = v.div_euclid(base);
    let nsec = v.rem_euclid(base) * unit.factor();
    (sec, nsec as u32)
}

pub(crate) async fn encode_arrow_timeunit<W>(
    time_unit: &arrow::datatypes::TimeUnit,
    writer: &mut W,
) -> Result<(), fusio::Error>
where
    W: fusio::Write,
{
    match time_unit {
        arrow::datatypes::TimeUnit::Second => 0u8.encode(writer).await?,
        arrow::datatypes::TimeUnit::Millisecond => 1u8.encode(writer).await?,
        arrow::datatypes::TimeUnit::Microsecond => 2u8.encode(writer).await?,
        arrow::datatypes::TimeUnit::Nanosecond => 3u8.encode(writer).await?,
    };
    Ok(())
}

pub(crate) async fn decode_arrow_timeunit<R>(
    reader: &mut R,
) -> Result<arrow::datatypes::TimeUnit, fusio::Error>
where
    R: fusio::SeqRead,
{
    let tag = u8::decode(reader).await?;
    match tag {
        0 => Ok(arrow::datatypes::TimeUnit::Second),
        1 => Ok(arrow::datatypes::TimeUnit::Millisecond),
        2 => Ok(arrow::datatypes::TimeUnit::Microsecond),
        3 => Ok(arrow::datatypes::TimeUnit::Nanosecond),
        _ => unreachable!(),
    }
}

pub(crate) fn encode_arrow_datatype<'a, 'b, W>(
    data_type: &'a arrow::datatypes::DataType,
    writer: &'b mut W,
) -> BoxedFuture<'a, Result<(), fusio::Error>>
where
    W: fusio::Write,
    'b: 'a,
{
    let fut = async move {
        match data_type {
            arrow::datatypes::DataType::Null => 0u8.encode(writer).await?,
            arrow::datatypes::DataType::UInt8 => 1u8.encode(writer).await?,
            arrow::datatypes::DataType::UInt16 => 2u8.encode(writer).await?,
            arrow::datatypes::DataType::UInt32 => 3u8.encode(writer).await?,
            arrow::datatypes::DataType::UInt64 => 4u8.encode(writer).await?,
            arrow::datatypes::DataType::Int8 => 5u8.encode(writer).await?,
            arrow::datatypes::DataType::Int16 => 6u8.encode(writer).await?,
            arrow::datatypes::DataType::Int32 => 7u8.encode(writer).await?,
            arrow::datatypes::DataType::Int64 => 8u8.encode(writer).await?,
            arrow::datatypes::DataType::Utf8 => 9u8.encode(writer).await?,
            arrow::datatypes::DataType::LargeUtf8 => 10u8.encode(writer).await?,
            arrow::datatypes::DataType::Boolean => 11u8.encode(writer).await?,
            arrow::datatypes::DataType::Binary => 12u8.encode(writer).await?,
            arrow::datatypes::DataType::LargeBinary => 13u8.encode(writer).await?,
            arrow::datatypes::DataType::Float32 => 14u8.encode(writer).await?,
            arrow::datatypes::DataType::Float64 => 15u8.encode(writer).await?,
            arrow::datatypes::DataType::Timestamp(time_unit, _) => {
                16u8.encode(writer).await?;
                encode_arrow_timeunit(time_unit, writer).await?;
            }
            arrow::datatypes::DataType::Date32 => 17u8.encode(writer).await?,
            arrow::datatypes::DataType::Date64 => 18u8.encode(writer).await?,
            arrow::datatypes::DataType::Time32(time_unit) => {
                19u8.encode(writer).await?;
                encode_arrow_timeunit(time_unit, writer).await?;
            }
            arrow::datatypes::DataType::Time64(time_unit) => {
                20u8.encode(writer).await?;
                encode_arrow_timeunit(time_unit, writer).await?;
            }
            arrow::datatypes::DataType::List(field) => {
                30u8.encode(writer).await?;
                field.name().encode(writer).await?;
                encode_arrow_datatype(field.data_type(), writer).await?;
                field.is_nullable().encode(writer).await?;
            }
            _ => unreachable!(),
        };
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

pub(crate) fn decode_arrow_datatype<R>(
    reader: &mut R,
) -> BoxedFuture<Result<arrow::datatypes::DataType, fusio::Error>>
where
    R: fusio::SeqRead,
{
    let fut = async move {
        let tag = u8::decode(reader).await?;
        match tag {
            0 => Ok(arrow::datatypes::DataType::Null),
            1 => Ok(arrow::datatypes::DataType::UInt8),
            2 => Ok(arrow::datatypes::DataType::UInt16),
            3 => Ok(arrow::datatypes::DataType::UInt32),
            4 => Ok(arrow::datatypes::DataType::UInt64),
            5 => Ok(arrow::datatypes::DataType::Int8),
            6 => Ok(arrow::datatypes::DataType::Int16),
            7 => Ok(arrow::datatypes::DataType::Int32),
            8 => Ok(arrow::datatypes::DataType::Int64),
            9 => Ok(arrow::datatypes::DataType::Utf8),
            10 => Ok(arrow::datatypes::DataType::LargeUtf8),
            11 => Ok(arrow::datatypes::DataType::Boolean),
            12 => Ok(arrow::datatypes::DataType::Binary),
            13 => Ok(arrow::datatypes::DataType::LargeBinary),
            14 => Ok(arrow::datatypes::DataType::Float32),
            15 => Ok(arrow::datatypes::DataType::Float64),
            16 => {
                let time_unit = decode_arrow_timeunit(reader).await?;
                Ok(arrow::datatypes::DataType::Timestamp(time_unit, None))
            }
            17 => Ok(arrow::datatypes::DataType::Date32),
            18 => Ok(arrow::datatypes::DataType::Date64),
            19 => {
                let time_unit = decode_arrow_timeunit(reader).await?;
                Ok(arrow::datatypes::DataType::Time32(time_unit))
            }
            20 => {
                let time_unit = decode_arrow_timeunit(reader).await?;
                Ok(arrow::datatypes::DataType::Time64(time_unit))
            }
            30 => {
                let name = String::decode(reader).await?;
                let data_type = decode_arrow_datatype(reader).await?;
                let is_nullable = bool::decode(reader).await?;
                Ok(arrow::datatypes::DataType::List(Arc::new(Field::new(
                    name,
                    data_type,
                    is_nullable,
                ))))
            }

            _ => unreachable!(),
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

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use arrow::datatypes::DataType;
    use tokio::io::AsyncSeekExt;

    use super::*;

    #[test]
    fn test_split_ns() {
        let (sec, nsec) = split_second_ns(1716393600, TimeUnit::Second);
        assert_eq!(sec, 1716393600);
        assert_eq!(nsec, 0);

        let (sec, mills) = split_second_ns(1716393600, TimeUnit::Millisecond);
        assert_eq!(sec, 1716393);
        assert_eq!(mills, 600_000_000);

        let (sec, micros) = split_second_ns(1716393600, TimeUnit::Microsecond);
        assert_eq!(sec, 1716);
        assert_eq!(micros, 393_600_000);

        let (sec, nanos) = split_second_ns(1716393600, TimeUnit::Nanosecond);
        assert_eq!(sec, 1);
        assert_eq!(nanos, 716_393_600);
    }

    #[tokio::test]
    async fn test_time_unit_encode_decode() {
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        TimeUnit::Second.encode(&mut cursor).await.unwrap();
        TimeUnit::Millisecond.encode(&mut cursor).await.unwrap();
        TimeUnit::Microsecond.encode(&mut cursor).await.unwrap();
        TimeUnit::Nanosecond.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let time_unit = decode_arrow_timeunit(&mut cursor).await.unwrap();
        assert_eq!(time_unit, arrow::datatypes::TimeUnit::Second);

        let time_unit = decode_arrow_timeunit(&mut cursor).await.unwrap();
        assert_eq!(time_unit, arrow::datatypes::TimeUnit::Millisecond);

        let time_unit = decode_arrow_timeunit(&mut cursor).await.unwrap();
        assert_eq!(time_unit, arrow::datatypes::TimeUnit::Microsecond);
    }

    #[should_panic]
    #[tokio::test]
    async fn test_timeunit_encode_decode_panic() {
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        4u8.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();

        let _ = decode_arrow_timeunit(&mut cursor).await;
    }

    #[tokio::test]
    async fn test_arrow_datatype_encode_decode() {
        {
            let data_type = DataType::Binary;
            let mut buf = Vec::new();
            let mut cursor = Cursor::new(&mut buf);
            encode_arrow_datatype(&data_type, &mut cursor)
                .await
                .unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();
            let decoded = decode_arrow_datatype(&mut cursor).await.unwrap();
            assert_eq!(data_type, decoded);
        }
        {
            let mut buf = Vec::new();
            let mut cursor = Cursor::new(&mut buf);
            let data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
            encode_arrow_datatype(&data_type, &mut cursor)
                .await
                .unwrap();

            cursor.seek(SeekFrom::Start(0)).await.unwrap();
            let decoded = decode_arrow_datatype(&mut cursor).await.unwrap();
            assert_eq!(data_type, decoded);
        }
    }
}
