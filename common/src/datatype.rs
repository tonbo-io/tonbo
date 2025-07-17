use arrow::datatypes::DataType as ArrowDataType;
use fusio_log::{Decode, Encode};

use crate::key::TimeUnit;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    LargeString,
    Boolean,
    Bytes,
    LargeBinary,
    Float32,
    Float64,
    Timestamp(TimeUnit),
    /// representing the elapsed time since midnight in the unit of
    /// `TimeUnit`. Must be either seconds or milliseconds.
    Time32(TimeUnit),
    /// representing the elapsed time since midnight in the unit of `TimeUnit`. Must be either
    /// microseconds or nanoseconds.
    ///
    /// See more details in [`arrow::datatypes::DataType::Time64`].
    Time64(TimeUnit),
    /// representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds.
    ///
    /// See [`arrow::datatypes::DataType::Date64`] for more details.
    Date64,
}

impl From<&ArrowDataType> for DataType {
    fn from(datatype: &ArrowDataType) -> Self {
        match datatype {
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Binary => DataType::Bytes,
            ArrowDataType::Timestamp(unit, tz) => {
                debug_assert!(tz.is_none(), "expected timezone is none, get {:?}", tz);
                DataType::Timestamp(unit.into())
            }
            ArrowDataType::Time32(unit) => DataType::Time32(unit.into()),
            ArrowDataType::Time64(unit) => DataType::Time64(unit.into()),
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,
            ArrowDataType::LargeBinary => DataType::LargeBinary,
            ArrowDataType::LargeUtf8 => DataType::LargeString,
            _ => todo!(),
        }
    }
}

impl Encode for DataType {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        match self {
            DataType::UInt8 => 0u8.encode(writer).await,
            DataType::UInt16 => 1u8.encode(writer).await,
            DataType::UInt32 => 2u8.encode(writer).await,
            DataType::UInt64 => 3u8.encode(writer).await,
            DataType::Int8 => 4u8.encode(writer).await,
            DataType::Int16 => 5u8.encode(writer).await,
            DataType::Int32 => 6u8.encode(writer).await,
            DataType::Int64 => 7u8.encode(writer).await,
            DataType::String => 8u8.encode(writer).await,
            DataType::Boolean => 9u8.encode(writer).await,
            DataType::Bytes => 10u8.encode(writer).await,
            DataType::Float32 => 11u8.encode(writer).await,
            DataType::Float64 => 12u8.encode(writer).await,
            DataType::Timestamp(TimeUnit::Second) => 13u8.encode(writer).await,
            DataType::Timestamp(TimeUnit::Millisecond) => 14u8.encode(writer).await,
            DataType::Timestamp(TimeUnit::Microsecond) => 15u8.encode(writer).await,
            DataType::Timestamp(TimeUnit::Nanosecond) => 16u8.encode(writer).await,
            DataType::Time32(TimeUnit::Second) => 17u8.encode(writer).await,
            DataType::Time32(TimeUnit::Millisecond) => 18u8.encode(writer).await,
            DataType::Time64(TimeUnit::Microsecond) => 19u8.encode(writer).await,
            DataType::Time64(TimeUnit::Nanosecond) => 20u8.encode(writer).await,
            DataType::Date32 => 21u8.encode(writer).await,
            DataType::Date64 => 22u8.encode(writer).await,
            DataType::LargeBinary => 23u8.encode(writer).await,
            DataType::LargeString => 24u8.encode(writer).await,
            DataType::Time32(_) | DataType::Time64(_) => unreachable!(),
        }
    }

    fn size(&self) -> usize {
        1
    }
}

impl Decode for DataType {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let tag = u8::decode(reader).await?;
        let data_type = match tag {
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
            17 => DataType::Time32(TimeUnit::Second),
            18 => DataType::Time32(TimeUnit::Millisecond),
            19 => DataType::Time64(TimeUnit::Microsecond),
            20 => DataType::Time64(TimeUnit::Nanosecond),
            21 => DataType::Date32,
            22 => DataType::Date64,
            23 => DataType::LargeBinary,
            24 => DataType::LargeString,
            _ => panic!("invalid datatype tag"),
        };
        Ok(data_type)
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use crate::{datatype::DataType, TimeUnit};

    #[tokio::test]
    async fn test_datatype_encode_decode() {
        let data_type = DataType::UInt8;
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        data_type.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let decoded = DataType::decode(&mut cursor).await.unwrap();
        assert_eq!(data_type, decoded);
    }

    #[tokio::test]
    #[should_panic(expected = "invalid datatype tag")]
    async fn test_datatype_encode_panic() {
        let data_type = DataType::Time32(TimeUnit::Nanosecond);
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        data_type.encode(&mut cursor).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "invalid datatype tag")]
    async fn test_datatype_decode_panic() {
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        u8::encode(&100, &mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        DataType::decode(&mut cursor).await.unwrap();
    }
}
