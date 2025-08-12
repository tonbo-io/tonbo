use fusio::Write;
use fusio_log::{Decode, Encode};

use crate::record::ValueError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DictionaryKeyType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

impl From<DictionaryKeyType> for arrow::datatypes::DataType {
    fn from(value: DictionaryKeyType) -> Self {
        match value {
            DictionaryKeyType::Int8 => arrow::datatypes::DataType::Int8,
            DictionaryKeyType::Int16 => arrow::datatypes::DataType::Int16,
            DictionaryKeyType::Int32 => arrow::datatypes::DataType::Int32,
            DictionaryKeyType::Int64 => arrow::datatypes::DataType::Int64,
            DictionaryKeyType::UInt8 => arrow::datatypes::DataType::UInt8,
            DictionaryKeyType::UInt16 => arrow::datatypes::DataType::UInt16,
            DictionaryKeyType::UInt32 => arrow::datatypes::DataType::UInt32,
            DictionaryKeyType::UInt64 => arrow::datatypes::DataType::UInt64,
        }
    }
}

impl From<&DictionaryKeyType> for arrow::datatypes::DataType {
    fn from(value: &DictionaryKeyType) -> Self {
        match value {
            DictionaryKeyType::Int8 => arrow::datatypes::DataType::Int8,
            DictionaryKeyType::Int16 => arrow::datatypes::DataType::Int16,
            DictionaryKeyType::Int32 => arrow::datatypes::DataType::Int32,
            DictionaryKeyType::Int64 => arrow::datatypes::DataType::Int64,
            DictionaryKeyType::UInt8 => arrow::datatypes::DataType::UInt8,
            DictionaryKeyType::UInt16 => arrow::datatypes::DataType::UInt16,
            DictionaryKeyType::UInt32 => arrow::datatypes::DataType::UInt32,
            DictionaryKeyType::UInt64 => arrow::datatypes::DataType::UInt64,
        }
    }
}

impl From<&arrow::datatypes::DataType> for DictionaryKeyType {
    fn from(value: &arrow::datatypes::DataType) -> Self {
        match value {
            arrow::datatypes::DataType::Int8 => DictionaryKeyType::Int8,
            arrow::datatypes::DataType::Int16 => DictionaryKeyType::Int16,
            arrow::datatypes::DataType::Int32 => DictionaryKeyType::Int32,
            arrow::datatypes::DataType::Int64 => DictionaryKeyType::Int64,
            arrow::datatypes::DataType::UInt8 => DictionaryKeyType::UInt8,
            arrow::datatypes::DataType::UInt16 => DictionaryKeyType::UInt16,
            arrow::datatypes::DataType::UInt32 => DictionaryKeyType::UInt32,
            arrow::datatypes::DataType::UInt64 => DictionaryKeyType::UInt64,
            _ => unreachable!("can not convert `{value}` to `DictionaryKeyType`"),
        }
    }
}

impl Encode for DictionaryKeyType {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        match self {
            DictionaryKeyType::Int8 => 0u8.encode(writer).await,
            DictionaryKeyType::Int16 => 1u8.encode(writer).await,
            DictionaryKeyType::Int32 => 2u8.encode(writer).await,
            DictionaryKeyType::Int64 => 3u8.encode(writer).await,
            DictionaryKeyType::UInt8 => 4u8.encode(writer).await,
            DictionaryKeyType::UInt16 => 5u8.encode(writer).await,
            DictionaryKeyType::UInt32 => 6u8.encode(writer).await,
            DictionaryKeyType::UInt64 => 7u8.encode(writer).await,
        }
    }

    fn size(&self) -> usize {
        1
    }
}

impl Decode for DictionaryKeyType {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let value = u8::decode(reader).await?;
        match value {
            0 => Ok(DictionaryKeyType::Int8),
            1 => Ok(DictionaryKeyType::Int16),
            2 => Ok(DictionaryKeyType::Int32),
            3 => Ok(DictionaryKeyType::Int64),
            4 => Ok(DictionaryKeyType::UInt8),
            5 => Ok(DictionaryKeyType::UInt16),
            6 => Ok(DictionaryKeyType::UInt32),
            7 => Ok(DictionaryKeyType::UInt64),
            _ => Err(fusio::Error::Other(Box::new(ValueError::InvalidDataType(
                "invalid dictionary key type".to_string(),
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use crate::record::DictionaryKeyType;

    #[tokio::test]
    async fn test_dict_key_type_encode_decode() {
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        let keys = [
            DictionaryKeyType::Int8,
            DictionaryKeyType::Int16,
            DictionaryKeyType::Int32,
            DictionaryKeyType::Int64,
            DictionaryKeyType::UInt8,
            DictionaryKeyType::UInt16,
            DictionaryKeyType::UInt32,
            DictionaryKeyType::UInt64,
        ];
        for key in keys.iter() {
            key.encode(&mut cursor).await.unwrap();
        }

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        for key in keys.iter() {
            let decoded = DictionaryKeyType::decode(&mut cursor).await.unwrap();
            assert_eq!(decoded, *key);
        }
    }
}
