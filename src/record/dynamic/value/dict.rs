use fusio::Write;
use fusio_log::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DictionaryKeyType {
    Int8 = 0,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

impl From<u8> for DictionaryKeyType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Int8,
            1 => Self::Int16,
            2 => Self::Int32,
            3 => Self::Int64,
            4 => Self::UInt8,
            5 => Self::UInt16,
            6 => Self::UInt32,
            7 => Self::UInt64,
            _ => unreachable!(),
        }
    }
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
        (*self as u8).encode(writer).await
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
        Ok(u8::decode(reader).await?.into())
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
