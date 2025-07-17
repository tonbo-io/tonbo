use std::{cmp::Ordering, sync::Arc};

use fusio_log::Decode;

use crate::{datatype::DataType, Date32, Date64, Keys, Time32, Time64, Timestamp, Value, F32, F64};

/// Decode a value from a reader
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

/// Compare two keys
pub fn compare(lhs: &Keys, rhs: &Keys) -> Ordering {
    debug_assert_eq!(lhs.len(), rhs.len());

    for (lkey, rkey) in lhs.iter().zip(rhs.iter()) {
        let res = lkey.cmp(rkey);
        if res != Ordering::Equal {
            return res;
        }
    }
    Ordering::Equal
}

/// Check if the lhs keys is equal to the rhs keys
pub fn value_eq(lhs: &Keys, rhs: &Keys) -> bool {
    compare(lhs, rhs) == Ordering::Equal
}

/// Check if the lhs keys is less than the rhs keys
pub fn value_lt(lhs: &Keys, rhs: &Keys) -> bool {
    compare(lhs, rhs) == Ordering::Less
}

/// Check if the lhs keys is greater than the rhs keys
pub fn value_gt(lhs: &Keys, rhs: &Keys) -> bool {
    compare(lhs, rhs) == Ordering::Greater
}

/// Check if the lhs keys is less than or equal to the rhs keys
pub fn value_le(lhs: &Keys, rhs: &Keys) -> bool {
    compare(lhs, rhs) == Ordering::Less || compare(lhs, rhs) == Ordering::Equal
}

/// Check if the lhs keys is greater than or equal to the rhs keys
pub fn value_ge(lhs: &Keys, rhs: &Keys) -> bool {
    compare(lhs, rhs) == Ordering::Greater || compare(lhs, rhs) == Ordering::Equal
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{
        cmp::Ordering,
        io::{Cursor, SeekFrom},
        sync::Arc,
    };

    use fusio_log::Encode;
    use tokio::io::AsyncSeekExt;

    use super::decode_value;
    use crate::{
        util::{compare, value_eq, value_ge, value_gt, value_le, value_lt},
        Key, Keys,
    };

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

    #[tokio::test]
    async fn test_compare() {
        {
            let lhs: Keys = vec![Arc::new(1_i8)];
            let rhs: Keys = vec![Arc::new(2_i8)];
            assert_eq!(compare(&lhs, &rhs), Ordering::Less);
            assert_eq!(compare(&rhs, &lhs), Ordering::Greater);
        }
        {
            let lhs: Keys = vec![Arc::new(2_i8)];
            let rhs: Keys = vec![Arc::new(2_i8)];
            assert_eq!(compare(&lhs, &rhs), Ordering::Equal);
        }
    }

    #[tokio::test]
    async fn test_value_cmp() {
        {
            let key1: Keys = vec![Arc::new(1_i8)];
            let key2: Keys = vec![Arc::new(2_i8)];
            let key3: Keys = vec![Arc::new(2_i8)];
            assert!(!value_eq(&key1, &key2));
            assert!(value_eq(&key2, &key3));
            // test value lt and le
            assert!(value_lt(&key1, &key2));
            assert!(value_le(&key1, &key2));
            assert!(value_le(&key2, &key3));
            // test value gt and ge
            assert!(value_gt(&key2, &key1));
            assert!(value_ge(&key2, &key1));
            assert!(value_ge(&key2, &key3));
        }
        {
            let key1: Keys = vec![Arc::new(2_i8), Arc::new("ab".to_string())];
            let key2: Keys = vec![Arc::new(2_i8), Arc::new("abc".to_string())];
            let key3: Keys = vec![Arc::new(2_i8), Arc::new("abc".to_string())];
            let key4: Keys = vec![Arc::new(2_i8), Arc::new("ac".to_string())];
            // test value eq
            assert!(!value_eq(&key1, &key2));
            assert!(value_eq(&key2, &key3));
            // test value lt
            assert!(value_lt(&key1, &key2));
            assert!(!value_lt(&key2, &key3));
            assert!(value_lt(&key3, &key4));
            // test value le
            assert!(value_le(&key1, &key2));
            assert!(value_le(&key2, &key3));
            // test value gt
            assert!(value_gt(&key2, &key1));
            assert!(!value_gt(&key3, &key2));
            assert!(value_gt(&key4, &key3));
            // test value ge
            assert!(value_ge(&key2, &key1));
            assert!(value_ge(&key3, &key2));
            assert!(!value_ge(&key3, &key4));
        }
    }
}
