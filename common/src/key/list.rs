use std::sync::Arc;

use super::{Key, KeyRef, Value};
use crate::datatype::DataType;

pub type LargeBinary = Vec<u8>;

impl Key for Vec<u8> {
    type Ref<'r> = &'r [u8];

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self.as_slice()
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl<'r> KeyRef<'r> for &'r [u8] {
    type Key = Vec<u8>;

    fn to_key(self) -> Self::Key {
        self.to_vec()
    }
}

impl Value for Vec<u8> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> crate::datatype::DataType {
        DataType::Bytes
    }

    fn size_of(&self) -> usize {
        self.len()
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(self.clone())
    }
}

impl Value for Option<Vec<u8>> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Bytes
    }

    fn size_of(&self) -> usize {
        match self {
            Some(v) => 1 + v.len(),
            None => 1,
        }
    }

    fn is_none(&self) -> bool {
        self.is_none()
    }

    fn is_some(&self) -> bool {
        self.is_some()
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(self.clone())
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::*;
    use crate::util::decode_value;

    #[tokio::test]
    async fn test_list_encode_decode() {
        let list = vec![1u8, 2, 3];

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        list.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let decoded = Vec::<u8>::decode(&mut cursor).await.unwrap();

        assert_eq!(list, decoded);
    }

    #[tokio::test]
    async fn test_list_encode_decode_value() {
        let list = Arc::new(vec![1u8, 2, 3]) as Arc<dyn Value>;

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        list.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let decoded = decode_value(&mut cursor).await.unwrap();

        assert_eq!(&list, &decoded);
        assert_eq!(
            decoded.as_any().downcast_ref::<Vec<u8>>().unwrap(),
            &vec![1u8, 2, 3]
        );
    }
}
