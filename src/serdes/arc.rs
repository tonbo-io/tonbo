use std::sync::Arc;

use fusio::{SeqRead, Write};

use super::{Decode, Encode};

impl<T> Decode for Arc<T>
where
    T: Decode,
{
    type Error = T::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        Ok(Arc::from(T::decode(reader).await?))
    }
}

impl<T> Encode for Arc<T>
where
    T: Encode + Send + Sync,
{
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        self.as_ref().encode(writer).await
    }

    fn size(&self) -> usize {
        Encode::size(self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source_0 = Arc::new(1u64);
        let source_1 = Arc::new("Hello! Tonbo".to_string());

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded_0 = Arc::<u64>::decode(&mut cursor).await.unwrap();
        let decoded_1 = Arc::<String>::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
    }
}
