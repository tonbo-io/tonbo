use fusio::{SeqRead, Write};
use crate::serdes::{Decode, Encode};

impl<T> Decode for Vec<T>
where
    T: Decode,
{
    type Error = T::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        let len = u32::decode(reader).await? as usize;
        let mut items = Vec::with_capacity(len);

        for _ in 0..len {
            items.push(T::decode(reader).await?);
        }
        Ok(items)
    }
}

impl<T> Encode for Vec<T>
where
    T: Encode + Send + Sync,
{
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        (self.len() as u32).encode(writer).await?;

        for item in self {
            item.encode(writer).await?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.iter().map(|item| item.size()).sum::<usize>() + size_of::<u32>()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_u8_encode_decode() {
        let source = b"hello! Tonbo".to_vec();

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Vec::<u8>::decode(&mut cursor).await.unwrap();

        assert_eq!(source, decoded);
    }
}
