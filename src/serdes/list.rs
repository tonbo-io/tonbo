use fusio::{SeqRead, Write};

use super::{Decode, Encode};

impl Decode for Vec<u8> {
    type Error = fusio::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        let len = u32::decode(reader).await?;
        let (result, buf) = reader
            .read_exact(vec![0u8; len as usize * size_of::<u8>()])
            .await;
        result?;

        Ok(buf)
    }
}

impl Encode for Vec<u8> {
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        (self.len() as u32).encode(writer).await?;
        let (result, _) = writer.write_all(self.as_slice()).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u32>() + size_of::<u8>() * self.len()
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
