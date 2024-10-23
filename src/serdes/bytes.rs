use bytes::Bytes;
use fusio::{IoBuf, SeqRead, Write};

use crate::serdes::{Decode, Encode};

impl Encode for &[u8] {
    type Error = fusio::Error;

    async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        (self.len() as u32).encode(writer).await?;
        let (result, _) = writer.write_all(*self).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl Encode for Bytes {
    type Error = fusio::Error;

    async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        (self.len() as u32).encode(writer).await?;
        let (result, _) = writer.write_all(self.as_slice()).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl Decode for Bytes {
    type Error = fusio::Error;

    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Self::Error> {
        let len = u32::decode(reader).await?;
        let (result, buf) = reader.read_exact(vec![0u8; len as usize]).await;
        result?;

        Ok(buf.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;
    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source = Bytes::from_static(b"hello! Tonbo");

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Bytes::decode(&mut cursor).await.unwrap();

        assert_eq!(source, decoded);
    }
}
