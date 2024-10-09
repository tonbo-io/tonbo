use std::mem::size_of;

use fusio::{Read, Write};

use super::{Decode, Encode};

impl<'r> Encode for &'r str {
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write + Unpin,
    {
        (self.len() as u16).encode(writer).await?;
        let (result, _) = writer.write_all(self.as_bytes()).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u16>() + self.len()
    }
}

impl Encode for String {
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write + Unpin + Send,
    {
        self.as_str().encode(writer).await
    }

    fn size(&self) -> usize {
        self.as_str().size()
    }
}

impl Decode for String {
    type Error = fusio::Error;

    async fn decode<R: Read + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let len = u16::decode(reader).await?;
        let (result, buf) = reader.read_exact(vec![0u8; len as usize]).await;
        result?;

        Ok(unsafe { String::from_utf8_unchecked(buf.as_slice().to_vec()) })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use fusio::Seek;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source_0 = "Hello! World";
        let source_1 = "Hello! Tonbo".to_string();

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();

        cursor.seek(0).await.unwrap();
        let decoded_0 = String::decode(&mut cursor).await.unwrap();
        let decoded_1 = String::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
    }
}
