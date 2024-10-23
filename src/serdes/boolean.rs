use std::mem::size_of;

use fusio::{SeqRead, Write};

use crate::serdes::{Decode, Encode};

impl Encode for bool {
    type Error = fusio::Error;

    async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        if *self { 1u8 } else { 0u8 }.encode(writer).await
    }

    fn size(&self) -> usize {
        size_of::<u8>()
    }
}

impl Decode for bool {
    type Error = fusio::Error;

    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Self::Error> {
        Ok(u8::decode(reader).await? == 1u8)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source_0 = true;
        let source_1 = false;
        let source_2 = true;

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();
        source_2.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();

        let decoded_0 = bool::decode(&mut cursor).await.unwrap();
        let decoded_1 = bool::decode(&mut cursor).await.unwrap();
        let decoded_2 = bool::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
    }
}
