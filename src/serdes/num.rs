use std::mem::size_of;

use fusio::{SeqRead, Write};

use super::{Decode, Encode};

#[macro_export]
macro_rules! implement_encode_decode {
    ($struct_name:ident) => {
        impl Encode for $struct_name {
            type Error = fusio::Error;

            async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
                let (result, _) = writer.write_all(&self.to_le_bytes()[..]).await;
                result?;

                Ok(())
            }

            fn size(&self) -> usize {
                size_of::<Self>()
            }
        }

        impl Decode for $struct_name {
            type Error = fusio::Error;

            async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Self::Error> {
                let mut bytes = [0u8; size_of::<Self>()];
                let (result, _) = reader.read_exact(&mut bytes[..]).await;
                result?;

                Ok(Self::from_le_bytes(bytes))
            }
        }
    };
}

implement_encode_decode!(i8);
implement_encode_decode!(i16);
implement_encode_decode!(i32);
implement_encode_decode!(i64);
implement_encode_decode!(u8);
implement_encode_decode!(u16);
implement_encode_decode!(u32);
implement_encode_decode!(u64);

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source_0 = 8u8;
        let source_1 = 16u16;
        let source_2 = 32u32;
        let source_3 = 64u64;
        let source_4 = 8i8;
        let source_5 = 16i16;
        let source_6 = 32i32;
        let source_7 = 64i64;

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();
        source_2.encode(&mut cursor).await.unwrap();
        source_3.encode(&mut cursor).await.unwrap();
        source_4.encode(&mut cursor).await.unwrap();
        source_5.encode(&mut cursor).await.unwrap();
        source_6.encode(&mut cursor).await.unwrap();
        source_7.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded_0 = u8::decode(&mut cursor).await.unwrap();
        let decoded_1 = u16::decode(&mut cursor).await.unwrap();
        let decoded_2 = u32::decode(&mut cursor).await.unwrap();
        let decoded_3 = u64::decode(&mut cursor).await.unwrap();
        let decoded_4 = i8::decode(&mut cursor).await.unwrap();
        let decoded_5 = i16::decode(&mut cursor).await.unwrap();
        let decoded_6 = i32::decode(&mut cursor).await.unwrap();
        let decoded_7 = i64::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
        assert_eq!(source_3, decoded_3);
        assert_eq!(source_4, decoded_4);
        assert_eq!(source_5, decoded_5);
        assert_eq!(source_6, decoded_6);
        assert_eq!(source_7, decoded_7);
    }
}
