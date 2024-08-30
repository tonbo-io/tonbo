mod arc;
mod boolean;
mod num;
pub(crate) mod option;
mod string;

use std::{future::Future, io};

use tokio::io::{AsyncRead, AsyncWrite};

pub trait Encode {
    type Error: From<io::Error> + std::error::Error + Send + Sync + 'static;

    fn encode<W>(&self, writer: &mut W) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        W: AsyncWrite + Unpin + Send;

    fn size(&self) -> usize;
}

impl<T: Encode + Sync> Encode for &T {
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        Encode::encode(*self, writer).await
    }

    fn size(&self) -> usize {
        Encode::size(*self)
    }
}

pub trait Decode: Sized {
    type Error: From<io::Error> + std::error::Error + Send + Sync + 'static;

    fn decode<R>(reader: &mut R) -> impl Future<Output = Result<Self, Self::Error>>
    where
        R: AsyncRead + Unpin;
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_encode_decode() {
        // Implement a simple struct that implements Encode and Decode
        struct TestStruct(u32);

        impl Encode for TestStruct {
            type Error = io::Error;

            async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
            where
                W: AsyncWrite + Unpin + Send,
            {
                writer.write_u32(self.0).await?;
                Ok(())
            }

            fn size(&self) -> usize {
                std::mem::size_of::<u32>()
            }
        }

        impl Decode for TestStruct {
            type Error = io::Error;

            async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
            where
                R: AsyncRead + Unpin,
            {
                let value = tokio::io::AsyncReadExt::read_u32(reader).await?;
                Ok(TestStruct(value))
            }
        }

        // Test encoding and decoding
        let original = TestStruct(42);
        let mut buffer = Vec::new();

        original.encode(&mut buffer).await.unwrap();
        assert_eq!(buffer.len(), original.size());

        let mut cursor = std::io::Cursor::new(buffer);
        let decoded = TestStruct::decode(&mut cursor).await.unwrap();

        assert_eq!(original.0, decoded.0);
    }
}
