use std::io;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::serdes::{Decode, Encode};

impl Encode for &[u8] {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        writer.write_all(self).await
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl Encode for Bytes {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        writer.write_all(self).await
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl Decode for Bytes {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut buf = Vec::new();
        reader.read_exact(&mut buf).await?;

        Ok(Bytes::from(buf))
    }
}
