use bytes::Bytes;
use fusio::{IoBuf, Read, Write};

use crate::serdes::{Decode, Encode};

impl Encode for &[u8] {
    type Error = fusio::Error;

    async fn encode<W: Write + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let (result, _) = writer.write(Bytes::copy_from_slice(self)).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl Encode for Bytes {
    type Error = fusio::Error;

    async fn encode<W: Write + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let (result, _) = writer.write(self.clone()).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl Decode for Bytes {
    type Error = fusio::Error;

    async fn decode<R: Read + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let buf = reader.read(None).await?;

        Ok(buf.as_bytes())
    }
}
