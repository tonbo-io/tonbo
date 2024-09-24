use std::mem::size_of;

use fusio::{IoBuf, Read, Write};
use tokio_util::bytes::Bytes;

use super::{Decode, Encode};

impl<'r> Encode for &'r str {
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write + Unpin,
    {
        (self.len() as u16).encode(writer).await?;
        let (result, _) = writer.write(Bytes::from(self.as_bytes().to_vec())).await;
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
        let buf = reader.read(Some(len as u64)).await?;

        Ok(unsafe { String::from_utf8_unchecked(buf.as_slice().to_vec()) })
    }
}
