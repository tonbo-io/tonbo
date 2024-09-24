use std::sync::Arc;

use fusio::{Read, Write};

use super::{Decode, Encode};

impl<T> Decode for Arc<T>
where
    T: Decode,
{
    type Error = T::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: Read + Unpin,
    {
        Ok(Arc::from(T::decode(reader).await?))
    }
}

impl<T> Encode for Arc<T>
where
    T: Encode + Send + Sync,
{
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write + Unpin + Send,
    {
        self.as_ref().encode(writer).await
    }

    fn size(&self) -> usize {
        Encode::size(self.as_ref())
    }
}
