use std::ops::Bound;

use fusio::{SeqRead, Write};

use crate::serdes::{Decode, Encode};

impl<T> Decode for Bound<T>
where
    T: Decode,
{
    type Error = T::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        Ok(match u8::decode(reader).await? {
            0 => Bound::Included(T::decode(reader).await?),
            1 => Bound::Excluded(T::decode(reader).await?),
            2 => Bound::Unbounded,
            _ => unreachable!(),
        })
    }
}

impl<T> Encode for Bound<T>
where
    T: Encode + Send + Sync,
{
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        match self {
            Bound::Included(value) => {
                0u8.encode(writer).await?;
                value.encode(writer).await?;
            }
            Bound::Excluded(value) => {
                1u8.encode(writer).await?;
                value.encode(writer).await?;
            }
            Bound::Unbounded => {
                2u8.encode(writer).await?;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u8>()
            + match self {
                Bound::Included(value) | Bound::Excluded(value) => value.size(),
                Bound::Unbounded => 0,
            }
    }
}
