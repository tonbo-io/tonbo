use std::mem::size_of;

use fusio::{Read, Write};

use crate::serdes::{Decode, Encode};

impl Encode for bool {
    type Error = fusio::Error;

    async fn encode<W: Write + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        if *self { 1u8 } else { 0u8 }.encode(writer).await
    }

    fn size(&self) -> usize {
        size_of::<u8>()
    }
}

impl Decode for bool {
    type Error = fusio::Error;

    async fn decode<R: Read + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        Ok(u8::decode(reader).await? == 1u8)
    }
}
