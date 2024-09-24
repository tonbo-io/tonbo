use std::mem::size_of;

use fusio::{IoBuf, Read, Write};

use super::{Decode, Encode};

#[macro_export]
macro_rules! implement_encode_decode {
    ($struct_name:ident) => {
        impl Encode for $struct_name {
            type Error = fusio::Error;

            async fn encode<W: Write + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
                let (result, _) = writer.write(&self.to_le_bytes()[..]).await;
                result?;

                Ok(())
            }

            fn size(&self) -> usize {
                size_of::<Self>()
            }
        }

        impl Decode for $struct_name {
            type Error = fusio::Error;

            async fn decode<R: Read + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
                let bytes = reader.read(Some(size_of::<Self>() as u64)).await?;

                // SAFETY
                Ok(Self::from_le_bytes(bytes.as_slice().try_into().unwrap()))
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
