pub mod timestamped;

use arrow::{
    array::{PrimitiveArray, Scalar},
    datatypes::UInt32Type,
};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};

pub use self::timestamped::*;

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Timestamp(u32);

pub(crate) const EPOCH: Timestamp = Timestamp(0);

impl From<u32> for Timestamp {
    fn from(ts: u32) -> Self {
        Self(ts)
    }
}

impl From<Timestamp> for u32 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl Timestamp {
    pub(crate) fn to_arrow_scalar(self) -> Scalar<PrimitiveArray<UInt32Type>> {
        PrimitiveArray::<UInt32Type>::new_scalar(self.0)
    }
}

impl Encode for Timestamp {
    type Error = fusio::Error;
    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        self.0.encode(writer).await
    }
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl Decode for Timestamp {
    type Error = fusio::Error;
    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        u32::decode(reader).await.map(Timestamp)
    }
}
