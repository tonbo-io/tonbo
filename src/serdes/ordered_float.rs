use fusio::{SeqRead, Write};
use ordered_float::OrderedFloat;

use crate::serdes::{Decode, Encode};

impl<T> Decode for OrderedFloat<T>
where
    T: Decode + ordered_float::FloatCore,
{
    type Error = T::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        Ok(OrderedFloat::from(T::decode(reader).await?))
    }
}

impl<T> Encode for OrderedFloat<T>
where
    T: Encode + Send + Sync,
{
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        self.0.encode(writer).await
    }

    fn size(&self) -> usize {
        Encode::size(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use ordered_float::OrderedFloat;
    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source_0 = OrderedFloat(32f32);
        let source_1 = OrderedFloat(64f64);

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded_0 = OrderedFloat::<f32>::decode(&mut cursor).await.unwrap();
        let decoded_1 = OrderedFloat::<f64>::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
    }
}
