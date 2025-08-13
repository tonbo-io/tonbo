#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use arrow::array::Datum;
    use fusio::{SeqRead, Write};
    use fusio_log::{Decode, Encode};

    use crate::record::{Key, KeyRef};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Key2<K1: Key, K2: Key>(pub K1, pub K2);

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Key2Ref<'r, R1: KeyRef<'r>, R2: KeyRef<'r>>(
        pub R1,
        pub R2,
        std::marker::PhantomData<&'r ()>,
    );

    impl<K1: Key, K2: Key> Key for Key2<K1, K2> {
        type Ref<'r> = Key2Ref<'r, <K1 as Key>::Ref<'r>, <K2 as Key>::Ref<'r>>;

        fn as_key_ref(&self) -> Self::Ref<'_> {
            Key2Ref(
                self.0.as_key_ref(),
                self.1.as_key_ref(),
                std::marker::PhantomData,
            )
        }

        fn to_arrow_datums(&self) -> Vec<std::sync::Arc<dyn Datum>> {
            let mut out = Vec::new();
            out.extend(self.0.to_arrow_datums());
            out.extend(self.1.to_arrow_datums());
            out
        }
    }

    impl<'r, R1, R2> KeyRef<'r> for Key2Ref<'r, R1, R2>
    where
        R1: KeyRef<'r>,
        R2: KeyRef<'r>,
    {
        type Key = Key2<R1::Key, R2::Key>;

        fn to_key(self) -> Self::Key {
            Key2(self.0.to_key(), self.1.to_key())
        }
    }

    impl<K1: Key, K2: Key> Encode for Key2<K1, K2> {
        async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
        where
            W: Write,
        {
            self.0.encode(writer).await?;
            self.1.encode(writer).await
        }

        fn size(&self) -> usize {
            self.0.size() + self.1.size()
        }
    }

    impl<'r, R1, R2> Encode for Key2Ref<'r, R1, R2>
    where
        R1: KeyRef<'r>,
        R2: KeyRef<'r>,
    {
        async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
        where
            W: Write,
        {
            self.0.encode(writer).await?;
            self.1.encode(writer).await
        }

        fn size(&self) -> usize {
            self.0.size() + self.1.size()
        }
    }

    impl<K1: Key, K2: Key> Decode for Key2<K1, K2> {
        async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
        where
            R: SeqRead,
        {
            let k1 = K1::decode(reader).await?;
            let k2 = K2::decode(reader).await?;
            Ok(Key2(k1, k2))
        }
    }

    #[tokio::test]
    async fn key2_roundtrip_and_order() {
        let k = Key2("x".to_string(), 10u32);
        let mut bytes = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut bytes);
        k.encode(&mut cursor).await.unwrap();
        cursor.set_position(0);
        let k2 = <Key2<String, u32> as Decode>::decode(&mut cursor)
            .await
            .unwrap();

        assert_eq!(k, k2);
        assert!(Key2("a".to_string(), 1u32) < Key2("a".to_string(), 2u32));
        assert!(Key2("a".to_string(), 1u32) < Key2("b".to_string(), 0u32));

        // Check borrowed ref form encodes properly and converts back to owned
        let r = Key2Ref::<_, _>("zz", 5u32, std::marker::PhantomData);
        let mut bytes2 = Vec::new();
        let mut w = std::io::Cursor::new(&mut bytes2);
        r.encode(&mut w).await.unwrap();
        let owned: Key2<String, u32> = r.to_key();
        assert_eq!(owned, Key2("zz".to_string(), 5));
    }
}
