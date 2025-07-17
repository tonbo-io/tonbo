use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem::size_of, ptr};

use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};

use crate::timestamp::Timestamp;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Ts<V> {
    pub ts: Timestamp,
    pub value: V,
}

impl<V> Copy for Ts<V> where V: Copy {}

impl<V> Ts<V> {
    pub(crate) fn new(value: V, ts: Timestamp) -> Self {
        Self { value, ts }
    }

    pub(crate) fn map<G>(&self, mut f: impl FnMut(&V) -> G) -> Ts<G> {
        Ts {
            value: f(&self.value),
            ts: self.ts,
        }
    }

    pub(crate) fn value(&self) -> &V {
        &self.value
    }

    #[allow(unused)]
    pub(crate) fn ts(&self) -> Timestamp {
        self.ts
    }

    #[allow(unused)]
    pub(crate) fn into_parts(self) -> (V, Timestamp) {
        (self.value, self.ts)
    }
}

impl<V> Ts<V>
where
    V: Encode,
{
    pub(crate) fn size(&self) -> usize {
        self.value.size() + size_of::<u32>()
    }
}

impl<V> PartialOrd<Self> for Ts<V>
where
    V: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        TsRef::new(&self.value, self.ts).partial_cmp(TsRef::new(&other.value, other.ts))
    }
}

impl<V> Ord for Ts<V>
where
    V: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.value()
            .cmp(other.value())
            .then_with(|| other.ts().cmp(&self.ts()))
        // TsRef::new(&self.value, self.ts).cmp(TsRef::new(&other.value, other.ts))
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct TsRef<V> {
    _marker: PhantomData<V>,
    _mem: [()],
}

impl<V> TsRef<V> {
    pub(crate) fn new(value: &V, ts: Timestamp) -> &Self {
        let value = value as *const _ as usize;
        let ts: u32 = ts.into();

        let mem = ptr::slice_from_raw_parts(value as *const V, ts as usize) as *const Self;
        unsafe { &*mem }
    }

    fn to_timestamped(&self) -> (&V, Timestamp) {
        let i = self as *const TsRef<V> as *const [()];
        unsafe { (&*(i as *const ()).cast::<V>(), (i.len() as u32).into()) }
    }

    pub(crate) fn value(&self) -> &V {
        self.to_timestamped().0
    }

    pub(crate) fn ts(&self) -> Timestamp {
        self.to_timestamped().1
    }
}

impl<Q, V> Borrow<TsRef<Q>> for Ts<V>
where
    V: Borrow<Q>,
{
    fn borrow(&self) -> &TsRef<Q> {
        TsRef::new(self.value.borrow(), self.ts)
    }
}

impl<V> PartialEq for TsRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value() && self.ts() == other.ts()
    }
}

impl<V> Eq for TsRef<V> where V: Eq {}

impl<V> PartialOrd<Self> for TsRef<V>
where
    V: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value()
            .partial_cmp(other.value())
            .map(|ordering| ordering.then_with(|| other.ts().cmp(&self.ts())))
    }
}

impl<K> Ord for TsRef<K>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.value()
            .cmp(other.value())
            .then_with(|| other.ts().cmp(&self.ts()))
    }
}

impl<V> Encode for Ts<V>
where
    V: Encode + Sync,
{
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        self.ts.encode(writer).await?;
        self.value.encode(writer).await
    }

    fn size(&self) -> usize {
        self.ts.size() + self.value.size()
    }
}

impl<V> Decode for Ts<V>
where
    V: Decode,
{
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: SeqRead,
    {
        let ts = Timestamp::decode(reader).await?;
        let value = V::decode(reader).await?;
        Ok(Ts::new(value, ts))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::PrimaryKey;

    use super::{Ts, TsRef};

    #[test]
    fn test_value_cmp() {
        let value1 = Ts::new(&1, 1_u32.into());
        let value2 = Ts::new(&2, 2_u32.into());
        assert!(value1 < value2);

        let value1 = Ts::new(&1, 1_u32.into());
        let value2 = Ts::new(&1, 2_u32.into());
        assert!(value1 > value2);
    }

    #[test]
    fn test_value_eq() {
        let value1 = Ts::new(&1, 1_u32.into());
        let value2 = Ts::new(&1, 1_u32.into());
        assert_eq!(value1, value2);

        let value1 = Ts::new(&1, 1_u32.into());
        let value2 = Ts::new(&2, 1_u32.into());
        assert_ne!(value1, value2);

        let value1 = Ts::new(&1, 1_u32.into());
        let value2 = Ts::new(&1, 2_u32.into());
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_timestamped_ref() {
        let value = Ts::new(1, 1_u32.into());
        let value_ref = TsRef::new(&value.value, value.ts);
        assert_eq!(value_ref.value(), &1);
        assert_eq!(value_ref.ts(), 1_u32.into());
    }

    #[test]
    fn test_pk_timestamped_ref() {
        let value = Ts::new(PrimaryKey::new(vec![Arc::new(1)]), 1_u32.into());
        let value_ref = TsRef::new(&value.value, value.ts);
        assert_eq!(value_ref.value(), &1.into());
        assert_eq!(value_ref.ts(), 1_u32.into());
    }

    #[test]
    fn test_timestamped_ref_cmp() {
        let value1 = Ts::new(1, 1_u32.into());
        let value2 = Ts::new(2, 2_u32.into());
        let value_ref1 = TsRef::new(&value1.value, value1.ts);
        let value_ref2 = TsRef::new(&value2.value, value2.ts);
        assert!(value_ref1 < value_ref2);

        let value1 = Ts::new(1, 1_u32.into());
        let value2 = Ts::new(1, 2_u32.into());
        let value_ref1 = TsRef::new(&value1.value, value1.ts);
        let value_ref2 = TsRef::new(&value2.value, value2.ts);
        assert!(value_ref1 > value_ref2);
    }
}
