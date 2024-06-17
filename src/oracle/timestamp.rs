use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem::transmute};

use crate::oracle::Timestamp;

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) struct Timestamped<V> {
    pub(crate) ts: Timestamp,
    pub(crate) value: V,
}

impl<V> Copy for Timestamped<V> where V: Copy {}

impl<V> Timestamped<V> {
    pub(crate) fn new(value: V, ts: Timestamp) -> Self {
        Self { value, ts }
    }

    pub(crate) fn map<G>(&self, mut f: impl FnMut(&V) -> G) -> Timestamped<G> {
        Timestamped {
            value: f(&self.value),
            ts: self.ts,
        }
    }

    pub(crate) fn value(&self) -> &V {
        &self.value
    }

    pub(crate) fn ts(&self) -> Timestamp {
        self.ts
    }

    pub(crate) fn into_parts(self) -> (V, Timestamp) {
        (self.value, self.ts)
    }
}

impl<V> PartialOrd<Self> for Timestamped<V>
where
    V: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Borrow::<TimestampedRef<_>>::borrow(self)
            .partial_cmp(Borrow::<TimestampedRef<_>>::borrow(other))
    }
}

impl<V> Ord for Timestamped<V>
where
    V: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        Borrow::<TimestampedRef<_>>::borrow(self).cmp(Borrow::<TimestampedRef<_>>::borrow(other))
    }
}

#[derive(Debug)]
pub(crate) struct TimestampedRef<V> {
    _marker: PhantomData<V>,
    _mem: [()],
}

impl<V> TimestampedRef<V> {
    pub(crate) fn new(value: &V, ts: Timestamp) -> &Self {
        let value = value as *const _ as usize;
        let ts: u32 = ts.into();
        unsafe { transmute([value, ts as usize]) }
    }

    pub(crate) fn value(&self) -> &V {
        unsafe { transmute::<usize, &V>(transmute::<&Self, [usize; 2]>(self)[0]) }
    }

    pub(crate) fn ts(&self) -> Timestamp {
        unsafe { transmute::<&Self, [usize; 2]>(self)[1] as u32 }.into()
    }
}

impl<Q, V> Borrow<TimestampedRef<Q>> for Timestamped<V>
where
    V: Borrow<Q>,
{
    fn borrow(&self) -> &TimestampedRef<Q> {
        TimestampedRef::new(self.value.borrow(), self.ts)
    }
}

impl<V> PartialEq for TimestampedRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let this = transmute::<&TimestampedRef<V>, [usize; 2]>(self);
            let other = transmute::<&TimestampedRef<V>, [usize; 2]>(other);
            let this_value = transmute::<usize, &V>(this[0]);
            let other_value = transmute::<usize, &V>(other[0]);
            this_value == other_value && this[1] == other[1]
        }
    }
}

impl<V> Eq for TimestampedRef<V> where V: Eq {}

impl<V> PartialOrd<Self> for TimestampedRef<V>
where
    V: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        unsafe {
            let this = transmute::<&TimestampedRef<V>, [usize; 2]>(self);
            let other = transmute::<&TimestampedRef<V>, [usize; 2]>(other);
            let this_value = transmute::<usize, &V>(this[0]);
            let other_value = transmute::<usize, &V>(other[0]);
            this_value
                .partial_cmp(other_value)
                .map(|ordering| ordering.then_with(|| other[1].cmp(&this[1])))
        }
    }
}

impl<K> Ord for TimestampedRef<K>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe {
            let this = transmute::<&TimestampedRef<K>, [usize; 2]>(self);
            let other = transmute::<&TimestampedRef<K>, [usize; 2]>(other);
            let this_value = transmute::<usize, &K>(this[0]);
            let other_value = transmute::<usize, &K>(other[0]);
            this_value
                .cmp(other_value)
                .then_with(|| other[1].cmp(&this[1]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_cmp() {
        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&2, 2_u32.into());
        assert!(value1 < value2);

        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&1, 2_u32.into());
        assert!(value1 > value2);
    }

    #[test]
    fn test_value_eq() {
        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&1, 1_u32.into());
        assert_eq!(value1, value2);

        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&2, 1_u32.into());
        assert_ne!(value1, value2);

        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&1, 2_u32.into());
        assert_ne!(value1, value2);
    }
}
