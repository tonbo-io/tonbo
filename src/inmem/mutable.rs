use std::ops::Bound;

use crossbeam_skiplist::{
    map::{Entry, Range},
    SkipMap,
};

use crate::{
    oracle::{
        timestamp::{Timestamped, TimestampedRef},
        Timestamp, EPOCH,
    },
    record::{KeyRef, Record},
};

pub(crate) type MutableScan<'scan, R> = Range<
    'scan,
    TimestampedRef<<R as Record>::Key>,
    (
        Bound<&'scan TimestampedRef<<R as Record>::Key>>,
        Bound<&'scan TimestampedRef<<R as Record>::Key>>,
    ),
    Timestamped<<R as Record>::Key>,
    Option<R>,
>;

#[derive(Debug)]
pub struct Mutable<R>
where
    R: Record,
{
    data: SkipMap<Timestamped<R::Key>, Option<R>>,
}

impl<R> Default for Mutable<R>
where
    R: Record,
{
    fn default() -> Self {
        Mutable {
            data: Default::default(),
        }
    }
}

impl<R> Mutable<R>
where
    R: Record,
{
    pub fn new() -> Self {
        Mutable::default()
    }
}

impl<R> Mutable<R>
where
    R: Record + Send + Sync,
    R::Key: Send,
{
    pub(crate) fn insert(&self, record: R, ts: Timestamp) {
        self.data
            // TODO: remove key cloning
            .insert(Timestamped::new(record.key().to_key(), ts), Some(record));
    }

    pub(crate) fn remove(&self, key: R::Key, ts: Timestamp) {
        self.data.insert(Timestamped::new(key, ts), None);
    }

    fn get(
        &self,
        key: &R::Key,
        ts: Timestamp,
    ) -> Option<Entry<'_, Timestamped<R::Key>, Option<R>>> {
        self.data
            .range::<TimestampedRef<R::Key>, _>((
                Bound::Included(TimestampedRef::new(key, ts)),
                Bound::Unbounded,
            ))
            .next()
            .and_then(|entry| {
                if &entry.key().value == key {
                    Some(entry)
                } else {
                    None
                }
            })
    }

    pub(crate) fn into_iter(self) -> impl Iterator<Item = (Timestamped<R::Key>, Option<R>)> {
        self.data.into_iter()
    }

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
    ) -> MutableScan<'scan, R> {
        let lower = range.0.map(|key| TimestampedRef::new(key, ts));
        let upper = range.1.map(|key| TimestampedRef::new(key, EPOCH));

        self.data.range((lower, upper))
    }
}

impl<R> Mutable<R>
where
    R: Record,
{
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::Mutable;
    use crate::{
        oracle::timestamp::Timestamped,
        record::Record,
        tests::{Test, TestRef},
    };

    #[test]
    fn insert_and_get() {
        let key_1 = "key_1".to_owned();
        let key_2 = "key_2".to_owned();

        let mem_table = Mutable::default();

        mem_table.insert(
            Test {
                vstring: key_1.clone(),
                vu32: 1,
                vobool: Some(true),
            },
            0_u32.into(),
        );
        mem_table.insert(
            Test {
                vstring: key_2.clone(),
                vu32: 2,
                vobool: None,
            },
            1_u32.into(),
        );

        let entry = mem_table.get(&key_1, 0_u32.into()).unwrap();
        assert_eq!(
            entry.value().as_ref().unwrap().as_record_ref(),
            TestRef {
                vstring: &key_1,
                vu32: 1,
                vbool: Some(true)
            }
        )
    }
}
