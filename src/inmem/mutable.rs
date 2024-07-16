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

pub(crate) type MutableScanInner<'scan, R> = Range<
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

pub(crate) struct MutableScan<'scan, R>
where
    R: Record,
{
    inner: MutableScanInner<'scan, R>,
    item_buf: Option<Entry<'scan, Timestamped<R::Key>, Option<R>>>,
    ts: Timestamp,
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
    pub(crate) fn insert(&self, record: Timestamped<R>) {
        let (record, ts) = record.into_parts();
        self.data
            // TODO: remove key cloning
            .insert(Timestamped::new(record.key().to_key(), ts), Some(record));
    }

    pub(crate) fn remove(&self, key: Timestamped<R::Key>) {
        self.data.insert(key, None);
    }

    fn get(
        &self,
        key: &TimestampedRef<R::Key>,
    ) -> Option<Entry<'_, Timestamped<R::Key>, Option<R>>> {
        self.data
            .range::<TimestampedRef<R::Key>, _>((Bound::Included(key), Bound::Unbounded))
            .next()
            .and_then(|entry| {
                if &entry.key().value == key.value() {
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

        let mut scan = MutableScan {
            inner: self.data.range((lower, upper)),
            item_buf: None,
            ts,
        };
        let _ = scan.next();
        scan
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

impl<'scan, R> Iterator for MutableScan<'scan, R>
where
    R: Record,
{
    type Item = Entry<'scan, Timestamped<R::Key>, Option<R>>;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.inner.by_ref() {
            let key = entry.key();
            if key.ts <= self.ts
                && matches!(
                    self.item_buf
                        .as_ref()
                        .map(|entry| entry.key().value() != key.value()),
                    Some(true) | None
                )
            {
                return self.item_buf.replace(entry);
            }
        }
        self.item_buf.take()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::Mutable;
    use crate::{
        oracle::timestamp::{Timestamped, TimestampedRef},
        record::Record,
        tests::{Test, TestRef},
    };

    #[test]
    fn insert_and_get() {
        let key_1 = "key_1".to_owned();
        let key_2 = "key_2".to_owned();

        let mem_table = Mutable::default();

        mem_table.insert(Timestamped::new(
            Test {
                vstring: key_1.clone(),
                vu32: 1,
                vobool: Some(true),
            },
            0_u32.into(),
        ));
        mem_table.insert(Timestamped::new(
            Test {
                vstring: key_2.clone(),
                vu32: 2,
                vobool: None,
            },
            1_u32.into(),
        ));

        let entry = mem_table
            .get(TimestampedRef::new(&key_1, 0_u32.into()))
            .unwrap();
        assert_eq!(
            entry.value().as_ref().unwrap().as_record_ref(),
            TestRef {
                vstring: &key_1,
                vu32: 1,
                vbool: Some(true)
            }
        )
    }

    #[test]
    fn range() {
        let mutable = Mutable::<String>::new();

        mutable.insert(Timestamped::new("1".into(), 0_u32.into()));
        mutable.insert(Timestamped::new("2".into(), 0_u32.into()));
        mutable.insert(Timestamped::new("2".into(), 1_u32.into()));
        mutable.insert(Timestamped::new("3".into(), 1_u32.into()));
        mutable.insert(Timestamped::new("4".into(), 0_u32.into()));

        let mut scan = mutable.scan((Bound::Unbounded, Bound::Unbounded), 0_u32.into());

        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("1".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("2".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("4".into(), 0_u32.into())
        );

        let lower = "1".to_string();
        let upper = "4".to_string();
        let mut scan = mutable.scan(
            (Bound::Included(&lower), Bound::Included(&upper)),
            1_u32.into(),
        );

        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("1".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("2".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("3".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("4".into(), 0_u32.into())
        );
    }
}
