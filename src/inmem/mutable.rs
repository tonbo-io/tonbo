use std::collections::BTreeMap;
use std::intrinsics::transmute;
use std::ops::Bound;
use async_lock::Mutex;
use crossbeam_skiplist::{
    map::{Entry, Range},
    SkipMap,
};
use futures_util::io;
use ulid::Ulid;
use crate::{DbOption, record::{KeyRef, Record}, timestamp::{
    timestamped::{Timestamped, TimestampedRef},
    Timestamp, EPOCH,
}, WriteError};
use crate::fs::{FileId, FileProvider};
use crate::inmem::immutable::{ArrowArrays, Builder, Immutable};
use crate::record::Key;
use crate::wal::log::{Log, LogType};
use crate::wal::record_entry::RecordEntry;
use crate::wal::WalFile;

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
pub struct Mutable<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) data: SkipMap<Timestamped<R::Key>, Option<R>>,
    wal: Mutex<WalFile<FP::File, R>>
}

impl<R, FP> Mutable<R, FP>
where
    FP: FileProvider,
    R: Record,
{
    pub async fn new(option: &DbOption) -> io::Result<Self> {
        let file_id = Ulid::new();
        let file = FP::open(option.wal_path(&file_id)).await?;

        Ok(Self {
            data: Default::default(),
            wal: Mutex::new(WalFile::new(file, file_id)),
        })
    }
}

impl<R, FP> Mutable<R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    pub(crate) async fn insert(&self, log_ty: LogType, record: R, ts: Timestamp) -> Result<usize, WriteError<R>> {
        self.append(log_ty, record.key().to_key(), ts, Some(record), false).await
    }

    pub(crate) async fn remove(&self, log_ty: LogType, key: R::Key, ts: Timestamp) -> Result<usize, WriteError<R>> {
        self.append(log_ty, key, ts, None, false).await
    }

    async fn append(&self, log_ty: LogType, key: R::Key, ts: Timestamp, value: Option<R>, is_recover: bool) -> Result<usize, WriteError<R>> {
        let timestamped_key = Timestamped::new(key, ts);

        if !is_recover {
            let mut wal_guard = self.wal.lock().await;

            wal_guard
                .write(log_ty, timestamped_key.map(|key| unsafe { transmute(key.as_key_ref()) }), value.as_ref().map(R::as_record_ref))
                .await.unwrap();
        }
        self.data.insert(timestamped_key, value);

        Ok(self.data.len())
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

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
    ) -> MutableScan<'scan, R> {
        let lower = range.0.map(|key| TimestampedRef::new(key, ts));
        let upper = range.1.map(|key| TimestampedRef::new(key, EPOCH));

        self.data.range((lower, upper))
    }

    pub(crate) fn check_conflict(&self, key: &R::Key, ts: Timestamp) -> bool {
        self.data
            .range::<TimestampedRef<<R as Record>::Key>, _>((
                Bound::Excluded(TimestampedRef::new(key, u32::MAX.into())),
                Bound::Excluded(TimestampedRef::new(key, ts)),
            ))
            .next()
            .is_some()
    }

    pub(crate) async fn to_immutable(self) -> io::Result<(FileId, Immutable<R::Columns>)> {
        let file_id = {
            let mut wal_guard = self.wal.lock().await;
            wal_guard.flush().await?;
            wal_guard.file_id()
        };

        Ok((file_id, Immutable::from(self.data)))
    }
}

impl<R, FP> Mutable<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::Mutable;
    use crate::{DbOption, record::Record, tests::{Test, TestRef}, timestamp::Timestamped};
    use crate::executor::tokio::TokioExecutor;
    use crate::wal::log::LogType;

    #[tokio::test]
    async fn insert_and_get() {
        let key_1 = "key_1".to_owned();
        let key_2 = "key_2".to_owned();

        let temp_dir = tempfile::tempdir().unwrap();
        let mem_table = Mutable::<Test, TokioExecutor>::new(&DbOption::from(temp_dir.path())).await.unwrap();

        mem_table.insert(
            LogType::Full,
            Test {
                vstring: key_1.clone(),
                vu32: 1,
                vbool: Some(true),
            },
            0_u32.into(),
        ).await.unwrap();
        mem_table.insert(
            LogType::Full,
            Test {
                vstring: key_2.clone(),
                vu32: 2,
                vbool: None,
            },
            1_u32.into(),
        ).await.unwrap();

        let entry = mem_table.get(&key_1, 0_u32.into()).unwrap();
        assert_eq!(
            entry.value().as_ref().unwrap().as_record_ref(),
            TestRef {
                vstring: &key_1,
                vu32: Some(1),
                vbool: Some(true)
            }
        )
    }

    #[tokio::test]
    async fn range() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mutable = Mutable::<String, TokioExecutor>::new(&DbOption::from(temp_dir.path())).await.unwrap();

        mutable.insert(LogType::Full,"1".into(), 0_u32.into()).await.unwrap();
        mutable.insert(LogType::Full,"2".into(), 0_u32.into()).await.unwrap();
        mutable.insert(LogType::Full,"2".into(), 1_u32.into()).await.unwrap();
        mutable.insert(LogType::Full,"3".into(), 1_u32.into()).await.unwrap();
        mutable.insert(LogType::Full,"4".into(), 0_u32.into()).await.unwrap();

        let mut scan = mutable.scan((Bound::Unbounded, Bound::Unbounded), 0_u32.into());

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
            &Timestamped::new("2".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("3".into(), 1_u32.into())
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
            &Timestamped::new("2".into(), 0_u32.into())
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
