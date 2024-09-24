use std::{intrinsics::transmute, ops::Bound, sync::Arc};

use async_lock::Mutex;
use crossbeam_skiplist::{
    map::{Entry, Range},
    SkipMap,
};
use fusio::{dynamic::DynFile, DynFs};
use ulid::Ulid;

use crate::{
    fs::{default_open_options, FileId},
    inmem::immutable::Immutable,
    record::{Key, KeyRef, Record, RecordInstance},
    timestamp::{
        timestamped::{Timestamped, TimestampedRef},
        Timestamp, EPOCH,
    },
    trigger::Trigger,
    wal::{log::LogType, WalFile},
    DbError, DbOption,
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

pub struct Mutable<R>
where
    R: Record,
{
    pub(crate) data: SkipMap<Timestamped<R::Key>, Option<R>>,
    wal: Option<Mutex<WalFile<Box<dyn DynFile>, R>>>,
    pub(crate) trigger: Arc<Box<dyn Trigger<R> + Send + Sync>>,
}

impl<R> Mutable<R>
where
    R: Record,
{
    pub async fn new(
        option: &DbOption<R>,
        trigger: Arc<Box<dyn Trigger<R> + Send + Sync>>,
        fs: &Arc<dyn DynFs>,
    ) -> Result<Self, fusio::Error> {
        let mut wal = None;
        if option.use_wal {
            let file_id = Ulid::new();
            let file = fs
                .open_options(&option.wal_path(&file_id), default_open_options())
                .await?;

            wal = Some(Mutex::new(WalFile::new(file, file_id)));
        };

        Ok(Self {
            data: Default::default(),
            wal,
            trigger,
        })
    }
}

impl<R> Mutable<R>
where
    R: Record + Send,
{
    pub(crate) async fn insert(
        &self,
        log_ty: LogType,
        record: R,
        ts: Timestamp,
    ) -> Result<bool, DbError<R>> {
        self.append(Some(log_ty), record.key().to_key(), ts, Some(record))
            .await
    }

    pub(crate) async fn remove(
        &self,
        log_ty: LogType,
        key: R::Key,
        ts: Timestamp,
    ) -> Result<bool, DbError<R>> {
        self.append(Some(log_ty), key, ts, None).await
    }

    pub(crate) async fn append(
        &self,
        log_ty: Option<LogType>,
        key: R::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<bool, DbError<R>> {
        let timestamped_key = Timestamped::new(key, ts);

        if let (Some(log_ty), Some(wal)) = (log_ty, &self.wal) {
            let mut wal_guard = wal.lock().await;

            wal_guard
                .write(
                    log_ty,
                    timestamped_key.map(|key| unsafe { transmute(key.as_key_ref()) }),
                    value.as_ref().map(R::as_record_ref),
                )
                .await
                .map_err(|e| DbError::WalWrite(Box::new(e)))?;
        }

        let is_exceeded = self.trigger.item(&value);
        self.data.insert(timestamped_key, value);

        Ok(is_exceeded)
    }

    pub(crate) fn get(
        &self,
        key: &R::Key,
        ts: Timestamp,
    ) -> Option<Entry<'_, Timestamped<R::Key>, Option<R>>> {
        self.data
            .range::<TimestampedRef<R::Key>, _>((
                Bound::Included(TimestampedRef::new(key, ts)),
                Bound::Included(TimestampedRef::new(key, EPOCH)),
            ))
            .next()
    }

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
    ) -> MutableScan<'scan, R> {
        let lower = match range.0 {
            Bound::Included(key) => Bound::Included(TimestampedRef::new(key, ts)),
            Bound::Excluded(key) => Bound::Excluded(TimestampedRef::new(key, EPOCH)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match range.1 {
            Bound::Included(key) => Bound::Included(TimestampedRef::new(key, EPOCH)),
            Bound::Excluded(key) => Bound::Excluded(TimestampedRef::new(key, ts)),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.data.range((lower, upper))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
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

    pub(crate) async fn into_immutable(
        self,
        instance: &RecordInstance,
    ) -> Result<(Option<FileId>, Immutable<R::Columns>), fusio::Error> {
        let mut file_id = None;

        if let Some(wal) = self.wal {
            let mut wal_guard = wal.lock().await;
            wal_guard.flush().await?;
            file_id = Some(wal_guard.file_id());
        }

        Ok((file_id, Immutable::from((self.data, instance))))
    }
}

impl<R> Mutable<R>
where
    R: Record,
{
    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use fusio::{local::TokioFs, path::Path, DynFs};

    use super::Mutable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::FileProvider,
        record::{Column, Datatype, DynRecord, Record},
        tests::{Test, TestRef},
        timestamp::Timestamped,
        trigger::TriggerFactory,
        wal::log::LogType,
        DbOption,
    };

    #[tokio::test]
    async fn insert_and_get() {
        let key_1 = "key_1".to_owned();
        let key_2 = "key_2".to_owned();

        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));
        let mem_table = Mutable::<Test>::new(&option, trigger, &fs).await.unwrap();

        mem_table
            .insert(
                LogType::Full,
                Test {
                    vstring: key_1.clone(),
                    vu32: 1,
                    vbool: Some(true),
                },
                0_u32.into(),
            )
            .await
            .unwrap();
        mem_table
            .insert(
                LogType::Full,
                Test {
                    vstring: key_2.clone(),
                    vu32: 2,
                    vbool: None,
                },
                1_u32.into(),
            )
            .await
            .unwrap();

        let entry = mem_table.get(&key_1, 0_u32.into()).unwrap();
        assert_eq!(
            entry.value().as_ref().unwrap().as_record_ref(),
            TestRef {
                vstring: &key_1,
                vu32: Some(1),
                vbool: Some(true)
            }
        );
        assert!(mem_table.get(&key_2, 0_u32.into()).is_none());
        assert!(mem_table.get(&key_2, 1_u32.into()).is_some());
    }

    #[tokio::test]
    async fn range() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let mutable = Mutable::<String>::new(&option, trigger, &fs).await.unwrap();

        mutable
            .insert(LogType::Full, "1".into(), 0_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "2".into(), 0_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "2".into(), 1_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "3".into(), 1_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "4".into(), 0_u32.into())
            .await
            .unwrap();

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
    #[tokio::test]
    async fn range_dyn() {
        let temp_dir = tempfile::tempdir().unwrap();
        let option = DbOption::with_path(temp_dir.path(), "age".to_string(), 0);
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let mutable = Mutable::<DynRecord, TokioExecutor>::new(&option, trigger)
            .await
            .unwrap();

        mutable
            .insert(
                LogType::Full,
                DynRecord::new(
                    vec![
                        Column::new(Datatype::INT8, Arc::new(1_i8), false),
                        Column::new(Datatype::INT16, Arc::new(1236_i16), true),
                    ],
                    0,
                ),
                0_u32.into(),
            )
            .await
            .unwrap();

        {
            let mut scan = mutable.scan((Bound::Unbounded, Bound::Unbounded), 0_u32.into());
            let entry = scan.next().unwrap();
            dbg!(entry.clone().value().as_ref().unwrap());
        }
        // assert_eq!(
        //     scan.next().unwrap().key(),
        //     &Timestamped::new("4".into(), 0_u32.into())
        // );
    }
}
