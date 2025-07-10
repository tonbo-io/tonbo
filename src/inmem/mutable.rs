use std::{ops::Bound, sync::Arc};

use async_lock::Mutex;
use common::KeyRef;
use crossbeam_skiplist::{
    map::{Entry, Range},
    SkipMap,
};
use fusio::DynFs;

use crate::{
    fs::{generate_file_id, FileId},
    inmem::immutable::Immutable,
    record::{Record, Schema},
    timestamp::{Timestamp, Ts, TsRef, EPOCH},
    trigger::FreezeTrigger,
    wal::{
        log::{Log, LogType},
        WalFile,
    },
    DbError, DbOption,
};

pub(crate) type MutableScan<'scan, R> = Range<
    'scan,
    TsRef<<<R as Record>::Schema as Schema>::Key>,
    (
        Bound<&'scan TsRef<<<R as Record>::Schema as Schema>::Key>>,
        Bound<&'scan TsRef<<<R as Record>::Schema as Schema>::Key>>,
    ),
    Ts<<<R as Record>::Schema as Schema>::Key>,
    Option<R>,
>;

pub(crate) struct MutableMemTable<R>
where
    R: Record,
{
    data: SkipMap<Ts<<R::Schema as Schema>::Key>, Option<R>>,
    wal: Option<Mutex<WalFile<R>>>,
    trigger: Arc<dyn FreezeTrigger<R>>,
    schema: Arc<R::Schema>,
}

impl<R> MutableMemTable<R>
where
    R: Record,
{
    pub(crate) async fn new(
        option: &DbOption,
        trigger: Arc<dyn FreezeTrigger<R>>,
        fs: Arc<dyn DynFs>,
        schema: Arc<R::Schema>,
    ) -> Result<Self, fusio::Error> {
        let mut wal = None;
        if option.use_wal {
            let file_id = generate_file_id();

            wal = Some(Mutex::new(
                WalFile::<R>::new(
                    fs,
                    option.wal_path(file_id),
                    option.wal_buffer_size,
                    file_id,
                )
                .await,
            ));
        };

        Ok(Self {
            data: Default::default(),
            wal,
            trigger,
            schema,
        })
    }

    pub(crate) async fn destroy(&mut self) -> Result<(), DbError> {
        if let Some(wal) = self.wal.take() {
            wal.into_inner().remove().await?;
        }
        Ok(())
    }
}

impl<R> MutableMemTable<R>
where
    R: Record + Send,
{
    pub(crate) async fn insert(
        &self,
        log_ty: LogType,
        record: R,
        ts: Timestamp,
    ) -> Result<bool, DbError> {
        self.append(Some(log_ty), record.key().to_key(), ts, Some(record))
            .await
    }

    pub(crate) async fn remove(
        &self,
        log_ty: LogType,
        key: <R::Schema as Schema>::Key,
        ts: Timestamp,
    ) -> Result<bool, DbError> {
        self.append(Some(log_ty), key, ts, None).await
    }

    pub(crate) async fn append(
        &self,
        log_ty: Option<LogType>,
        key: <R::Schema as Schema>::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<bool, DbError> {
        let timestamped_key = Ts::new(key, ts);

        let record_entry = Log::new(timestamped_key, value, log_ty);
        if let (Some(_log_ty), Some(wal)) = (log_ty, &self.wal) {
            wal.lock()
                .await
                .write(&record_entry)
                .await
                .map_err(|e| DbError::WalWrite(Box::new(e)))?;
        }

        let entry = self.data.insert(record_entry.key, record_entry.value);

        Ok(entry
            .value()
            .as_ref()
            .map(|v| self.trigger.check_if_exceed(v))
            .unwrap_or(false))
    }

    pub(crate) fn get(
        &self,
        key: &<R::Schema as Schema>::Key,
        ts: Timestamp,
    ) -> Option<Entry<'_, Ts<<R::Schema as Schema>::Key>, Option<R>>> {
        self.data
            .range::<TsRef<<R::Schema as Schema>::Key>, _>((
                Bound::Included(TsRef::new(key, ts)),
                Bound::Included(TsRef::new(key, EPOCH)),
            ))
            .next()
    }

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
        ts: Timestamp,
    ) -> MutableScan<'scan, R> {
        let lower = match range.0 {
            Bound::Included(key) => Bound::Included(TsRef::new(key, ts)),
            Bound::Excluded(key) => Bound::Excluded(TsRef::new(key, EPOCH)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match range.1 {
            Bound::Included(key) => Bound::Included(TsRef::new(key, EPOCH)),
            Bound::Excluded(key) => Bound::Excluded(TsRef::new(key, ts)),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.data.range((lower, upper))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn check_conflict(&self, key: &<R::Schema as Schema>::Key, ts: Timestamp) -> bool {
        self.data
            .range::<TsRef<<R::Schema as Schema>::Key>, _>((
                Bound::Excluded(TsRef::new(key, u32::MAX.into())),
                Bound::Excluded(TsRef::new(key, ts)),
            ))
            .next()
            .is_some()
    }

    pub(crate) async fn into_immutable(
        self,
    ) -> Result<
        (Option<FileId>, Immutable<<R::Schema as Schema>::Columns>),
        fusio_log::error::LogError,
    > {
        let mut file_id = None;

        if let Some(wal) = self.wal {
            let mut wal_guard = wal.lock().await;
            wal_guard.flush().await?;
            file_id = Some(wal_guard.file_id());
        }

        Ok((
            file_id,
            Immutable::new(self.data, self.schema.arrow_schema().clone()),
        ))
    }

    pub(crate) async fn flush_wal(&self) -> Result<(), DbError> {
        if let Some(wal) = self.wal.as_ref() {
            let mut wal_guard = wal.lock().await;
            wal_guard.flush().await?;
        }
        Ok(())
    }
}

impl<R> MutableMemTable<R>
where
    R: Record,
{
    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use common::{datatype::DataType, PrimaryKey};
    use fusio::{disk::TokioFs, path::Path, DynFs};

    use super::MutableMemTable;
    use crate::{
        inmem::immutable::tests::TestSchema,
        record::{test::StringSchema, DynRecord, DynSchema, Record, ValueDesc},
        tests::{Test, TestRef},
        timestamp::Ts,
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
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);
        let mem_table =
            MutableMemTable::<Test>::new(&option, trigger, fs.clone(), Arc::new(TestSchema {}))
                .await
                .unwrap();

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
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let mutable =
            MutableMemTable::<String>::new(&option, trigger, fs.clone(), Arc::new(StringSchema))
                .await
                .unwrap();

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
            &Ts::new("1".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("2".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("2".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("3".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("4".into(), 0_u32.into())
        );

        let lower = "1".to_string();
        let upper = "4".to_string();
        let mut scan = mutable.scan(
            (Bound::Included(&lower), Bound::Included(&upper)),
            1_u32.into(),
        );

        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("1".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("2".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("2".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("3".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Ts::new("4".into(), 0_u32.into())
        );
    }

    #[tokio::test]
    async fn test_dyn_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let schema = DynSchema::new(
            vec![
                ValueDesc::new("age".to_string(), DataType::Int8, false),
                ValueDesc::new("height".to_string(), DataType::Int16, true),
            ],
            0,
        );
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &schema,
        );
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let trigger = TriggerFactory::create(option.trigger_type);

        let schema = Arc::new(schema);

        let mutable = MutableMemTable::<DynRecord>::new(&option, trigger, fs.clone(), schema)
            .await
            .unwrap();

        mutable
            .insert(
                LogType::Full,
                DynRecord::new(vec![Arc::new(1_i8), Arc::new(1236_i16)], 0),
                0_u32.into(),
            )
            .await
            .unwrap();

        {
            let mut scan = mutable.scan((Bound::Unbounded, Bound::Unbounded), 0_u32.into());
            let entry = scan.next().unwrap();
            assert_eq!(entry.key().value, PrimaryKey::new(vec![Arc::new(1_i8)]));
            assert_eq!(entry.key().ts, 0u32.into());
        }
    }
}
