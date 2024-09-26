use std::{
    collections::{
        btree_map::{Entry, Range},
        BTreeMap, Bound,
    },
    io,
    mem::transmute,
    sync::Arc,
};

use async_lock::RwLockReadGuard;
use flume::SendError;
use lockable::AsyncLimit;
use parquet::{arrow::ProjectionMask, errors::ParquetError};
use thiserror::Error;

use crate::{
    compaction::CompactTask,
    fs::manager::StoreManager,
    record::{Key, KeyRef},
    stream,
    stream::mem_projection::MemProjectionStream,
    timestamp::{Timestamp, Timestamped},
    version::{TransactionTs, VersionRef},
    wal::log::LogType,
    DbError, LockMap, Projection, Record, Scan, Schema,
};

pub(crate) struct TransactionScan<'scan, R: Record> {
    inner: Range<'scan, R::Key, Option<R>>,
    ts: Timestamp,
}

impl<'scan, R> Iterator for TransactionScan<'scan, R>
where
    R: Record,
{
    type Item = (Timestamped<<R::Key as Key>::Ref<'scan>>, &'scan Option<R>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(key, value)| (Timestamped::new(key.as_key_ref(), self.ts), value))
    }
}
/// optimistic ACID transaction, open with
/// [`DB::transaction`](crate::DB::transaction) method
pub struct Transaction<'txn, R>
where
    R: Record,
{
    ts: Timestamp,
    local: BTreeMap<R::Key, Option<R>>,
    share: RwLockReadGuard<'txn, Schema<R>>,
    version: VersionRef<R>,
    lock_map: LockMap<R::Key>,
    manager: Arc<StoreManager>,
}

impl<'txn, R> Transaction<'txn, R>
where
    R: Record + Send,
{
    pub(crate) fn new(
        version: VersionRef<R>,
        share: RwLockReadGuard<'txn, Schema<R>>,
        lock_map: LockMap<R::Key>,
        manager: Arc<StoreManager>,
    ) -> Self {
        Self {
            ts: version.load_ts(),
            local: BTreeMap::new(),
            share,
            version,
            lock_map,
            manager,
        }
    }

    /// get the record with `key` as the primary key and get only the data specified in
    /// [`Projection`]
    pub async fn get<'get>(
        &'get self,
        key: &'get R::Key,
        projection: Projection,
    ) -> Result<Option<TransactionEntry<'get, R>>, DbError<R>> {
        Ok(match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some(TransactionEntry::Local(v.as_record_ref())),
            None => self
                .share
                .get(&self.version, &self.manager, key, self.ts, projection)
                .await?
                .and_then(|entry| {
                    if entry.value().is_none() {
                        None
                    } else {
                        Some(TransactionEntry::Stream(entry))
                    }
                }),
        })
    }

    /// scan records with primary keys in the `range`
    pub fn scan<'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
    ) -> Scan<'scan, R> {
        Scan::new(
            &self.share,
            &self.manager,
            range,
            self.ts,
            &self.version,
            Box::new(move |projection_mask: Option<ProjectionMask>| {
                let mut transaction_scan = TransactionScan {
                    inner: self.local.range(range),
                    ts: self.ts,
                }
                .into();
                if let Some(mask) = projection_mask {
                    transaction_scan = MemProjectionStream::new(transaction_scan, mask).into();
                }
                Some(transaction_scan)
            }),
        )
    }

    /// insert a sequence of data as a single batch on this transaction
    pub fn insert(&mut self, value: R) {
        self.entry(value.key().to_key(), Some(value))
    }

    /// delete the record with the primary key as the `key` on this transaction
    pub fn remove(&mut self, key: R::Key) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: R::Key, value: Option<R>) {
        match self.local.entry(key) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    /// commit the data in the [`Transaction`] to the corresponding
    /// [`DB`](crate::DB)
    pub async fn commit(mut self) -> Result<(), CommitError<R>> {
        let mut _key_guards = Vec::new();

        for (key, _) in self.local.iter() {
            // SAFETY: Error is Never
            _key_guards.push(
                self.lock_map
                    .async_lock(key.clone(), AsyncLimit::no_limit())
                    .await
                    .unwrap(),
            );
        }
        for (key, _) in self.local.iter() {
            if self.share.check_conflict(key, self.ts) {
                return Err(CommitError::WriteConflict(key.clone()));
            }
        }

        let len = self.local.len();
        let is_excess = match len {
            0 => false,
            1 => {
                let new_ts = self.version.increase_ts();
                let (key, record) = self.local.pop_first().unwrap();
                Self::append(&self.share, LogType::Full, key, record, new_ts).await?
            }
            _ => {
                let new_ts = self.version.increase_ts();
                let mut iter = self.local.into_iter();

                let (key, record) = iter.next().unwrap();
                Self::append(&self.share, LogType::First, key, record, new_ts).await?;

                for (key, record) in (&mut iter).take(len - 2) {
                    Self::append(&self.share, LogType::Middle, key, record, new_ts).await?;
                }

                let (key, record) = iter.next().unwrap();
                Self::append(&self.share, LogType::Last, key, record, new_ts).await?
            }
        };
        if is_excess {
            let _ = self.share.compaction_tx.try_send(CompactTask::Freeze);
        }
        Ok(())
    }

    async fn append(
        schema: &Schema<R>,
        log_ty: LogType,
        key: <R as Record>::Key,
        record: Option<R>,
        new_ts: Timestamp,
    ) -> Result<bool, CommitError<R>> {
        Ok(match record {
            Some(record) => schema.write(log_ty, record, new_ts).await?,
            None => schema.remove(log_ty, key, new_ts).await?,
        })
    }
}

pub enum TransactionEntry<'entry, R>
where
    R: Record,
{
    Stream(stream::Entry<'entry, R>),
    Local(R::Ref<'entry>),
}

impl<'entry, R> TransactionEntry<'entry, R>
where
    R: Record,
{
    pub fn get(&self) -> R::Ref<'_> {
        match self {
            TransactionEntry::Stream(entry) => entry.value().unwrap(),
            TransactionEntry::Local(value) => {
                // Safety: shorter lifetime must be safe
                unsafe { transmute::<R::Ref<'entry>, R::Ref<'_>>(value.clone()) }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CommitError<R>
where
    R: Record,
{
    #[error("transaction io error {:?}", .0)]
    Io(#[from] io::Error),
    #[error("transaction parquet error {:?}", .0)]
    Parquet(#[from] ParquetError),
    #[error("transaction database error {:?}", .0)]
    Database(#[from] DbError<R>),
    #[error("transaction write conflict: {:?}", .0)]
    WriteConflict(R::Key),
    #[error("Failed to send compact task")]
    SendCompactTaskError(#[from] SendError<CompactTask>),
    #[error("Channel is closed")]
    ChannelClose,
}

#[cfg(test)]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use fusio::{local::TokioFs, path::Path};
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version,
        executor::tokio::TokioExecutor,
        record::{
            runtime::{Column, Datatype, DynRecord},
            ColumnDesc,
        },
        fs::manager::StoreManager,
        tests::{build_db, build_schema, Test},
        transaction::CommitError,
        version::TransactionTs,
        DbOption, Projection, DB,
    };

    #[tokio::test]
    async fn transaction_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);

        let db = DB::<String, TokioExecutor>::new(
            DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap()),
            TokioExecutor::new(),
            manager,
        )
        .await
        .unwrap();
        {
            let mut txn1 = db.transaction().await;
            txn1.insert("foo".to_string());

            let txn2 = db.transaction().await;
            assert!(txn2
                .get(&"foo".to_string(), Projection::All)
                .await
                .unwrap()
                .is_none());

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();
        }

        {
            let txn3 = db.transaction().await;
            assert!(txn3
                .get(&"foo".to_string(), Projection::All)
                .await
                .unwrap()
                .is_some());
            txn3.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn transaction_get() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(Arc::new(TokioFs), vec![]));
        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (_, version) = build_version(&option, &manager).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::new(),
            schema,
            version,
            manager,
        )
        .await
        .unwrap();

        {
            let _ = db.version_set.increase_ts();
        }
        let name = "erika".to_string();
        {
            let mut txn = db.transaction().await;
            {
                let entry = txn.get(&name, Projection::All).await.unwrap();
                assert_eq!(entry.as_ref().unwrap().get().vu32.unwrap(), 5);
            }
            txn.insert(Test {
                vstring: name.clone(),
                vu32: 50,
                vbool: Some(false),
            });

            txn.commit().await.unwrap();
        }
        {
            let mut txn = db.transaction().await;
            // rewrite data in SSTable
            for i in (1..6).step_by(2) {
                txn.insert(Test {
                    vstring: (i as usize).to_string(),
                    vu32: i * 10 + i,
                    vbool: Some(false),
                });
            }
            {
                // seek in mutable table before immutable
                let entry = txn.get(&name, Projection::All).await.unwrap();
                assert_eq!(entry.as_ref().unwrap().get().vu32.unwrap(), 50);

                for i in 1..6 {
                    let key = i.to_string();
                    let entry = txn.get(&key, Projection::All).await.unwrap();
                    assert!(entry.is_some());
                    if i % 2 == 1 {
                        // seek in local buffer first
                        assert_eq!(entry.as_ref().unwrap().get().vu32.unwrap(), i * 10 + i);
                        assert!(!entry.unwrap().get().vbool.unwrap());
                    } else {
                        // mem-table will miss, so seek in SSTable
                        assert_eq!(entry.as_ref().unwrap().get().vu32.unwrap(), 0);
                        assert!(entry.unwrap().get().vbool.unwrap());
                    }
                }
                // seek miss
                assert!(txn
                    .get(&"benn".to_owned(), Projection::All)
                    .await
                    .unwrap()
                    .is_none())
            }
        }
    }

    #[tokio::test]
    async fn write_conflicts() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);
        let option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());

        let db = DB::<String, TokioExecutor>::new(option, TokioExecutor::new(), manager)
            .await
            .unwrap();

        let mut txn = db.transaction().await;
        txn.insert(0.to_string());
        txn.insert(1.to_string());
        txn.commit().await.unwrap();

        let mut txn_0 = db.transaction().await;
        let mut txn_1 = db.transaction().await;
        let mut txn_2 = db.transaction().await;

        txn_0.insert(1.to_string());
        txn_1.insert(1.to_string());
        txn_1.insert(2.to_string());
        txn_2.insert(2.to_string());

        txn_0.commit().await.unwrap();

        if let Err(CommitError::WriteConflict(conflict_key)) = txn_1.commit().await {
            assert_eq!(conflict_key, 1.to_string());
            txn_2.commit().await.unwrap();
            return;
        }
        unreachable!();
    }

    #[tokio::test]
    async fn transaction_projection() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);
        let option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());

        let db = DB::<Test, TokioExecutor>::new(option, TokioExecutor::new(), manager)
            .await
            .unwrap();

        let mut txn1 = db.transaction().await;
        txn1.insert(Test {
            vstring: 0.to_string(),
            vu32: 0,
            vbool: Some(true),
        });

        let key = 0.to_string();
        let entry = txn1.get(&key, Projection::All).await.unwrap().unwrap();

        assert_eq!(entry.get().vstring, 0.to_string());
        assert_eq!(entry.get().vu32, Some(0));
        assert_eq!(entry.get().vbool, Some(true));
        drop(entry);

        txn1.commit().await.unwrap();
    }

    #[tokio::test]
    async fn transaction_scan() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(Arc::new(TokioFs), vec![]));
        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (_, version) = build_version(&option, &manager).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::new(),
            schema,
            version,
            manager,
        )
        .await
        .unwrap();

        {
            // to increase timestamps to 1 because the data ts built in advance is 1
            db.version_set.increase_ts();
        }
        let mut txn = db.transaction().await;
        txn.insert(Test {
            vstring: "king".to_string(),
            vu32: 8,
            vbool: Some(true),
        });

        let mut stream = txn
            .scan((Bound::Unbounded, Bound::Unbounded))
            .projection(vec![1])
            .take()
            .await
            .unwrap();

        let entry_0 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_0.key().value, "1");
        assert!(entry_0.value().unwrap().vbool.is_none());
        let entry_1 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_1.key().value, "2");
        assert!(entry_1.value().unwrap().vbool.is_none());
        let entry_2 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_2.key().value, "3");
        assert!(entry_2.value().unwrap().vbool.is_none());
        let entry_3 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_3.key().value, "4");
        assert!(entry_3.value().unwrap().vbool.is_none());
        let entry_4 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_4.key().value, "5");
        assert!(entry_4.value().unwrap().vbool.is_none());
        let entry_5 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_5.key().value, "6");
        assert!(entry_5.value().unwrap().vbool.is_none());
        let entry_6 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_6.key().value, "7");
        assert!(entry_6.value().unwrap().vbool.is_none());
        let entry_7 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_7.key().value, "8");
        assert!(entry_7.value().unwrap().vbool.is_none());
        let entry_8 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_8.key().value, "9");
        assert!(entry_8.value().unwrap().vbool.is_none());
        let entry_9 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_9.key().value, "alice");
        let entry_10 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_10.key().value, "ben");
        let entry_11 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_11.key().value, "carl");
        let entry_12 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_12.key().value, "dice");
        let entry_13 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_13.key().value, "erika");
        let entry_14 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_14.key().value, "funk");
        let entry_15 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_15.key().value, "king");
    }

    #[tokio::test]
    async fn test_transaction_scan_bound() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(Arc::new(TokioFs), vec![]));
        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (_, version) = build_version(&option, &manager).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::new(),
            schema,
            version,
            manager,
        )
        .await
        .unwrap();
        {
            // to increase timestamps to 1 because the data ts built in advance is 1
            db.version_set.increase_ts();
        }

        // skip timestamp
        let txn = db.transaction().await;
        txn.commit().await.unwrap();

        // test inmem
        {
            let txn2 = db.transaction().await;
            let lower = "ben".into();
            let upper = "dice".into();
            {
                let mut stream = txn2
                    .scan((Bound::Included(&lower), Bound::Included(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "ben");
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "carl");
                let entry_2 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_2.key().value, "dice");
                assert!(stream.next().await.is_none());
            }

            {
                let mut stream = txn2
                    .scan((Bound::Included(&lower), Bound::Excluded(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();
                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "ben");
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "carl");
                assert!(stream.next().await.is_none());
            }

            {
                let mut stream = txn2
                    .scan((Bound::Excluded(&lower), Bound::Included(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();
                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "carl");
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "dice");
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn2
                    .scan((Bound::Excluded(&lower), Bound::Excluded(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();
                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "carl");
                assert!(stream.next().await.is_none());
            }
        }
        // test SSTable
        {
            let txn3 = db.transaction().await;
            let lower = "1".into();
            let upper = "2".into();
            {
                let mut stream = txn3
                    .scan((Bound::Included(&lower), Bound::Included(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "1");
                assert!(entry_0.value().unwrap().vbool.is_none());
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "2");
                assert!(entry_1.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Included(&lower), Bound::Excluded(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "1");
                assert!(entry_0.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Excluded(&lower), Bound::Included(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "2");
                assert!(entry_0.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Excluded(&lower), Bound::Excluded(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();

                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Unbounded, Bound::Excluded(&upper)))
                    .projection(vec![1])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "1");
                assert!(entry_0.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_transaction_scan_limit() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(Arc::new(TokioFs), vec![]));
        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (_, version) = build_version(&option, &manager).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::new(),
            schema,
            version,
            manager,
        )
        .await
        .unwrap();

        let txn = db.transaction().await;
        txn.commit().await.unwrap();

        {
            let txn2 = db.transaction().await;
            {
                let mut stream = txn2
                    .scan((Bound::Unbounded, Bound::Unbounded))
                    .limit(1)
                    .take()
                    .await
                    .unwrap();

                assert!(stream.next().await.is_some());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn2
                    .scan((Bound::Unbounded, Bound::Unbounded))
                    .limit(0)
                    .take()
                    .await
                    .unwrap();

                assert!(stream.next().await.is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_dyn_record() {
        let descs = vec![
            ColumnDesc::new("age".to_string(), Datatype::Int8, false),
            ColumnDesc::new("height".to_string(), Datatype::Int16, true),
            ColumnDesc::new("weight".to_string(), Datatype::Int32, false),
        ];

        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::with_path(temp_dir.path(), "age".to_string(), 0);
        let db = DB::with_schema(option, TokioExecutor::default(), descs, 0)
            .await
            .unwrap();

        db.insert(DynRecord::new(
            vec![
                Column::new(Datatype::Int8, "age".to_string(), Arc::new(1_i8), false),
                Column::new(
                    Datatype::Int16,
                    "height".to_string(),
                    Arc::new(Some(180_i16)),
                    true,
                ),
                Column::new(
                    Datatype::Int32,
                    "weight".to_string(),
                    Arc::new(56_i32),
                    false,
                ),
            ],
            0,
        ))
        .await
        .unwrap();

        let txn = db.transaction().await;
        {
            let key = Column::new(Datatype::Int8, "age".to_string(), Arc::new(1_i8), false);

            let record_ref = txn.get(&key, Projection::All).await.unwrap();
            assert!(record_ref.is_some());
            let res = record_ref.unwrap();
            let record_ref = res.get();

            assert_eq!(record_ref.columns.len(), 3);
            let col = record_ref.columns.first().unwrap();
            assert_eq!(col.datatype, Datatype::Int8);
            let name = col.value.as_ref().downcast_ref::<i8>();
            assert!(name.is_some());
            assert_eq!(*name.unwrap(), 1);

            let col = record_ref.columns.get(1).unwrap();
            let height = col.value.as_ref().downcast_ref::<Option<i16>>();
            assert!(height.is_some());
            assert_eq!(*height.unwrap(), Some(180_i16));

            let col = record_ref.columns.get(2).unwrap();
            let weight = col.value.as_ref().downcast_ref::<Option<i32>>();
            assert!(weight.is_some());
            assert_eq!(*weight.unwrap(), Some(56_i32));
        }
        {
            let mut scan = txn
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(vec![0, 1, 2])
                .take()
                .await
                .unwrap();
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                assert_eq!(entry.value().unwrap().primary_index, 0);
                assert_eq!(entry.value().unwrap().columns.len(), 3);
                let columns = entry.value().unwrap().columns;
                dbg!(columns.clone());

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype, Datatype::Int8);
                assert_eq!(
                    *primary_key_col.value.as_ref().downcast_ref::<i8>().unwrap(),
                    1
                );

                let col = columns.get(1).unwrap();
                assert_eq!(col.datatype, Datatype::Int16);
                assert_eq!(
                    *col.value.as_ref().downcast_ref::<Option<i16>>().unwrap(),
                    Some(180)
                );

                let col = columns.get(2).unwrap();
                assert_eq!(col.datatype, Datatype::Int32);
                let weight = col.value.as_ref().downcast_ref::<Option<i32>>();
                assert!(weight.is_some());
                assert_eq!(*weight.unwrap(), Some(56_i32));
            }
        }
    }
}
