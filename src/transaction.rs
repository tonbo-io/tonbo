use std::{
    collections::{
        btree_map::{Entry, Range},
        BTreeMap, Bound,
    },
    io,
    mem::transmute,
    sync::Arc,
};

use common::{Key, KeyRef, PrimaryKey, PrimaryKeyRef};
use flume::SendError;
use lockable::AsyncLimit;
use parquet::{
    arrow::{ArrowSchemaConverter, ProjectionMask},
    errors::ParquetError,
};
use thiserror::Error;

use crate::{
    compaction::CompactTask,
    record::RecordRef,
    snapshot::Snapshot,
    stream::{self, mem_projection::MemProjectionStream},
    timestamp::{Timestamp, Ts},
    wal::log::LogType,
    DbError, DbStorage, LockMap, Projection, Record, Scan,
};

pub(crate) struct TransactionScan<'scan, R: Record> {
    inner: Range<'scan, PrimaryKey, Option<R>>,
    ts: Timestamp,
}

impl<'scan, R> Iterator for TransactionScan<'scan, R>
where
    R: Record,
{
    type Item = (Ts<PrimaryKeyRef<'scan>>, &'scan Option<R>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(key, value)| (Ts::new(key.as_key_ref(), self.ts), value))
    }
}
/// optimistic ACID transaction, open with
/// [`DB::transaction`](crate::DB::transaction) method
///
/// Transaction will store all mutations in local [`BTreeMap`] and only write to memtable when
/// committed successfully. Otherwise, all mutations will be rolled back.
pub struct Transaction<'txn, R>
where
    R: Record,
{
    local: BTreeMap<PrimaryKey, Option<R>>,
    snapshot: Snapshot<'txn, R>,
    lock_map: LockMap<PrimaryKey>,
}

impl<'txn, R> Transaction<'txn, R>
where
    R: Record + Send,
{
    pub(crate) fn new(snapshot: Snapshot<'txn, R>, lock_map: LockMap<PrimaryKey>) -> Self {
        Self {
            local: BTreeMap::new(),
            snapshot,
            lock_map,
        }
    }

    /// get the record with `key` as the primary key and get only the data specified in
    /// [`Projection`]
    pub async fn get<'get>(
        &'get self,
        key: &'get PrimaryKey,
        projection: Projection<'get>,
    ) -> Result<Option<TransactionEntry<'get, R>>, DbError> {
        Ok(match self.local.get(key) {
            Some(v) => v.as_ref().map(|v| {
                let mut record_ref = v.as_record_ref();
                if let Projection::Parts(projection) = projection {
                    let primary_key_index =
                        self.snapshot.schema().record_schema.primary_key_index();
                    let schema = self.snapshot.schema().record_schema.arrow_schema();
                    let mut projection = projection
                        .iter()
                        .map(|name| {
                            schema
                                .index_of(name)
                                .unwrap_or_else(|_| panic!("unexpected field {}", name))
                        })
                        .collect::<Vec<usize>>();

                    let mut fixed_projection = vec![0, 1, primary_key_index];
                    fixed_projection.append(&mut projection);
                    fixed_projection.dedup();

                    let mask = ProjectionMask::roots(
                        &ArrowSchemaConverter::new().convert(schema).unwrap(),
                        fixed_projection.clone(),
                    );
                    record_ref.projection(&mask);
                }
                TransactionEntry::Local(record_ref)
            }),
            None => self
                .snapshot
                .get(key, projection)
                .await?
                .map(TransactionEntry::Stream),
        })
    }

    /// scan records with primary keys in the `range`, return a [`Scan`] that can be convert to a
    /// [`futures_core::Stream`] by using [`Scan::take`].
    ///
    /// [`Scan::projection`] and [`Scan::limit`] can be used to push down projection and limit.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut txn = db.transaction().await;
    /// txn.scan((Bound::Included("Alice"), Bound::Excluded("Bob")))
    ///     // only read primary key and `age`
    ///     .projection(&["age"])
    ///     // read at most 10 records
    ///     .limit(10)
    ///     .take()
    ///     .await
    ///     .unwrap();
    ///
    /// while let Some(entry) = scan_stream.next().await.transpose().unwrap() {
    ///     println!("{:#?}", entry.value())
    /// }
    /// ```
    pub fn scan<'scan, 'range>(
        &'scan self,
        range: (Bound<&'range PrimaryKey>, Bound<&'range PrimaryKey>),
    ) -> Scan<'scan, 'range, R> {
        let ts = self.snapshot.ts();
        let inner = self.local.range(range);
        self.snapshot._scan(
            range,
            Box::new(move |projection_mask: Option<ProjectionMask>| {
                let mut transaction_scan = TransactionScan { inner, ts }.into();
                if let Some(mask) = projection_mask {
                    transaction_scan = MemProjectionStream::new(transaction_scan, mask).into();
                }
                Some(transaction_scan)
            }),
        )
    }

    /// insert a sequence of data as a single batch on this transaction
    pub fn insert(&mut self, value: R) {
        // Convert R::Key to PrimaryKey for storage
        // let key = PrimaryKey::new(vec![Arc::new(value.key().to_key())]);
        let key = value.key().to_key();
        self.entry(key, Some(value))
    }

    /// delete the record with the primary key as the `key` on this transaction
    pub fn remove(&mut self, key: PrimaryKey) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: PrimaryKey, value: Option<R>) {
        match self.local.entry(key) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    /// commit the data in the [`Transaction`] to the corresponding
    /// [`DB`](crate::DB)
    ///
    /// # Error
    /// This function will return an error if the mutation in the transaction conflict with
    /// other committed transaction
    pub async fn commit(mut self) -> Result<(), CommitError> {
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
            if self
                .snapshot
                .schema()
                .check_conflict(key, self.snapshot.ts())
            {
                return Err(CommitError::WriteConflict(key.clone()));
            }
        }

        let len = self.local.len();
        let is_excess = match len {
            0 => false,
            1 => {
                let new_ts = self.snapshot.increase_ts();
                let (key, record) = self.local.pop_first().unwrap();
                Self::append(self.snapshot.schema(), LogType::Full, key, record, new_ts).await?
            }
            _ => {
                let new_ts = self.snapshot.increase_ts();
                let mut iter = self.local.into_iter();

                let (key, record) = iter.next().unwrap();
                Self::append(self.snapshot.schema(), LogType::First, key, record, new_ts).await?;

                for (key, record) in (&mut iter).take(len - 2) {
                    Self::append(self.snapshot.schema(), LogType::Middle, key, record, new_ts)
                        .await?;
                }

                let (key, record) = iter.next().unwrap();
                Self::append(self.snapshot.schema(), LogType::Last, key, record, new_ts).await?
            }
        };
        if is_excess {
            let _ = self
                .snapshot
                .schema()
                .compaction_tx
                .try_send(CompactTask::Freeze);
        }
        Ok(())
    }

    async fn append(
        schema: &DbStorage<R>,
        log_ty: LogType,
        key: PrimaryKey,
        record: Option<R>,
        new_ts: Timestamp,
    ) -> Result<bool, CommitError> {
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
    /// get the [`RecordRef`] inside the entry.
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
pub enum CommitError {
    #[error("transaction io error {:?}", .0)]
    Io(#[from] io::Error),
    #[error("transaction parquet error {:?}", .0)]
    Parquet(#[from] ParquetError),
    #[error("transaction database error {:?}", .0)]
    Database(#[from] DbError),
    #[error("transaction write conflict: {:?}", .0)]
    WriteConflict(PrimaryKey),
    #[error("Failed to send compact task")]
    SendCompactTaskError(#[from] SendError<CompactTask>),
    #[error("Channel is closed")]
    ChannelClose,
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use common::{datatype::DataType, AsValue, PrimaryKey};
    use fusio::path::Path;
    use fusio_dispatch::FsOptions;
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version,
        executor::tokio::TokioExecutor,
        fs::manager::StoreManager,
        record::{
            // runtime::{test::test_dyn_item_schema, DynRecord},
            runtime::test::test_dyn_item_schema,
            test::string_arrow_schema,
            DynRecord,
            Schema,
        },
        tests::{build_db, build_schema, Test},
        transaction::CommitError,
        DbOption, Projection, DB,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_read_write() {
        let temp_dir = TempDir::new().unwrap();

        let schema = Schema::from_arrow_schema(string_arrow_schema(), 0).unwrap();
        let db = DB::<String, TokioExecutor>::new(
            DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap()),
            TokioExecutor::current(),
            schema,
        )
        .await
        .unwrap();
        {
            let mut txn1 = db.transaction().await;
            txn1.insert("foo".to_string());

            let txn2 = db.transaction().await;
            let key = PrimaryKey::new(vec![Arc::new("foo".to_string())]);
            assert!(txn2.get(&key, Projection::All).await.unwrap().is_none());

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();
        }

        {
            let txn3 = db.transaction().await;
            let key = PrimaryKey::new(vec![Arc::new("foo".to_string())]);
            assert!(txn3.get(&key, Projection::All).await.unwrap().is_some());
            txn3.commit().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_get() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let record_schema = Arc::new(Test::schema());
        let (_, version) = build_version(&option, &manager, &record_schema).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::current(),
            schema,
            record_schema.clone(),
            version,
            manager,
        )
        .await
        .unwrap();

        {
            let _ = db.ctx.increase_ts();
        }
        let name = PrimaryKey::new(vec![Arc::new("erika".to_string())]);
        // let name = "erika".to_string();
        {
            let mut txn = db.transaction().await;
            {
                let entry = txn.get(&name, Projection::All).await.unwrap();

                assert_eq!(entry.as_ref().unwrap().get().vu32.unwrap(), 5);
            }
            txn.insert(Test {
                vstring: name.get(0).unwrap().as_string().clone(),
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
                    // let key = i.to_string();
                    let key = PrimaryKey::new(vec![Arc::new(i.to_string())]);
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
                let key = PrimaryKey::new(vec![Arc::new("benn".to_owned())]);
                assert!(txn.get(&key, Projection::All).await.unwrap().is_none())
            }
        }
    }

    // https://github.com/tonbo-io/tonbo/issues/352
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_remove() {
        let temp_dir = TempDir::new().unwrap();

        let schema = Schema::from_arrow_schema(string_arrow_schema(), 0).unwrap();
        let db = DB::<String, TokioExecutor>::new(
            DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap()),
            TokioExecutor::current(),
            schema,
        )
        .await
        .unwrap();

        // Insert a record and commit
        {
            let mut txn = db.transaction().await;
            txn.insert("foo".to_string());
            txn.commit().await.unwrap();
        }
        // In a new transaction, remove the record and check visibility
        {
            let mut txn = db.transaction().await;
            // let key = "foo".to_string();
            let key = PrimaryKey::new(vec![Arc::new("foo".to_string())]);

            // Verify the record exists before removal
            assert!(txn.get(&key, Projection::All).await.unwrap().is_some());

            // Remove the record
            txn.remove(key.clone());

            // The record should NOT be visible after removal in the same transaction
            let result_after = txn.get(&key, Projection::All).await.unwrap();

            assert!(
                result_after.is_none(),
                "Record should not be visible after removal in the same transaction"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_conflicts() {
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());

        let schema = Schema::from_arrow_schema(string_arrow_schema(), 0).unwrap();
        let db = DB::<String, TokioExecutor>::new(option, TokioExecutor::current(), schema)
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

        let key = PrimaryKey::new(vec![Arc::new(1.to_string())]);

        if let Err(CommitError::WriteConflict(conflict_key)) = txn_1.commit().await {
            // assert_eq!(conflict_key, 1.to_string());
            assert_eq!(&conflict_key, &key);
            txn_2.commit().await.unwrap();
            return;
        }
        unreachable!();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_projection() {
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());

        let schema = Test::schema();
        let db = DB::<Test, TokioExecutor>::new(option, TokioExecutor::current(), schema)
            .await
            .unwrap();

        let mut txn1 = db.transaction().await;
        txn1.insert(Test {
            vstring: 0.to_string(),
            vu32: 0,
            vbool: Some(true),
        });

        // let key = 0.to_string();
        let key = PrimaryKey::new(vec![Arc::new(0.to_string())]);

        let entry = txn1.get(&key, Projection::All).await.unwrap().unwrap();

        assert_eq!(entry.get().vstring, 0.to_string());
        assert_eq!(entry.get().vu32, Some(0));
        assert_eq!(entry.get().vbool, Some(true));
        drop(entry);

        let entry = txn1
            .get(&key, Projection::Parts(vec!["vstring", "vu32"]))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(entry.get().vstring, 0.to_string());
        assert_eq!(entry.get().vu32, Some(0));
        assert_eq!(entry.get().vbool, None);
        drop(entry);

        txn1.commit().await.unwrap();

        let txn2 = db.transaction().await;
        let entry = txn2
            .get(&key, Projection::Parts(vec!["vstring", "vu32"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.get().vstring, 0.to_string());
        assert_eq!(entry.get().vu32, Some(0));
        assert_eq!(entry.get().vbool, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_scan() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let record_schema = Arc::new(Test::schema());
        let (_, version) = build_version(&option, &manager, &record_schema).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();

        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::current(),
            schema,
            record_schema.clone(),
            version,
            manager,
        )
        .await
        .unwrap();

        {
            // to increase timestamps to 1 because the data ts built in advance is 1
            db.ctx.increase_ts();
        }
        let mut txn = db.transaction().await;
        txn.insert(Test {
            vstring: "king".to_string(),
            vu32: 8,
            vbool: Some(true),
        });

        let mut stream = txn
            .scan((Bound::Unbounded, Bound::Unbounded))
            .projection(&["vu32"])
            .take()
            .await
            .unwrap();

        let entry_0 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_0.key().value, "1".into());
        assert!(entry_0.value().unwrap().vbool.is_none());
        let entry_1 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_1.key().value, "2".into());
        assert!(entry_1.value().unwrap().vbool.is_none());
        let entry_2 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_2.key().value, "3".into());
        assert!(entry_2.value().unwrap().vbool.is_none());
        let entry_3 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_3.key().value, "4".into());
        assert!(entry_3.value().unwrap().vbool.is_none());
        let entry_4 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_4.key().value, "5".into());
        assert!(entry_4.value().unwrap().vbool.is_none());
        let entry_5 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_5.key().value, "6".into());
        assert!(entry_5.value().unwrap().vbool.is_none());
        let entry_6 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_6.key().value, "7".into());
        assert!(entry_6.value().unwrap().vbool.is_none());
        let entry_7 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_7.key().value, "8".into());
        assert!(entry_7.value().unwrap().vbool.is_none());
        let entry_8 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_8.key().value, "9".into());
        assert!(entry_8.value().unwrap().vbool.is_none());
        let entry_9 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_9.key().value, "alice".into());
        let entry_10 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_10.key().value, "ben".into());
        let entry_11 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_11.key().value, "carl".into());
        let entry_12 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_12.key().value, "dice".into());
        let entry_13 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_13.key().value, "erika".into());
        let entry_14 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_14.key().value, "funk".into());
        let entry_15 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_15.key().value, "king".into());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_scan_bound() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let record_schema = Arc::new(Test::schema());
        let (_, version) = build_version(&option, &manager, &record_schema).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::current(),
            schema,
            record_schema.clone(),
            version,
            manager,
        )
        .await
        .unwrap();
        {
            // to increase timestamps to 1 because the data ts built in advance is 1
            db.ctx.increase_ts();
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
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "ben".into());
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "carl".into());
                let entry_2 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_2.key().value, "dice".into());
                assert!(stream.next().await.is_none());
            }

            {
                let mut stream = txn2
                    .scan((Bound::Included(&lower), Bound::Excluded(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();
                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "ben".into());
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "carl".into());
                assert!(stream.next().await.is_none());
            }

            {
                let mut stream = txn2
                    .scan((Bound::Excluded(&lower), Bound::Included(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();
                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "carl".into());
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "dice".into());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn2
                    .scan((Bound::Excluded(&lower), Bound::Excluded(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();
                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "carl".into());
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
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "1".into());
                assert!(entry_0.value().unwrap().vbool.is_none());
                let entry_1 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_1.key().value, "2".into());
                assert!(entry_1.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Included(&lower), Bound::Excluded(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "1".into());
                assert!(entry_0.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Excluded(&lower), Bound::Included(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "2".into());
                assert!(entry_0.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Excluded(&lower), Bound::Excluded(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();

                assert!(stream.next().await.is_none());
            }
            {
                let mut stream = txn3
                    .scan((Bound::Unbounded, Bound::Excluded(&upper)))
                    .projection(&["vu32"])
                    .take()
                    .await
                    .unwrap();

                let entry_0 = stream.next().await.unwrap().unwrap();
                assert_eq!(entry_0.key().value, "1".into());
                assert!(entry_0.value().unwrap().vbool.is_none());
                assert!(stream.next().await.is_none());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_scan_limit() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let record_schema = Arc::new(Test::schema());
        let (_, version) = build_version(&option, &manager, &record_schema).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::current(),
            schema,
            record_schema.clone(),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dyn_record() {
        let temp_dir = TempDir::new().unwrap();
        let schema = test_dyn_item_schema();
        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());

        let db = DB::new(option, TokioExecutor::current(), schema)
            .await
            .unwrap();

        db.insert(DynRecord::new(
            vec![Arc::new(1_i8), Arc::new(Some(180_i16)), Arc::new(56_i32)],
            0,
        ))
        .await
        .unwrap();

        let txn = db.transaction().await;
        {
            let key = PrimaryKey::new(vec![Arc::new(1_i8)]);

            let record_ref = txn.get(&key, Projection::All).await.unwrap();
            assert!(record_ref.is_some());
            let res = record_ref.unwrap();
            let record_ref = res.get();

            assert_eq!(record_ref.columns.len(), 3);
            let col = record_ref.columns.first().unwrap();
            assert_eq!(col.data_type(), DataType::Int8);
            let name = col.as_i8();
            assert_eq!(*name, 1);

            let col = record_ref.columns.get(1).unwrap();
            let height = col.as_i16_opt();
            assert_eq!(*height, Some(180_i16));

            let col = record_ref.columns.get(2).unwrap();
            let weight = col.as_i32_opt();
            assert!(weight.is_some());
            assert_eq!(*weight, Some(56_i32));
        }
        {
            let mut scan = txn
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(&["id", "age", "height"])
                .take()
                .await
                .unwrap();
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                assert_eq!(entry.value().unwrap().primary_index, 0);
                assert_eq!(entry.value().unwrap().columns.len(), 3);
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.data_type(), DataType::Int8);
                assert_eq!(*primary_key_col.as_i8(), 1);

                let col = columns.get(1).unwrap();
                assert_eq!(col.data_type(), DataType::Int16);
                assert_eq!(*col.as_i16_opt(), Some(180));

                let col = columns.get(2).unwrap();
                assert_eq!(col.data_type(), DataType::Int32);
                let weight = col.as_i32_opt();
                assert_eq!(*weight, Some(56_i32));
            }
        }
    }
}
