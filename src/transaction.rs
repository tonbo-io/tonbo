use std::{
    collections::{
        btree_map::{Entry, Range},
        BTreeMap, Bound,
    },
    io,
    iter::Rev,
    mem::transmute,
};

use flume::SendError;
use lockable::AsyncLimit;
use parquet::{
    arrow::{ArrowSchemaConverter, ProjectionMask},
    errors::ParquetError,
};
use thiserror::Error;

use crate::{
    compaction::CompactTask,
    inmem::mutable::WriteResult,
    option::Order,
    record::{Key, KeyRef, RecordRef, Schema},
    snapshot::Snapshot,
    stream::{self, mem_projection::MemProjectionStream},
    version::timestamp::{Timestamp, Ts},
    wal::log::LogType,
    DbError, DbStorage, LockMap, Projection, Record, Scan,
};

pub(crate) enum TransactionScanInner<'scan, R: Record> {
    Forward(Range<'scan, <R::Schema as Schema>::Key, Option<R>>),
    Reverse(Rev<Range<'scan, <R::Schema as Schema>::Key, Option<R>>>),
}

pub(crate) struct TransactionScan<'scan, R: Record> {
    inner: TransactionScanInner<'scan, R>,
    ts: Timestamp,
}

impl<'scan, R> Iterator for TransactionScan<'scan, R>
where
    R: Record,
{
    type Item = (
        Ts<<<R::Schema as Schema>::Key as Key>::Ref<'scan>>,
        &'scan Option<R>,
    );

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            TransactionScanInner::Forward(iter) => iter
                .next()
                .map(|(key, value)| (Ts::new(key.as_key_ref(), self.ts), value)),
            TransactionScanInner::Reverse(iter) => iter
                .next()
                .map(|(key, value)| (Ts::new(key.as_key_ref(), self.ts), value)),
        }
    }
}
/// Optimistic ACID transaction, open with
/// [`DB::transaction`](crate::DB::transaction) method
///
/// Transaction will store all mutations in local [`BTreeMap`] and only write to memtable when
/// committed successfully. Otherwise, all mutations will be rolled back.
pub struct Transaction<'txn, R, E>
where
    R: Record,
    <R::Schema as Schema>::Columns: Send + Sync,
    E: crate::executor::Executor,
    E::RwLock<crate::DbStorage<R>>: 'txn,
{
    local: BTreeMap<<R::Schema as Schema>::Key, Option<R>>,
    snapshot: Snapshot<'txn, R, E>,
    lock_map: LockMap<<R::Schema as Schema>::Key>,
}

impl<'txn, R, E> Transaction<'txn, R, E>
where
    R: Record + Send,
    <R::Schema as Schema>::Columns: Send + Sync,
    E: crate::executor::Executor,
    E::RwLock<crate::DbStorage<R>>: 'txn,
{
    pub(crate) fn new(
        snapshot: Snapshot<'txn, R, E>,
        lock_map: LockMap<<R::Schema as Schema>::Key>,
    ) -> Self {
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
        key: &'get <R::Schema as Schema>::Key,
        projection: Projection<'get>,
    ) -> Result<Option<TransactionEntry<'get, R>>, DbError> {
        Ok(match self.local.get(key) {
            Some(v) => v.as_ref().map(|v| {
                let mut record_ref = v.as_record_ref();
                if let Projection::Parts(projection) = projection {
                    let pk_indices = self
                        .snapshot
                        .mem_storage()
                        .record_schema
                        .primary_key_indices();
                    let schema = self.snapshot.mem_storage().record_schema.arrow_schema();
                    let mut projection = projection
                        .iter()
                        .map(|name| {
                            schema
                                .index_of(name)
                                .unwrap_or_else(|_| panic!("unexpected field {name}"))
                        })
                        .collect::<Vec<usize>>();

                    let mut fixed_projection = vec![0, 1];
                    fixed_projection.extend_from_slice(pk_indices);
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
        range: (
            Bound<&'range <R::Schema as Schema>::Key>,
            Bound<&'range <R::Schema as Schema>::Key>,
        ),
    ) -> Scan<'scan, 'range, R> {
        let ts = self.snapshot.ts();
        let local = &self.local;
        self.snapshot._scan(
            range,
            Box::new(
                move |projection_mask: Option<ProjectionMask>, order: Option<Order>| {
                    let inner = if order == Some(Order::Desc) {
                        TransactionScanInner::Reverse(local.range(range).rev())
                    } else {
                        TransactionScanInner::Forward(local.range(range))
                    };
                    let mut transaction_scan = TransactionScan { inner, ts }.into();
                    if let Some(mask) = projection_mask {
                        transaction_scan = MemProjectionStream::new(transaction_scan, mask).into();
                    }
                    Some(transaction_scan)
                },
            ),
        )
    }

    /// insert a sequence of data as a single batch on this transaction
    pub fn insert(&mut self, value: R) {
        self.entry(value.key().to_key(), Some(value))
    }

    /// delete the record with the primary key as the `key` on this transaction
    pub fn remove(&mut self, key: <R::Schema as Schema>::Key) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: <R::Schema as Schema>::Key, value: Option<R>) {
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
            if self
                .snapshot
                .mem_storage()
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
                let write_result = Self::append(
                    self.snapshot.mem_storage(),
                    LogType::Full,
                    key,
                    record,
                    new_ts,
                )
                .await?;
                write_result.needs_compaction()
            }
            _ => {
                let new_ts = self.snapshot.increase_ts();
                let mut iter = self.local.into_iter();

                let (key, record) = iter.next().unwrap();
                Self::append(
                    self.snapshot.mem_storage(),
                    LogType::First,
                    key,
                    record,
                    new_ts,
                )
                .await?;

                for (key, record) in (&mut iter).take(len - 2) {
                    Self::append(
                        self.snapshot.mem_storage(),
                        LogType::Middle,
                        key,
                        record,
                        new_ts,
                    )
                    .await?;
                }

                let (key, record) = iter.next().unwrap();
                let write_result = Self::append(
                    self.snapshot.mem_storage(),
                    LogType::Last,
                    key,
                    record,
                    new_ts,
                )
                .await?;
                write_result.needs_compaction()
            }
        };
        if is_excess {
            let _ = self
                .snapshot
                .mem_storage()
                .compaction_tx
                .try_send(CompactTask::Freeze);
        }
        Ok(())
    }

    async fn append(
        schema: &DbStorage<R>,
        log_ty: LogType,
        key: <R::Schema as Schema>::Key,
        record: Option<R>,
        new_ts: Timestamp,
    ) -> Result<WriteResult, CommitError<R>> {
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
pub enum CommitError<R>
where
    R: Record,
{
    #[error("transaction io error {:?}", .0)]
    Io(#[from] io::Error),
    #[error("transaction parquet error {:?}", .0)]
    Parquet(#[from] ParquetError),
    #[error("transaction database error {:?}", .0)]
    Database(#[from] DbError),
    #[error("transaction write conflict: {:?}", .0)]
    WriteConflict(<R::Schema as Schema>::Key),
    #[error("Failed to send compact task")]
    SendCompactTaskError(#[from] SendError<CompactTask>),
    #[error("Channel is closed")]
    ChannelClose,
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use fusio::path::Path;
    use fusio_dispatch::FsOptions;
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version,
        executor::tokio::TokioExecutor,
        fs::manager::StoreManager,
        inmem::immutable::tests::TestSchema,
        record::{
            dynamic::{test::test_dyn_item_schema, DynRecord, Value},
            test::StringSchema,
            ValueRef,
        },
        tests::{build_db, build_schema, Test},
        transaction::CommitError,
        DbOption, Projection, DB,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_read_write() {
        let temp_dir = TempDir::new().unwrap();

        let db = DB::<String, TokioExecutor>::new(
            DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &StringSchema,
            ),
            TokioExecutor::default(),
            StringSchema,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_get() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
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

        let (_, version) = build_version(&option, &manager, &Arc::new(TestSchema)).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::default(),
            schema,
            Arc::new(TestSchema),
            version,
            manager,
        )
        .await
        .unwrap();

        {
            let _ = db.ctx.increase_ts();
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

    // https://github.com/tonbo-io/tonbo/issues/352
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_remove() {
        let temp_dir = TempDir::new().unwrap();

        let db = DB::<String, TokioExecutor>::new(
            DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &StringSchema,
            ),
            TokioExecutor::default(),
            StringSchema,
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
            let key = "foo".to_string();

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
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        );

        let db = DB::<String, TokioExecutor>::new(option, TokioExecutor::default(), StringSchema)
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

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_projection() {
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );

        let db = DB::<Test, TokioExecutor>::new(option, TokioExecutor::default(), TestSchema)
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
            &TestSchema,
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

        let (_, version) = build_version(&option, &manager, &Arc::new(TestSchema)).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::default(),
            schema,
            Arc::new(TestSchema),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_scan_bound() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
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

        let (_, version) = build_version(&option, &manager, &Arc::new(TestSchema)).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::default(),
            schema,
            Arc::new(TestSchema),
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
                    .projection(&["vu32"])
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
                    .projection(&["vu32"])
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
                    .projection(&["vu32"])
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
                    .projection(&["vu32"])
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
                    .projection(&["vu32"])
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
                    .projection(&["vu32"])
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
                assert_eq!(entry_0.key().value, "1");
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
            &TestSchema,
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

        let (_, version) = build_version(&option, &manager, &Arc::new(TestSchema)).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::default(),
            schema,
            Arc::new(TestSchema),
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
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &schema,
        );
        let db = DB::new(option, TokioExecutor::default(), schema)
            .await
            .unwrap();

        db.insert(DynRecord::new(
            vec![
                Value::Int8(1_i8),
                Value::Int16(180_i16),
                Value::Int32(56_i32),
            ],
            0,
        ))
        .await
        .unwrap();

        let txn = db.transaction().await;
        {
            let key = Value::Int8(1_i8);

            let record_ref = txn.get(&key, Projection::All).await.unwrap();
            assert!(record_ref.is_some());
            let res = record_ref.unwrap();
            let record_ref = res.get();

            assert_eq!(record_ref.columns.len(), 3);
            let col = record_ref.columns.first().unwrap();
            assert_eq!(col, &ValueRef::Int8(1_i8));

            let height = record_ref.columns.get(1).unwrap();
            assert_eq!(height, &ValueRef::Int16(180_i16));

            let weight = record_ref.columns.get(2).unwrap();
            assert_eq!(weight, &ValueRef::Int32(56_i32));
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
                dbg!(columns.clone());

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col, &ValueRef::Int8(1_i8));

                let col = columns.get(1).unwrap();
                assert_eq!(col, &ValueRef::Int16(180_i16));

                let col = columns.get(2).unwrap();
                assert_eq!(col, &ValueRef::Int32(56_i32));
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_scan_reverse_simple() {
        // Simple test with only transaction local data
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );

        let db = DB::<Test, TokioExecutor>::new(option, TokioExecutor::default(), TestSchema)
            .await
            .unwrap();

        // Test with only local buffer data
        {
            let mut txn = db.transaction().await;

            // Add data to the transaction's local buffer
            for i in 1..=5 {
                txn.insert(Test {
                    vstring: i.to_string(),
                    vu32: i,
                    vbool: Some(true),
                });
            }

            // Test ascending order
            let mut stream = txn
                .scan((Bound::Unbounded, Bound::Unbounded))
                .take()
                .await
                .unwrap();

            let mut results = Vec::new();
            while let Some(entry_result) = stream.next().await {
                let entry = entry_result.unwrap();
                results.push(entry.key().value.to_string());
            }
            assert_eq!(results, vec!["1", "2", "3", "4", "5"]);

            // Test descending order
            let mut stream = txn
                .scan((Bound::Unbounded, Bound::Unbounded))
                .reverse()
                .take()
                .await
                .unwrap();

            let mut results = Vec::new();
            while let Some(entry_result) = stream.next().await {
                let entry = entry_result.unwrap();
                results.push(entry.key().value.to_string());
            }
            assert_eq!(results, vec!["5", "4", "3", "2", "1"]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_scan_reverse() {
        // Test transaction scan with reverse order
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );

        let db = DB::<Test, TokioExecutor>::new(option, TokioExecutor::default(), TestSchema)
            .await
            .unwrap();

        // First, insert some committed data
        for i in [1, 3, 5, 7, 9] {
            db.insert(Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            })
            .await
            .unwrap();
        }

        // Test transaction scan with local buffer data
        {
            let mut txn = db.transaction().await;

            // Add data to the transaction's local buffer
            for i in [2, 4, 6, 8] {
                txn.insert(Test {
                    vstring: i.to_string(),
                    vu32: i,
                    vbool: Some(true),
                });
            }

            // Test ascending order
            let mut stream = txn
                .scan((Bound::Unbounded, Bound::Unbounded))
                .take()
                .await
                .unwrap();

            let mut results = Vec::new();
            while let Some(entry_result) = stream.next().await {
                let entry = entry_result.unwrap();
                results.push(entry.key().value.to_string());
            }
            // Verify ascending order
            assert_eq!(results, vec!["1", "2", "3", "4", "5", "6", "7", "8", "9"]);

            // Test descending order
            let mut stream = txn
                .scan((Bound::Unbounded, Bound::Unbounded))
                .reverse()
                .take()
                .await
                .unwrap();

            let mut results = Vec::new();
            while let Some(entry_result) = stream.next().await {
                let entry = entry_result.unwrap();
                results.push(entry.key().value.to_string());
            }
            // Verify descending order
            assert_eq!(results, vec!["9", "8", "7", "6", "5", "4", "3", "2", "1"]);
        }
    }
}
