//! `tonbo` is a structured embedded database, built
//! on Apache `Arrow` & `Parquet`, designed to
//! store, filter, and project
//! structured data using `LSM` Tree.
//! Its name comes from the Japanese word for `dragonfly`.
//!
//! This database is fully thread-safe and all operations
//! are atomic to ensure data consistency.
//! most operations are asynchronous and non-blocking,
//! providing efficient concurrent processing capabilities.
//!
//! `tonbo` constructs an instance using the [`DB::new`] method to serve a
//! specific `Tonbo Record` type.
//!
//! `Tonbo Record` is automatically implemented by the macro [`tonbo_record`].
//! Support type
//! - String
//! - Boolean
//! - Int8
//! - Int16
//! - Int32
//! - Int64
//! - UInt8
//! - UInt16
//! - UInt32
//! - UInt64
//!
//! ACID `optimistic` transactions for concurrent data reading and writing are
//! supported with the [`DB::transaction`] method.
//!
//! # Examples
//!
//! ```no_run
//! use std::{ops::Bound, sync::Arc};
//!
//! use fusio::{local::TokioFs, path::Path};
//! use futures_util::stream::StreamExt;
//! use tokio::fs;
//! use tokio_util::bytes::Bytes;
//! use tonbo::{
//!     executor::tokio::TokioExecutor, fs::manager::StoreManager, DbOption, Projection, Record, DB,
//! };
//!
//! // use macro to define schema of column family just like ORM
//! // it provides type safety read & write API
//! #[derive(Record, Debug)]
//! pub struct User {
//!     #[record(primary_key)]
//!     name: String,
//!     email: Option<String>,
//!     age: u8,
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // make sure the path exists
//!     let _ = fs::create_dir_all("./db_path/users").await;
//!
//!     let manager = StoreManager::new(Arc::new(TokioFs), vec![]);
//!     let options = DbOption::from(Path::from_filesystem_path("./db_path/users").unwrap());
//!     // pluggable async runtime and I/O
//!     let db = DB::new(options, TokioExecutor::default(), manager)
//!         .await
//!         .unwrap();
//!     // insert with owned value
//!     db.insert(User {
//!         name: "Alice".into(),
//!         email: Some("alice@gmail.com".into()),
//!         age: 22,
//!     })
//!     .await
//!     .unwrap();
//!
//!     {
//!         // tonbo supports transaction
//!         let txn = db.transaction().await;
//!
//!         // get from primary key
//!         let name = "Alice".into();
//!
//!         // get the zero-copy reference of record without any allocations.
//!         let user = txn
//!             .get(
//!                 &name,
//!                 // tonbo supports pushing down projection
//!                 Projection::All,
//!             )
//!             .await
//!             .unwrap();
//!         assert!(user.is_some());
//!         assert_eq!(user.unwrap().get().age, Some(22));
//!
//!         {
//!             let upper = "Blob".into();
//!             // range scan of
//!             let mut scan = txn
//!                 .scan((Bound::Included(&name), Bound::Excluded(&upper)))
//!                 // tonbo supports pushing down projection
//!                 .projection(vec![1])
//!                 .take()
//!                 .await
//!                 .unwrap();
//!             while let Some(entry) = scan.next().await.transpose().unwrap() {
//!                 assert_eq!(
//!                     entry.value(),
//!                     Some(UserRef {
//!                         name: "Alice",
//!                         email: Some("alice@gmail.com"),
//!                         age: Some(22),
//!                     })
//!                 );
//!             }
//!         }
//!
//!         // commit transaction
//!         txn.commit().await.unwrap();
//!     }
//! }
//! ```
mod compaction;
pub mod executor;
pub mod fs;
pub mod inmem;
mod ondisk;
pub mod option;
pub mod record;
mod scope;
pub mod serdes;
pub mod stream;
pub mod timestamp;
pub mod transaction;
mod trigger;
mod version;
mod wal;

use std::{collections::HashMap, io, marker::PhantomData, mem, ops::Bound, pin::pin, sync::Arc};

pub use arrow;
use async_lock::RwLock;
use async_stream::stream;
use flume::{bounded, Sender};
use fusio::dynamic::DynFile;
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{immutable::Immutable, mutable::Mutable};
use lockable::LockableHashMap;
pub use once_cell;
pub use parquet;
use parquet::{
    arrow::{arrow_to_parquet_schema, ProjectionMask},
    errors::ParquetError,
};
use record::{ColumnDesc, DynRecord, Record, RecordInstance};
use thiserror::Error;
use timestamp::{Timestamp, TimestampedRef};
use tokio::sync::oneshot;
pub use tonbo_macros::{KeyAttributes, Record};
use tracing::error;
use transaction::{CommitError, Transaction, TransactionEntry};

pub use crate::option::*;
use crate::{
    compaction::{CompactTask, Compactor},
    executor::Executor,
    fs::{default_open_options, manager::StoreManager, parse_file_id, FileId, FileType},
    serdes::Decode,
    stream::{
        mem_projection::MemProjectionStream, merge::MergeStream, package::PackageStream, Entry,
        ScanStream,
    },
    timestamp::Timestamped,
    trigger::{Trigger, TriggerFactory},
    version::{cleaner::Cleaner, set::VersionSet, TransactionTs, Version, VersionError},
    wal::{log::LogType, RecoverError, WalFile},
};

pub struct DB<R, E>
where
    R: Record,
    E: Executor,
{
    schema: Arc<RwLock<Schema<R>>>,
    version_set: VersionSet<R>,
    lock_map: LockMap<R::Key>,
    manager: Arc<StoreManager>,
    _p: PhantomData<E>,
}

impl<E: Executor> DB<DynRecord, E>
where
    E: Executor + Send + Sync + 'static,
{
    /// Open [`DB`] with schema which determined by [`ColumnDesc`].
    pub async fn with_schema(
        option: DbOption<DynRecord>,
        executor: E,
        manager: StoreManager,
        column_descs: Vec<ColumnDesc>,
        primary_index: usize,
    ) -> Result<Self, DbError<DynRecord>> {
        let option = Arc::new(option);
        let manager = Arc::new(manager);

        {
            let base_fs = manager.base_fs();

            base_fs
                .create_dir_all(&option.wal_dir_path())
                .await
                .map_err(DbError::Fusio)?;
            base_fs
                .create_dir_all(&option.version_log_dir_path())
                .await
                .map_err(DbError::Fusio)?;
        }

        let instance =
            RecordInstance::Runtime(DynRecord::empty_record(column_descs, primary_index));

        Self::build(option, executor, instance, manager).await
    }
}

impl<R, E> DB<R, E>
where
    R: Record + Send + Sync,
    R::Columns: Send + Sync,
    E: Executor + Send + Sync + 'static,
{
    /// Open [`DB`] with a [`DbOption`]. This will create a new directory at the
    /// path specified in [`DbOption`] (if it does not exist before) and run it
    /// according to the configuration of [`DbOption`].
    ///
    /// For more configurable options, please refer to [`DbOption`].
    pub async fn new(
        option: DbOption<R>,
        executor: E,
        manager: StoreManager,
    ) -> Result<Self, DbError<R>> {
        let option = Arc::new(option);
        let manager = Arc::new(manager);

        {
            let base_fs = manager.base_fs();

            // FIXME: error handle
            let _ = base_fs.create_dir_all(&option.wal_dir_path()).await;
            let _ = base_fs.create_dir_all(&option.version_log_dir_path()).await;
        }

        Self::build(option, executor, RecordInstance::Normal, manager).await
    }

    async fn build(
        option: Arc<DbOption<R>>,
        executor: E,
        instance: RecordInstance,
        manager: Arc<StoreManager>,
    ) -> Result<Self, DbError<R>> {
        let (task_tx, task_rx) = bounded(1);

        let (mut cleaner, clean_sender) = Cleaner::<R>::new(option.clone(), manager.clone());

        let version_set = VersionSet::new(clean_sender, option.clone(), manager.clone()).await?;
        let schema = Arc::new(RwLock::new(
            Schema::new(option.clone(), task_tx, &version_set, instance, &manager).await?,
        ));
        let mut compactor = Compactor::<R>::new(
            schema.clone(),
            option.clone(),
            version_set.clone(),
            manager.clone(),
        );

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });
        executor.spawn(async move {
            while let Ok(task) = task_rx.recv_async().await {
                if let Err(err) = match task {
                    CompactTask::Freeze => compactor.check_then_compaction(None).await,
                    CompactTask::Flush(option_tx) => {
                        compactor.check_then_compaction(option_tx).await
                    }
                } {
                    error!("[Compaction Error]: {}", err)
                }
            }
        });
        Ok(Self {
            schema,
            version_set,
            lock_map: Arc::new(Default::default()),
            manager,
            _p: Default::default(),
        })
    }

    /// open an optimistic ACID transaction
    pub async fn transaction(&self) -> Transaction<'_, R> {
        Transaction::new(
            self.version_set.current().await,
            self.schema.read().await,
            self.lock_map.clone(),
            self.manager.clone(),
        )
    }

    /// insert a single tonbo record
    pub async fn insert(&self, record: R) -> Result<(), CommitError<R>> {
        Ok(self.write(record, self.version_set.increase_ts()).await?)
    }

    /// insert a sequence of data as a single batch
    pub async fn insert_batch(
        &self,
        records: impl ExactSizeIterator<Item = R>,
    ) -> Result<(), CommitError<R>> {
        Ok(self
            .write_batch(records, self.version_set.increase_ts())
            .await?)
    }

    /// delete the record with the primary key as the `key`
    pub async fn remove(&self, key: R::Key) -> Result<bool, CommitError<R>> {
        Ok(self
            .schema
            .read()
            .await
            .remove(LogType::Full, key, self.version_set.increase_ts())
            .await?)
    }

    pub async fn flush(&self) -> Result<(), CommitError<R>> {
        let (tx, rx) = oneshot::channel();
        let compaction_tx = { self.schema.read().await.compaction_tx.clone() };
        compaction_tx.send(CompactTask::Flush(Some(tx)))?;

        rx.await.map_err(|_| CommitError::ChannelClose)?;

        Ok(())
    }

    /// get the record with `key` as the primary key and process it using closure `f`
    pub async fn get<T>(
        &self,
        key: &R::Key,
        mut f: impl FnMut(TransactionEntry<'_, R>) -> T,
    ) -> Result<Option<T>, CommitError<R>> {
        Ok(self
            .schema
            .read()
            .await
            .get(
                &*self.version_set.current().await,
                &self.manager,
                key,
                self.version_set.load_ts(),
                Projection::All,
            )
            .await?
            .map(|e| f(TransactionEntry::Stream(e))))
    }

    /// scan records with primary keys in the `range` and process them using closure `f`
    pub async fn scan<'scan, T: 'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        mut f: impl FnMut(TransactionEntry<'_, R>) -> T + 'scan,
    ) -> impl Stream<Item = Result<T, CommitError<R>>> + 'scan {
        stream! {
            let schema = self.schema.read().await;
            let manager = &self.manager;
            let current = self.version_set.current().await;
            let mut scan = Scan::new(
                &schema,
                manager,
                range,
                self.version_set.load_ts(),
                &*current,
                Box::new(|_| None),
            ).take().await?;

            while let Some(record) = scan.next().await {
                yield Ok(f(TransactionEntry::Stream(record?)))
            }
        }
    }

    pub(crate) async fn write(&self, record: R, ts: Timestamp) -> Result<(), DbError<R>> {
        let schema = self.schema.read().await;

        if schema.write(LogType::Full, record, ts).await? {
            let _ = schema.compaction_tx.try_send(CompactTask::Freeze);
        }

        Ok(())
    }

    pub(crate) async fn write_batch(
        &self,
        mut records: impl ExactSizeIterator<Item = R>,
        ts: Timestamp,
    ) -> Result<(), DbError<R>> {
        let schema = self.schema.read().await;

        if let Some(first) = records.next() {
            let is_excess = if let Some(record) = records.next() {
                schema.write(LogType::First, first, ts).await?;

                let mut last_buf = record;

                for record in records {
                    schema
                        .write(LogType::Middle, mem::replace(&mut last_buf, record), ts)
                        .await?;
                }
                schema.write(LogType::Last, last_buf, ts).await?
            } else {
                schema.write(LogType::Full, first, ts).await?
            };
            if is_excess {
                let _ = schema.compaction_tx.try_send(CompactTask::Freeze);
            }
        };

        Ok(())
    }
}

pub(crate) struct Schema<R>
where
    R: Record,
{
    pub mutable: Mutable<R>,
    pub immutables: Vec<(Option<FileId>, Immutable<R::Columns>)>,
    compaction_tx: Sender<CompactTask>,
    recover_wal_ids: Option<Vec<FileId>>,
    trigger: Arc<Box<dyn Trigger<R> + Send + Sync>>,
    record_instance: RecordInstance,
}

impl<R> Schema<R>
where
    R: Record + Send,
{
    async fn new(
        option: Arc<DbOption<R>>,
        compaction_tx: Sender<CompactTask>,
        version_set: &VersionSet<R>,
        record_instance: RecordInstance,
        manager: &StoreManager,
    ) -> Result<Self, DbError<R>> {
        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));
        let mut schema = Schema {
            mutable: Mutable::new(&option, trigger.clone(), manager.base_fs()).await?,
            immutables: Default::default(),
            compaction_tx,
            recover_wal_ids: None,
            trigger,
            record_instance,
        };

        let base_fs = manager.base_fs();
        let wal_dir_path = option.wal_dir_path();
        let mut transaction_map = HashMap::new();
        let mut wal_ids = Vec::new();

        let wal_metas = {
            let mut wal_metas = Vec::new();
            let mut wal_stream = base_fs.list(&wal_dir_path).await?;

            while let Some(file_meta) = wal_stream.next().await {
                wal_metas.push(file_meta?);
            }
            wal_metas.sort_by(|meta_a, meta_b| meta_a.path.cmp(&meta_b.path));
            wal_metas
        };

        for wal_meta in wal_metas {
            let wal_path = wal_meta.path;

            let file = base_fs
                .open_options(&wal_path, default_open_options())
                .await?;
            // SAFETY: wal_stream return only file name
            let wal_id = parse_file_id(&wal_path, FileType::Wal)?.unwrap();
            let mut wal = WalFile::<Box<dyn DynFile>, R>::new(file, wal_id);
            wal_ids.push(wal_id);

            let mut recover_stream = pin!(wal.recover());
            while let Some(record) = recover_stream.next().await {
                let (log_type, Timestamped { ts, value: key }, value_option) = record?;

                let is_excess = match log_type {
                    LogType::Full => {
                        schema
                            .recover_append(key, version_set.increase_ts(), value_option)
                            .await?
                    }
                    LogType::First => {
                        transaction_map.insert(ts, vec![(key, value_option)]);
                        false
                    }
                    LogType::Middle => {
                        transaction_map
                            .get_mut(&ts)
                            .unwrap()
                            .push((key, value_option));
                        false
                    }
                    LogType::Last => {
                        let mut is_excess = false;
                        let mut records = transaction_map.remove(&ts).unwrap();
                        records.push((key, value_option));

                        let ts = version_set.increase_ts();
                        for (key, value_option) in records {
                            is_excess = schema.recover_append(key, ts, value_option).await?;
                        }
                        is_excess
                    }
                };
                if is_excess {
                    let _ = schema.compaction_tx.try_send(CompactTask::Freeze);
                }
            }
        }
        schema.recover_wal_ids = Some(wal_ids);

        Ok(schema)
    }

    async fn write(&self, log_ty: LogType, record: R, ts: Timestamp) -> Result<bool, DbError<R>> {
        self.mutable.insert(log_ty, record, ts).await
    }

    async fn remove(
        &self,
        log_ty: LogType,
        key: R::Key,
        ts: Timestamp,
    ) -> Result<bool, DbError<R>> {
        self.mutable.remove(log_ty, key, ts).await
    }

    async fn recover_append(
        &self,
        key: R::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<bool, DbError<R>> {
        self.mutable.append(None, key, ts, value).await
    }

    async fn get<'get>(
        &'get self,
        version: &'get Version<R>,
        manager: &StoreManager,
        key: &'get R::Key,
        ts: Timestamp,
        projection: Projection,
    ) -> Result<Option<Entry<'get, R>>, DbError<R>> {
        if let Some(entry) = self.mutable.get(key, ts) {
            return Ok(Some(Entry::Mutable(entry)));
        }
        let primary_key_index = self.record_instance.primary_key_index::<R>();

        let projection = match projection {
            Projection::All => ProjectionMask::all(),
            Projection::Parts(projection) => {
                let mut fixed_projection: Vec<usize> = [0, 1, primary_key_index]
                    .into_iter()
                    .chain(projection.into_iter().map(|p| p + 2))
                    .collect();
                fixed_projection.dedup();

                ProjectionMask::roots(
                    &arrow_to_parquet_schema(&self.record_instance.arrow_schema::<R>()).unwrap(),
                    fixed_projection,
                )
            }
        };

        for (_, immutable) in self.immutables.iter().rev() {
            if let Some(entry) = immutable.get(key, ts, projection.clone()) {
                return Ok(Some(Entry::RecordBatch(entry)));
            }
        }

        Ok(version
            .query(manager, TimestampedRef::new(key, ts), projection)
            .await?
            .map(|entry| Entry::RecordBatch(entry)))
    }

    fn check_conflict(&self, key: &R::Key, ts: Timestamp) -> bool {
        self.mutable.check_conflict(key, ts)
            || self
                .immutables
                .iter()
                .rev()
                .any(|(_, immutable)| immutable.check_conflict(key, ts))
    }
}

/// scan configuration intermediate structure
pub struct Scan<'scan, R>
where
    R: Record,
{
    schema: &'scan Schema<R>,
    manager: &'scan StoreManager,
    lower: Bound<&'scan R::Key>,
    upper: Bound<&'scan R::Key>,
    ts: Timestamp,

    version: &'scan Version<R>,
    fn_pre_stream:
        Box<dyn FnOnce(Option<ProjectionMask>) -> Option<ScanStream<'scan, R>> + Send + 'scan>,

    limit: Option<usize>,
    projection_indices: Option<Vec<usize>>,
    projection: ProjectionMask,
}

impl<'scan, R> Scan<'scan, R>
where
    R: Record + Send,
{
    fn new(
        schema: &'scan Schema<R>,
        manager: &'scan StoreManager,
        (lower, upper): (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
        version: &'scan Version<R>,
        fn_pre_stream: Box<
            dyn FnOnce(Option<ProjectionMask>) -> Option<ScanStream<'scan, R>> + Send + 'scan,
        >,
    ) -> Self {
        Self {
            schema,
            manager,
            lower,
            upper,
            ts,
            version,
            fn_pre_stream,
            limit: None,
            projection_indices: None,
            projection: ProjectionMask::all(),
        }
    }

    /// limit for the scan
    pub fn limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// fields in projection Record by field indices
    pub fn projection(self, mut projection: Vec<usize>) -> Self {
        // skip two columns: _null and _ts
        for p in &mut projection {
            *p += 2;
        }
        let primary_key_index = self.schema.record_instance.primary_key_index::<R>();
        let mut fixed_projection = vec![0, 1, primary_key_index];
        fixed_projection.append(&mut projection);
        fixed_projection.dedup();

        let mask = ProjectionMask::roots(
            &arrow_to_parquet_schema(&self.schema.record_instance.arrow_schema::<R>()).unwrap(),
            fixed_projection.clone(),
        );

        Self {
            projection: mask,
            projection_indices: Some(fixed_projection),
            ..self
        }
    }

    /// get a Stream that returns single row of Record
    pub async fn take(
        self,
    ) -> Result<impl Stream<Item = Result<Entry<'scan, R>, ParquetError>>, DbError<R>> {
        let mut streams = Vec::new();
        let is_projection = self.projection_indices.is_some();

        if let Some(pre_stream) =
            (self.fn_pre_stream)(is_projection.then(|| self.projection.clone()))
        {
            streams.push(pre_stream);
        }

        // Mutable
        {
            let mut mutable_scan = self
                .schema
                .mutable
                .scan((self.lower, self.upper), self.ts)
                .into();
            if is_projection {
                mutable_scan =
                    MemProjectionStream::new(mutable_scan, self.projection.clone()).into();
            }
            streams.push(mutable_scan);
        }
        for (_, immutable) in self.schema.immutables.iter().rev() {
            streams.push(
                immutable
                    .scan((self.lower, self.upper), self.ts, self.projection.clone())
                    .into(),
            );
        }
        self.version
            .streams(
                self.manager,
                &mut streams,
                (self.lower, self.upper),
                self.ts,
                self.limit,
                self.projection,
            )
            .await?;

        let mut merge_stream = MergeStream::from_vec(streams, self.ts).await?;
        if let Some(limit) = self.limit {
            merge_stream = merge_stream.limit(limit);
        }
        Ok(merge_stream)
    }

    /// Get a Stream that returns RecordBatch consisting of a `batch_size` number of records
    pub async fn package(
        self,
        batch_size: usize,
    ) -> Result<impl Stream<Item = Result<R::Columns, ParquetError>> + 'scan, DbError<R>> {
        let mut streams = Vec::new();
        let is_projection = self.projection_indices.is_some();

        if let Some(pre_stream) =
            (self.fn_pre_stream)(is_projection.then(|| self.projection.clone()))
        {
            streams.push(pre_stream);
        }

        // Mutable
        {
            let mut mutable_scan = self
                .schema
                .mutable
                .scan((self.lower, self.upper), self.ts)
                .into();
            if is_projection {
                mutable_scan =
                    MemProjectionStream::new(mutable_scan, self.projection.clone()).into();
            }
            streams.push(mutable_scan);
        }
        for (_, immutable) in self.schema.immutables.iter().rev() {
            streams.push(
                immutable
                    .scan((self.lower, self.upper), self.ts, self.projection.clone())
                    .into(),
            );
        }
        self.version
            .streams(
                self.manager,
                &mut streams,
                (self.lower, self.upper),
                self.ts,
                self.limit,
                self.projection,
            )
            .await?;
        let merge_stream = MergeStream::from_vec(streams, self.ts).await?;

        Ok(PackageStream::new(
            batch_size,
            merge_stream,
            self.projection_indices,
            &self.schema.record_instance,
        ))
    }
}

#[derive(Debug, Error)]
pub enum DbError<R>
where
    R: Record,
{
    #[error("write io error: {0}")]
    Io(#[from] io::Error),
    #[error("write version error: {0}")]
    Version(#[from] VersionError<R>),
    #[error("write parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("write ulid decode error: {0}")]
    UlidDecode(#[from] ulid::DecodeError),
    #[error("write fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    // #[error("write encode error: {0}")]
    // Encode(<<R as Record>::Ref as Encode>::Error),
    #[error("write recover error: {0}")]
    Recover(#[from] RecoverError<<R as Decode>::Error>),
    #[error("wal write error: {0}")]
    WalWrite(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("exceeds the maximum level(0-6)")]
    ExceedsMaxLevel,
}

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

pub enum Projection {
    All,
    Parts(Vec<usize>),
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::{BTreeMap, Bound},
        mem,
        sync::Arc,
    };

    use arrow::{
        array::{Array, AsArray, RecordBatch},
        datatypes::{DataType, Field, Schema, UInt32Type},
    };
    use async_lock::RwLock;
    use flume::{bounded, Receiver};
    use fusio::{disk::TokioFs, path::Path, DynFs, Read, Write};
    use futures::StreamExt;
    use once_cell::sync::Lazy;
    use parquet::{arrow::ProjectionMask, format::SortingColumn, schema::types::ColumnPath};
    use tempfile::TempDir;
    use tracing::error;

    use crate::{
        compaction::{CompactTask, Compactor},
        executor::{tokio::TokioExecutor, Executor},
        fs::{manager::StoreManager, FileId},
        inmem::{immutable::tests::TestImmutableArrays, mutable::Mutable},
        record::{
            internal::InternalRecordRef,
            test::{test_dyn_item_schema, test_dyn_items},
            Column, Datatype, DynRecord, RecordDecodeError, RecordEncodeError, RecordInstance,
            RecordRef,
        },
        serdes::{Decode, Encode},
        trigger::{TriggerFactory, TriggerType},
        version::{cleaner::Cleaner, set::tests::build_version_set, Version},
        wal::log::LogType,
        DbError, DbOption, Immutable, Projection, Record, DB,
    };

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub struct Test {
        pub vstring: String,
        pub vu32: u32,
        pub vbool: Option<bool>,
    }

    impl Decode for Test {
        type Error = RecordDecodeError;

        async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
        where
            R: Read + Unpin,
        {
            let vstring =
                String::decode(reader)
                    .await
                    .map_err(|err| RecordDecodeError::Decode {
                        field_name: "vstring".to_string(),
                        error: Box::new(err),
                    })?;
            let vu32 = Option::<u32>::decode(reader)
                .await
                .map_err(|err| RecordDecodeError::Decode {
                    field_name: "vu32".to_string(),
                    error: Box::new(err),
                })?
                .unwrap();
            let vbool =
                Option::<bool>::decode(reader)
                    .await
                    .map_err(|err| RecordDecodeError::Decode {
                        field_name: "vbool".to_string(),
                        error: Box::new(err),
                    })?;

            Ok(Self {
                vstring,
                vu32,
                vbool,
            })
        }
    }

    impl Record for Test {
        type Columns = TestImmutableArrays;

        type Key = String;

        type Ref<'r>
            = TestRef<'r>
        where
            Self: 'r;

        fn key(&self) -> &str {
            &self.vstring
        }

        fn primary_key_index() -> usize {
            2
        }

        fn primary_key_path() -> (ColumnPath, Vec<SortingColumn>) {
            (
                ColumnPath::new(vec!["_ts".to_string(), "vstring".to_string()]),
                vec![
                    SortingColumn::new(1, true, true),
                    SortingColumn::new(2, false, true),
                ],
            )
        }

        fn as_record_ref(&self) -> Self::Ref<'_> {
            TestRef {
                vstring: &self.vstring,
                vu32: Some(self.vu32),
                vbool: self.vbool,
            }
        }

        fn arrow_schema() -> &'static Arc<Schema> {
            static SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
                Arc::new(Schema::new(vec![
                    Field::new("_null", DataType::Boolean, false),
                    Field::new("_ts", DataType::UInt32, false),
                    Field::new("vstring", DataType::Utf8, false),
                    Field::new("vu32", DataType::UInt32, false),
                    Field::new("vbool", DataType::Boolean, true),
                ]))
            });

            &SCHEMA
        }

        fn size(&self) -> usize {
            let string_size = self.vstring.len();
            let u32_size = mem::size_of::<u32>();
            let bool_size = self.vbool.map_or(0, |_| mem::size_of::<bool>());
            string_size + u32_size + bool_size
        }
    }

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub struct TestRef<'r> {
        pub vstring: &'r str,
        pub vu32: Option<u32>,
        pub vbool: Option<bool>,
    }

    impl<'r> Encode for TestRef<'r> {
        type Error = RecordEncodeError;

        async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
        where
            W: Write + Unpin + Send,
        {
            self.vstring
                .encode(writer)
                .await
                .map_err(|err| RecordEncodeError::Encode {
                    field_name: "vstring".to_string(),
                    error: Box::new(err),
                })?;
            self.vu32
                .encode(writer)
                .await
                .map_err(|err| RecordEncodeError::Encode {
                    field_name: "vu32".to_string(),
                    error: Box::new(err),
                })?;
            self.vbool
                .encode(writer)
                .await
                .map_err(|err| RecordEncodeError::Encode {
                    field_name: "vbool".to_string(),
                    error: Box::new(err),
                })?;

            Ok(())
        }

        fn size(&self) -> usize {
            self.vstring.size() + self.vu32.size() + self.vbool.size()
        }
    }

    impl<'r> RecordRef<'r> for TestRef<'r> {
        type Record = Test;

        fn key(self) -> <<Self::Record as Record>::Key as crate::record::Key>::Ref<'r> {
            self.vstring
        }

        fn projection(&mut self, projection_mask: &ProjectionMask) {
            if !projection_mask.leaf_included(3) {
                self.vu32 = None;
            }
            if !projection_mask.leaf_included(4) {
                self.vbool = None;
            }
        }

        fn from_record_batch(
            record_batch: &'r RecordBatch,
            offset: usize,
            projection_mask: &'r ProjectionMask,
            _: &Arc<Schema>,
        ) -> InternalRecordRef<'r, Self> {
            let mut column_i = 2;
            let null = record_batch.column(0).as_boolean().value(offset);

            let ts = record_batch
                .column(1)
                .as_primitive::<UInt32Type>()
                .value(offset)
                .into();

            let vstring = record_batch
                .column(column_i)
                .as_string::<i32>()
                .value(offset);
            column_i += 1;

            let mut vu32 = None;

            if projection_mask.leaf_included(3) {
                vu32 = Some(
                    record_batch
                        .column(column_i)
                        .as_primitive::<UInt32Type>()
                        .value(offset),
                );
                column_i += 1;
            }

            let mut vbool = None;

            if projection_mask.leaf_included(4) {
                let vbool_array = record_batch.column(column_i).as_boolean();

                if !vbool_array.is_null(offset) {
                    vbool = Some(vbool_array.value(offset));
                }
            }

            let record = TestRef {
                vstring,
                vu32,
                vbool,
            };
            InternalRecordRef::new(ts, record, null)
        }
    }

    pub(crate) async fn get_test_record_batch<E: Executor + Send + Sync + 'static>(
        option: DbOption<Test>,
        executor: E,
        manager: StoreManager,
    ) -> RecordBatch {
        let base_fs = manager.base_fs().clone();
        let db: DB<Test, E> = DB::new(option.clone(), executor, manager).await.unwrap();

        db.write(
            Test {
                vstring: "hello".to_string(),
                vu32: 12,
                vbool: Some(true),
            },
            1.into(),
        )
        .await
        .unwrap();
        db.write(
            Test {
                vstring: "world".to_string(),
                vu32: 12,
                vbool: None,
            },
            1.into(),
        )
        .await
        .unwrap();

        let mut schema = db.schema.write().await;

        let trigger = schema.trigger.clone();
        let mutable = mem::replace(
            &mut schema.mutable,
            Mutable::new(&option, trigger, &base_fs).await.unwrap(),
        );

        Immutable::<<Test as Record>::Columns>::from((mutable.data, &RecordInstance::Normal))
            .as_record_batch()
            .clone()
    }

    pub(crate) async fn build_schema(
        option: Arc<DbOption<Test>>,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(crate::Schema<Test>, Receiver<CompactTask>), fusio::Error> {
        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let mutable = Mutable::new(&option, trigger.clone(), fs).await?;

        mutable
            .insert(
                LogType::Full,
                Test {
                    vstring: "alice".to_string(),
                    vu32: 1,
                    vbool: Some(true),
                },
                1_u32.into(),
            )
            .await
            .unwrap();
        mutable
            .insert(
                LogType::Full,
                Test {
                    vstring: "ben".to_string(),
                    vu32: 2,
                    vbool: Some(true),
                },
                1_u32.into(),
            )
            .await
            .unwrap();
        mutable
            .insert(
                LogType::Full,
                Test {
                    vstring: "carl".to_string(),
                    vu32: 3,
                    vbool: Some(true),
                },
                1_u32.into(),
            )
            .await
            .unwrap();

        let immutables = {
            let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

            let mutable: Mutable<Test> = Mutable::new(&option, trigger.clone(), fs).await?;

            mutable
                .insert(
                    LogType::Full,
                    Test {
                        vstring: "dice".to_string(),
                        vu32: 4,
                        vbool: Some(true),
                    },
                    1_u32.into(),
                )
                .await
                .unwrap();
            mutable
                .insert(
                    LogType::Full,
                    Test {
                        vstring: "erika".to_string(),
                        vu32: 5,
                        vbool: Some(true),
                    },
                    1_u32.into(),
                )
                .await
                .unwrap();
            mutable
                .insert(
                    LogType::Full,
                    Test {
                        vstring: "funk".to_string(),
                        vu32: 6,
                        vbool: Some(true),
                    },
                    1_u32.into(),
                )
                .await
                .unwrap();

            vec![(
                Some(FileId::new()),
                Immutable::from((mutable.data, &RecordInstance::Normal)),
            )]
        };

        let (compaction_tx, compaction_rx) = bounded(1);

        Ok((
            crate::Schema {
                mutable,
                immutables,
                compaction_tx,
                recover_wal_ids: None,
                trigger,
                record_instance: RecordInstance::Normal,
            },
            compaction_rx,
        ))
    }

    pub(crate) async fn build_db<R, E>(
        option: Arc<DbOption<R>>,
        compaction_rx: Receiver<CompactTask>,
        executor: E,
        schema: crate::Schema<R>,
        version: Version<R>,
        manager: Arc<StoreManager>,
    ) -> Result<DB<R, E>, DbError<R>>
    where
        R: Record + Send + Sync,
        R::Columns: Send + Sync,
        E: Executor + Send + Sync + 'static,
    {
        {
            let base_fs = manager.base_fs();

            let _ = base_fs.create_dir_all(&option.wal_dir_path()).await;
            let _ = base_fs.create_dir_all(&option.version_log_dir_path()).await;
        }

        let schema = Arc::new(RwLock::new(schema));

        let (mut cleaner, clean_sender) = Cleaner::<R>::new(option.clone(), manager.clone());
        let version_set =
            build_version_set(version, clean_sender, option.clone(), manager.clone()).await?;
        let mut compactor = Compactor::<R>::new(
            schema.clone(),
            option.clone(),
            version_set.clone(),
            manager.clone(),
        );

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });
        executor.spawn(async move {
            while let Ok(task) = compaction_rx.recv_async().await {
                if let Err(err) = match task {
                    CompactTask::Freeze => compactor.check_then_compaction(None).await,
                    CompactTask::Flush(option_tx) => {
                        compactor.check_then_compaction(option_tx).await
                    }
                } {
                    error!("[Compaction Error]: {}", err)
                }
            }
        });

        Ok(DB {
            schema,
            version_set,
            lock_map: Arc::new(Default::default()),
            manager,
            _p: Default::default(),
        })
    }

    fn test_items() -> Vec<Test> {
        vec![
            Test {
                vstring: 0.to_string(),
                vu32: 0,
                vbool: Some(true),
            },
            Test {
                vstring: 1.to_string(),
                vu32: 1,
                vbool: Some(true),
            },
            Test {
                vstring: 2.to_string(),
                vu32: 2,
                vbool: Some(true),
            },
            Test {
                vstring: 3.to_string(),
                vu32: 3,
                vbool: Some(true),
            },
            Test {
                vstring: 4.to_string(),
                vu32: 4,
                vbool: Some(true),
            },
            Test {
                vstring: 5.to_string(),
                vu32: 5,
                vbool: Some(true),
            },
            Test {
                vstring: 6.to_string(),
                vu32: 6,
                vbool: Some(true),
            },
            Test {
                vstring: 7.to_string(),
                vu32: 7,
                vbool: Some(true),
            },
            Test {
                vstring: 8.to_string(),
                vu32: 8,
                vbool: Some(true),
            },
            Test {
                vstring: 9.to_string(),
                vu32: 9,
                vbool: Some(true),
            },
            Test {
                vstring: 10.to_string(),
                vu32: 0,
                vbool: Some(true),
            },
            Test {
                vstring: 11.to_string(),
                vu32: 1,
                vbool: Some(true),
            },
            Test {
                vstring: 12.to_string(),
                vu32: 2,
                vbool: Some(true),
            },
            Test {
                vstring: 13.to_string(),
                vu32: 3,
                vbool: Some(true),
            },
            Test {
                vstring: 14.to_string(),
                vu32: 4,
                vbool: Some(true),
            },
            Test {
                vstring: 15.to_string(),
                vu32: 5,
                vbool: Some(true),
            },
            Test {
                vstring: 16.to_string(),
                vu32: 6,
                vbool: Some(true),
            },
            Test {
                vstring: 17.to_string(),
                vu32: 7,
                vbool: Some(true),
            },
            Test {
                vstring: 18.to_string(),
                vu32: 8,
                vbool: Some(true),
            },
            Test {
                vstring: 19.to_string(),
                vu32: 9,
                vbool: Some(true),
            },
            Test {
                vstring: 20.to_string(),
                vu32: 0,
                vbool: Some(true),
            },
            Test {
                vstring: 21.to_string(),
                vu32: 1,
                vbool: Some(true),
            },
            Test {
                vstring: 22.to_string(),
                vu32: 2,
                vbool: Some(true),
            },
            Test {
                vstring: 23.to_string(),
                vu32: 3,
                vbool: Some(true),
            },
            Test {
                vstring: 24.to_string(),
                vu32: 4,
                vbool: Some(true),
            },
            Test {
                vstring: 25.to_string(),
                vu32: 5,
                vbool: Some(true),
            },
            Test {
                vstring: 26.to_string(),
                vu32: 6,
                vbool: Some(true),
            },
            Test {
                vstring: 27.to_string(),
                vu32: 7,
                vbool: Some(true),
            },
            Test {
                vstring: 28.to_string(),
                vu32: 8,
                vbool: Some(true),
            },
            Test {
                vstring: 29.to_string(),
                vu32: 9,
                vbool: Some(true),
            },
            Test {
                vstring: 30.to_string(),
                vu32: 0,
                vbool: Some(true),
            },
            Test {
                vstring: 31.to_string(),
                vu32: 1,
                vbool: Some(true),
            },
            Test {
                vstring: 32.to_string(),
                vu32: 2,
                vbool: Some(true),
            },
            Test {
                vstring: 33.to_string(),
                vu32: 3,
                vbool: Some(true),
            },
            Test {
                vstring: 34.to_string(),
                vu32: 4,
                vbool: Some(true),
            },
            Test {
                vstring: 35.to_string(),
                vu32: 5,
                vbool: Some(true),
            },
            Test {
                vstring: 36.to_string(),
                vu32: 6,
                vbool: Some(true),
            },
            Test {
                vstring: 37.to_string(),
                vu32: 7,
                vbool: Some(true),
            },
            Test {
                vstring: 38.to_string(),
                vu32: 8,
                vbool: Some(true),
            },
            Test {
                vstring: 39.to_string(),
                vu32: 9,
                vbool: Some(true),
            },
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_from_disk() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);

        let mut option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(/* max_mutable_len */ 5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::new(), manager)
            .await
            .unwrap();

        for (i, item) in test_items().into_iter().enumerate() {
            db.write(item, 0.into()).await.unwrap();
            if i % 5 == 0 {
                db.flush().await.unwrap();
            }
        }

        let tx = db.transaction().await;
        let key = 20.to_string();
        let option1 = tx.get(&key, Projection::All).await.unwrap().unwrap();

        dbg!(db.version_set.current().await);

        let version = db.version_set.current().await;
        assert!(!version.level_slice[1].is_empty());

        assert_eq!(option1.get().vstring, "20");
        assert_eq!(option1.get().vu32, Some(0));
        assert_eq!(option1.get().vbool, Some(true));
    }

    #[tokio::test]
    async fn test_flush() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);

        let mut option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(/* max_mutable_len */ 5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::new(), manager)
            .await
            .unwrap();

        for item in &test_items()[0..10] {
            db.write(item.clone(), 0.into()).await.unwrap();
        }

        dbg!(db.version_set.current().await);
        db.flush().await.unwrap();
        dbg!(db.version_set.current().await);

        let version = db.version_set.current().await;
        assert!(!version.level_slice[0].is_empty());
    }

    #[tokio::test]
    async fn schema_recover() {
        let temp_dir = TempDir::new().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;

        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let (task_tx, _task_rx) = bounded(1);

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));
        let schema: crate::Schema<Test> = crate::Schema {
            mutable: Mutable::new(&option, trigger.clone(), &fs).await.unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx.clone(),
            recover_wal_ids: None,
            trigger,
            record_instance: RecordInstance::Normal,
        };

        for (i, item) in test_items().into_iter().enumerate() {
            schema
                .write(LogType::Full, item, (i as u32).into())
                .await
                .unwrap();
        }
        drop(schema);

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let schema: crate::Schema<Test> = crate::Schema {
            mutable: Mutable::new(&option, trigger.clone(), &fs).await.unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx,
            recover_wal_ids: None,
            trigger,
            record_instance: RecordInstance::Normal,
        };
        let range = schema
            .mutable
            .scan((Bound::Unbounded, Bound::Unbounded), u32::MAX.into());

        let mut sort_items = BTreeMap::new();
        for item in test_items() {
            sort_items.insert(item.vstring.clone(), item);
        }
        for entry in range {
            let (_, test) = sort_items.pop_first().unwrap();

            assert_eq!(entry.key().value.as_str(), test.key());
            assert_eq!(entry.value().as_ref().unwrap().vstring, test.vstring);
            assert_eq!(entry.value().as_ref().unwrap().vu32, test.vu32);
            assert_eq!(entry.value().as_ref().unwrap().vbool, test.vbool);
        }
    }

    #[tokio::test]
    async fn dyn_schema_recover() {
        let temp_dir = TempDir::new().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);

        let (desc, primary_key_index) = test_dyn_item_schema();
        let option = Arc::new(DbOption::with_path(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            "age".to_owned(),
            primary_key_index,
        ));
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let (task_tx, _task_rx) = bounded(1);

        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));
        let schema: crate::Schema<DynRecord> = crate::Schema {
            mutable: Mutable::new(&option, trigger.clone(), &fs).await.unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx.clone(),
            recover_wal_ids: None,
            trigger,
            record_instance: RecordInstance::Normal,
        };

        for item in test_dyn_items().into_iter() {
            schema
                .write(LogType::Full, item, 0_u32.into())
                .await
                .unwrap();
        }
        drop(schema);

        let option = DbOption::with_path(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            "age".to_owned(),
            primary_key_index,
        );
        let db: DB<DynRecord, TokioExecutor> = DB::with_schema(
            option,
            TokioExecutor::new(),
            manager,
            desc,
            primary_key_index,
        )
        .await
        .unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_dyn_items() {
            sort_items.insert(item.key(), item);
        }

        {
            let tx = db.transaction().await;
            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(vec![0, 1, 2])
                .take()
                .await
                .unwrap();

            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns1 = entry.value().unwrap().columns;
                let (_, record) = sort_items.pop_first().unwrap();
                let columns2 = record.as_record_ref().columns;

                assert_eq!(columns1.get(1), columns2.get(1));
                assert_eq!(columns1.get(2), columns2.get(2));
            }
        }
    }

    #[tokio::test]
    async fn test_read_write_dyn() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(Arc::new(TokioFs), vec![]);

        let (cols_desc, primary_key_index) = test_dyn_item_schema();
        let mut option = DbOption::with_path(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            "age".to_string(),
            primary_key_index,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<DynRecord, TokioExecutor> = DB::with_schema(
            option,
            TokioExecutor::new(),
            manager,
            cols_desc,
            primary_key_index,
        )
        .await
        .unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            if i == 28 {
                db.remove(item.key()).await.unwrap();
            } else {
                db.write(item, 0.into()).await.unwrap();
            }
        }

        dbg!(db.version_set.current().await);
        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Column::new(Datatype::Int8, "age".to_string(), Arc::new(i as i8), false);
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                if i == 28 {
                    assert!(option1.is_none());
                    continue;
                }
                let entry = option1.unwrap();
                let record_ref = entry.get();

                assert_eq!(
                    *record_ref
                        .columns
                        .first()
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<i8>()
                        .unwrap(),
                    i as i8
                );
                let height = record_ref
                    .columns
                    .get(1)
                    .unwrap()
                    .value
                    .as_ref()
                    .downcast_ref::<Option<i16>>()
                    .unwrap();
                if i < 45 {
                    assert_eq!(*height, Some(20 * i as i16),);
                } else {
                    assert!(height.is_none());
                }
                assert_eq!(
                    *record_ref
                        .columns
                        .get(2)
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<Option<i32>>()
                        .unwrap(),
                    Some(200 * i),
                );
            }
            tx.commit().await.unwrap();
        }
        // test scan
        {
            let tx = db.transaction().await;
            let lower = Column::new(Datatype::Int8, "age".to_owned(), Arc::new(0_i8), false);
            let upper = Column::new(Datatype::Int8, "age".to_owned(), Arc::new(49_i8), false);
            let mut scan = tx
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(vec![0, 1])
                .take()
                .await
                .unwrap();

            let mut i = 0_i8;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                if i == 28 {
                    assert!(entry.value().is_none());
                    i += 1;
                    continue;
                }
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype, Datatype::Int8);
                assert_eq!(primary_key_col.name, "age".to_string());
                assert_eq!(
                    *primary_key_col.value.as_ref().downcast_ref::<i8>().unwrap(),
                    i
                );

                let col = columns.get(1).unwrap();
                assert_eq!(col.datatype, Datatype::Int16);
                assert_eq!(col.name, "height".to_string());
                let height = *col.value.as_ref().downcast_ref::<Option<i16>>().unwrap();
                if i < 45 {
                    assert_eq!(height, Some(i as i16 * 20));
                } else {
                    assert!(col
                        .value
                        .as_ref()
                        .downcast_ref::<Option<i16>>()
                        .unwrap()
                        .is_none(),);
                }

                let col = columns.get(2).unwrap();
                assert_eq!(col.datatype, Datatype::Int32);
                assert_eq!(col.name, "weight".to_string());
                let weight = col.value.as_ref().downcast_ref::<Option<i32>>();
                assert!(weight.is_some());
                assert_eq!(*weight.unwrap(), None);
                i += 1
            }
        }
    }

    #[tokio::test]
    async fn test_dyn_multiple_db() {
        let manager1 = StoreManager::new(Arc::new(TokioFs), vec![]);
        let manager2 = StoreManager::new(Arc::new(TokioFs), vec![]);
        let manager3 = StoreManager::new(Arc::new(TokioFs), vec![]);
        let temp_dir1 = TempDir::with_prefix("db1").unwrap();

        let (cols_desc, primary_key_index) = test_dyn_item_schema();
        let mut option = DbOption::with_path(
            Path::from_filesystem_path(temp_dir1.path()).unwrap(),
            "age".to_string(),
            primary_key_index,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let temp_dir2 = TempDir::with_prefix("db2").unwrap();
        let mut option2 = DbOption::with_path(
            Path::from_filesystem_path(temp_dir2.path()).unwrap(),
            "age".to_string(),
            primary_key_index,
        );
        option2.immutable_chunk_num = 1;
        option2.immutable_chunk_max_num = 1;
        option2.major_threshold_with_sst_size = 3;
        option2.major_default_oldest_table_num = 1;
        option2.trigger_type = TriggerType::Length(5);

        let temp_dir3 = TempDir::with_prefix("db3").unwrap();
        let mut option3 = DbOption::with_path(
            Path::from_filesystem_path(temp_dir3.path()).unwrap(),
            "age".to_string(),
            primary_key_index,
        );
        option3.immutable_chunk_num = 1;
        option3.immutable_chunk_max_num = 1;
        option3.major_threshold_with_sst_size = 3;
        option3.major_default_oldest_table_num = 1;
        option3.trigger_type = TriggerType::Length(5);

        let db1: DB<DynRecord, TokioExecutor> = DB::with_schema(
            option,
            TokioExecutor::new(),
            manager1,
            cols_desc.clone(),
            primary_key_index,
        )
        .await
        .unwrap();
        let db2: DB<DynRecord, TokioExecutor> = DB::with_schema(
            option2,
            TokioExecutor::new(),
            manager2,
            cols_desc.clone(),
            primary_key_index,
        )
        .await
        .unwrap();
        let db3: DB<DynRecord, TokioExecutor> = DB::with_schema(
            option3,
            TokioExecutor::new(),
            manager3,
            cols_desc,
            primary_key_index,
        )
        .await
        .unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            if i >= 40 {
                db3.write(item, 0.into()).await.unwrap();
            } else if i % 2 == 0 {
                db1.write(item, 0.into()).await.unwrap();
            } else {
                db2.write(item, 0.into()).await.unwrap();
            }
        }

        // test get
        {
            let tx1 = db1.transaction().await;
            let tx2 = db2.transaction().await;
            let tx3 = db3.transaction().await;

            for i in 0..50 {
                let key = Column::new(Datatype::Int8, "age".to_string(), Arc::new(i as i8), false);
                let option1 = tx1.get(&key, Projection::All).await.unwrap();
                let option2 = tx2.get(&key, Projection::All).await.unwrap();
                let option3 = tx3.get(&key, Projection::All).await.unwrap();
                let entry = if i >= 40 {
                    assert!(option2.is_none());
                    assert!(option1.is_none());
                    option3.unwrap()
                } else if i % 2 == 0 {
                    assert!(option2.is_none());
                    assert!(option3.is_none());
                    option1.unwrap()
                } else {
                    assert!(option1.is_none());
                    assert!(option3.is_none());
                    option2.unwrap()
                };
                let record_ref = entry.get();

                assert_eq!(
                    *record_ref
                        .columns
                        .first()
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<i8>()
                        .unwrap(),
                    i as i8
                );
                assert_eq!(
                    *record_ref
                        .columns
                        .get(2)
                        .unwrap()
                        .value
                        .as_ref()
                        .downcast_ref::<Option<i32>>()
                        .unwrap(),
                    Some(200 * i),
                );
            }
            tx1.commit().await.unwrap();
        }
        // test scan
        {
            let tx1 = db1.transaction().await;
            let lower = Column::new(Datatype::Int8, "age".to_owned(), Arc::new(8_i8), false);
            let upper = Column::new(Datatype::Int8, "age".to_owned(), Arc::new(43_i8), false);
            let mut scan = tx1
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(vec![0, 1])
                .take()
                .await
                .unwrap();

            let mut i = 8_i8;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype, Datatype::Int8);
                assert_eq!(primary_key_col.name, "age".to_string());
                assert_eq!(
                    *primary_key_col.value.as_ref().downcast_ref::<i8>().unwrap(),
                    i
                );

                i += 2
            }
            assert_eq!(i, 40);
            let tx2 = db2.transaction().await;
            let mut scan = tx2
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(vec![0, 1])
                .take()
                .await
                .unwrap();

            let mut i = 9_i8;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype, Datatype::Int8);
                assert_eq!(primary_key_col.name, "age".to_string());
                assert_eq!(
                    *primary_key_col.value.as_ref().downcast_ref::<i8>().unwrap(),
                    i
                );

                i += 2
            }
            assert_eq!(i, 41);
            let tx3 = db3.transaction().await;
            let mut scan = tx3
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(vec![0, 1])
                .take()
                .await
                .unwrap();

            let mut i = 40_i8;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype, Datatype::Int8);
                assert_eq!(primary_key_col.name, "age".to_string());
                assert_eq!(
                    *primary_key_col.value.as_ref().downcast_ref::<i8>().unwrap(),
                    i
                );

                i += 1
            }
        }
    }

    #[test]
    fn build_test() {
        let t = trybuild::TestCases::new();
        t.pass("tests/success/*.rs");
    }
    #[test]
    fn fail_build_test() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/fail/*.rs");
    }
}
