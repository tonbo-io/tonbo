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
//! `Tonbo Record` is automatically implemented by the macro [`tonbo_record`],
//! it support types like `String`, `bool`, `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`.
//!
//! ACID `optimistic` transactions for concurrent data reading and writing are
//! supported with the [`DB::transaction`] method.
//!
//! # Examples
//!
//! ```no_run
//! use std::ops::Bound;
//!
//! use fusio::path::Path;
//! use futures_util::stream::StreamExt;
//! use tokio::fs;
//! use tonbo::{executor::tokio::TokioExecutor, DbOption, Projection, Record, DB};
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
//!     let options = DbOption::new(
//!         Path::from_filesystem_path("./db_path/users").unwrap(),
//!         &UserSchema,
//!     );
//!     // pluggable async runtime and I/O
//!     let db = DB::new(options, TokioExecutor::current(), UserSchema)
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
//!                 .projection(&["email"])
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
mod context;
pub mod executor;
pub mod fs;
pub mod inmem;
pub mod magic;
mod ondisk;
pub mod option;
pub mod record;
mod scope;
pub mod snapshot;
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
use compaction::leveled::LeveledCompactor;
use context::Context;
use flume::{bounded, Sender};
use fs::FileId;
pub use fusio::{SeqRead, Write};
pub use fusio_log::{Decode, Encode};
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{immutable::Immutable, mutable::MutableMemTable};
use lockable::LockableHashMap;
use magic::USER_COLUMN_OFFSET;
pub use once_cell;
pub use parquet;
use parquet::{
    arrow::{ArrowSchemaConverter, ProjectionMask},
    errors::ParquetError,
};
use parquet_lru::{DynLruCache, NoCache};
use record::Record;
use thiserror::Error;
use timestamp::{Timestamp, TsRef};
use tokio::sync::oneshot;
pub use tonbo_macros::{KeyAttributes, Record};
use tracing::error;
use transaction::{CommitError, Transaction, TransactionEntry};
use trigger::FreezeTrigger;
use wal::log::Log;

pub use crate::option::*;
use crate::{
    compaction::{CompactTask, CompactionError, Compactor},
    executor::Executor,
    fs::{manager::StoreManager, parse_file_id, FileType},
    record::Schema,
    snapshot::Snapshot,
    stream::{
        mem_projection::MemProjectionStream, merge::MergeStream, package::PackageStream, Entry,
        ScanStream,
    },
    trigger::TriggerFactory,
    version::{cleaner::Cleaner, set::VersionSet, TransactionTs, Version, VersionError},
    wal::{log::LogType, RecoverError, WalFile},
};

pub struct DB<R, E>
where
    R: Record,
    E: Executor,
{
    schema: Arc<RwLock<DbStorage<R>>>,
    ctx: Arc<Context<R>>,
    lock_map: LockMap<<R::Schema as Schema>::Key>,
    _p: PhantomData<E>,
}

impl<R, E> DB<R, E>
where
    R: Record + Send + Sync,
    <R::Schema as Schema>::Columns: Send + Sync,
    E: Executor + Send + Sync + 'static,
{
    /// Open [`DB`] with a [`DbOption`]. This will create a new directory at the
    /// path specified in [`DbOption`] (if it does not exist before) and run it
    /// according to the configuration of [`DbOption`].
    ///
    /// For more configurable options, please refer to [`DbOption`].
    pub async fn new(option: DbOption, executor: E, schema: R::Schema) -> Result<Self, DbError<R>> {
        Self::build(
            Arc::new(option),
            executor,
            schema,
            Arc::new(NoCache::default()),
        )
        .await
    }
}

impl<R, E> DB<R, E>
where
    R: Record + Send + Sync,
    <R::Schema as Schema>::Columns: Send + Sync,
    E: Executor + Send + Sync + 'static,
{
    async fn build(
        option: Arc<DbOption>,
        executor: E,
        schema: R::Schema,
        lru_cache: ParquetLru,
    ) -> Result<Self, DbError<R>> {
        let record_schema = Arc::new(schema);
        let manager = Arc::new(StoreManager::new(
            option.base_fs.clone(),
            option.level_paths.clone(),
        )?);
        {
            manager
                .base_fs()
                .create_dir_all(&option.wal_dir_path())
                .await
                .map_err(DbError::Fusio)?;
            manager
                .base_fs()
                .create_dir_all(&option.version_log_dir_path())
                .await
                .map_err(DbError::Fusio)?;
        }
        let (task_tx, task_rx) = bounded(1);

        let (mut cleaner, clean_sender) = Cleaner::new(option.clone(), manager.clone());

        let version_set = VersionSet::new(clean_sender, option.clone(), manager.clone()).await?;
        let schema = Arc::new(RwLock::new(
            DbStorage::new(
                option.clone(),
                task_tx,
                &version_set,
                record_schema.clone(),
                &manager,
            )
            .await?,
        ));
        let ctx = Arc::new(Context::new(
            manager,
            lru_cache.clone(),
            version_set,
            record_schema.arrow_schema().clone(),
        ));
        let mut compactor = match option.compaction_option {
            CompactionOption::Leveled => Compactor::Leveled(LeveledCompactor::<R>::new(
                schema.clone(),
                record_schema,
                option.clone(),
                ctx.clone(),
            )),
        };

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });

        executor.spawn(async move {
            while let Ok(task) = task_rx.recv_async().await {
                if let Err(err) = match task {
                    CompactTask::Freeze => compactor.check_then_compaction(false).await,
                    CompactTask::Flush(option_tx) => {
                        let mut result = compactor.check_then_compaction(true).await;
                        if let Some(tx) = option_tx {
                            if result.is_ok() {
                                result = tx.send(()).map_err(|_| CompactionError::ChannelClose);
                            }
                        }
                        result
                    }
                } {
                    error!("[Compaction Error]: {}", err)
                }
            }
        });

        Ok(Self {
            schema,
            lock_map: Arc::new(Default::default()),
            ctx,
            _p: Default::default(),
        })
    }

    /// open an optimistic ACID transaction
    ///
    /// ## Examples
    /// ```ignore
    /// #[derive(Record, Debug)]
    /// pub struct User {
    ///     #[record(primary_key)]
    ///     name: String,
    ///     age: u8,
    /// }
    /// ```
    /// ```ignore
    /// let mut txn = db.transaction().await;
    /// txn.insert(User {
    ///     name: "Alice".into(),
    ///     email: None,
    ///     age: 20,
    /// });
    /// txn.scan((Bound::Included("Alice"), Bound::Excluded("Bob")))
    ///     .projection(&["age"])
    ///     .limit(10)
    ///     .take()
    ///     .await
    ///     .unwrap();
    ///
    /// txn.commit().await.unwrap();
    /// ```
    pub async fn transaction(&self) -> Transaction<'_, R> {
        Transaction::new(self.snapshot().await, self.lock_map.clone())
    }

    pub async fn snapshot(&self) -> Snapshot<'_, R> {
        Snapshot::new(
            self.schema.read().await,
            self.ctx.version_set().current().await,
            self.ctx.clone(),
        )
    }

    /// insert a single tonbo record
    pub async fn insert(&self, record: R) -> Result<(), CommitError<R>> {
        Ok(self.write(record, self.ctx.increase_ts()).await?)
    }

    /// insert a sequence of data as a single batch
    pub async fn insert_batch(
        &self,
        records: impl ExactSizeIterator<Item = R>,
    ) -> Result<(), CommitError<R>> {
        Ok(self.write_batch(records, self.ctx.increase_ts()).await?)
    }

    /// delete the record with the primary key as the `key`
    pub async fn remove(&self, key: <R::Schema as Schema>::Key) -> Result<bool, CommitError<R>> {
        Ok(self
            .schema
            .read()
            .await
            .remove(LogType::Full, key, self.ctx.increase_ts())
            .await?)
    }

    /// trigger compaction manually. This will flush the WAL and trigger compaction
    pub async fn flush(&self) -> Result<(), CommitError<R>> {
        let (tx, rx) = oneshot::channel();
        let compaction_tx = { self.schema.read().await.compaction_tx.clone() };
        compaction_tx
            .send_async(CompactTask::Flush(Some(tx)))
            .await?;

        rx.await.map_err(|_| CommitError::ChannelClose)?;

        Ok(())
    }

    /// get the record with `key` as the primary key and process it using closure `f`
    pub async fn get<T>(
        &self,
        key: &<R::Schema as Schema>::Key,
        mut f: impl FnMut(TransactionEntry<'_, R>) -> Option<T>,
    ) -> Result<Option<T>, CommitError<R>> {
        Ok(self
            .schema
            .read()
            .await
            .get(
                &self.ctx,
                &*self.ctx.version_set.current().await,
                key,
                self.ctx.load_ts(),
                Projection::All,
            )
            .await?
            .and_then(|entry| {
                if entry.value().is_none() {
                    None
                } else {
                    f(TransactionEntry::Stream(entry))
                }
            }))
    }

    /// scan records with primary keys in the `range` and process them using closure `f`
    pub async fn scan<'scan, T: 'scan>(
        &'scan self,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
        mut f: impl FnMut(TransactionEntry<'_, R>) -> T + 'scan,
    ) -> impl Stream<Item = Result<T, CommitError<R>>> + 'scan {
        stream! {
            let schema = self.schema.read().await;
            let current = self.ctx.version_set.current().await;
            let mut scan = Scan::new(
                &schema,
                range,
                self.ctx.load_ts(),
                &*current,
                Box::new(|_| None),
                self.ctx.clone(),
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

    /// flush WAL to the stable storage. If WAL is disabled, this method will do nothing.
    ///
    /// There is no guarantee that the data will be flushed to WAL because of the buffer. So it is
    /// necessary to call this method before exiting if data loss is not acceptable. See also
    /// [`DbOption::disable_wal`] and [`DbOption::wal_buffer_size`].
    pub async fn flush_wal(&self) -> Result<(), DbError<R>> {
        self.schema.write().await.flush_wal().await?;
        Ok(())
    }

    /// destroy [`DB`].
    ///
    /// **Note:** This will remove all wal and manifest file in the directory.
    pub async fn destroy(self) -> Result<(), DbError<R>> {
        self.schema.write().await.destroy(&self.ctx.manager).await?;
        if let Some(ctx) = Arc::into_inner(self.ctx) {
            ctx.version_set.destroy().await?;
        }

        Ok(())
    }
}

pub(crate) struct DbStorage<R>
where
    R: Record,
{
    pub mutable: MutableMemTable<R>,
    pub immutables: Vec<(Option<FileId>, Immutable<<R::Schema as Schema>::Columns>)>,
    compaction_tx: Sender<CompactTask>,
    recover_wal_ids: Option<Vec<FileId>>,
    trigger: Arc<dyn FreezeTrigger<R>>,
    record_schema: Arc<R::Schema>,
    option: Arc<DbOption>,
}

impl<R> DbStorage<R>
where
    R: Record + Send,
{
    async fn new(
        option: Arc<DbOption>,
        compaction_tx: Sender<CompactTask>,
        version_set: &VersionSet<R>,
        record_schema: Arc<R::Schema>,
        manager: &StoreManager,
    ) -> Result<Self, DbError<R>> {
        let base_fs = manager.base_fs();
        let wal_dir_path = option.wal_dir_path();
        let mut transaction_map = HashMap::new();
        let mut wal_ids = Vec::new();

        let wal_metas = {
            let mut wal_metas = Vec::new();
            let mut wal_stream = base_fs.list(&wal_dir_path).await?;

            while let Some(file_meta) = wal_stream.next().await {
                let file_meta = file_meta?;
                if file_meta.path.as_ref().ends_with("wal") {
                    wal_metas.push(file_meta);
                }
            }
            wal_metas.sort_by(|meta_a, meta_b| meta_a.path.cmp(&meta_b.path));
            wal_metas
        };

        let trigger = TriggerFactory::create(option.trigger_type);
        let mut schema = DbStorage {
            mutable: MutableMemTable::new(
                &option,
                trigger.clone(),
                manager.base_fs().clone(),
                record_schema.clone(),
            )
            .await?,
            immutables: Default::default(),
            compaction_tx,
            recover_wal_ids: None,
            trigger,
            record_schema,
            option: option.clone(),
        };

        for wal_meta in wal_metas {
            let wal_path = wal_meta.path;

            // SAFETY: wal_stream return only file name
            let wal_id = parse_file_id(&wal_path, FileType::Wal)?.unwrap();
            wal_ids.push(wal_id);

            let mut recover_stream =
                pin!(WalFile::<R>::recover(option.base_fs.clone(), wal_path).await);
            while let Some(record) = recover_stream.next().await {
                let record_batch = record?;

                for entry in record_batch {
                    let Log {
                        key,
                        value,
                        log_type,
                    } = entry;
                    let ts = key.ts;
                    let key = key.value;

                    let is_excess = match log_type.unwrap() {
                        LogType::Full => {
                            schema
                                .recover_append(key, version_set.increase_ts(), value)
                                .await?
                        }
                        LogType::First => {
                            transaction_map.insert(ts, vec![(key, value)]);
                            false
                        }
                        LogType::Middle => {
                            transaction_map.get_mut(&ts).unwrap().push((key, value));
                            false
                        }
                        LogType::Last => {
                            let mut is_excess = false;
                            let mut records = transaction_map.remove(&ts).unwrap();
                            records.push((key, value));

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
        key: <R::Schema as Schema>::Key,
        ts: Timestamp,
    ) -> Result<bool, DbError<R>> {
        self.mutable.remove(log_ty, key, ts).await
    }

    async fn recover_append(
        &self,
        key: <R::Schema as Schema>::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<bool, DbError<R>> {
        self.mutable.append(None, key, ts, value).await
    }

    async fn get<'get>(
        &'get self,
        ctx: &Context<R>,
        version: &'get Version<R>,
        key: &'get <R::Schema as Schema>::Key,
        ts: Timestamp,
        projection: Projection<'get>,
    ) -> Result<Option<Entry<'get, R>>, DbError<R>> {
        let primary_key_index = self.record_schema.primary_key_index();
        let schema = ctx.arrow_schema();

        let projection = match projection {
            Projection::All => ProjectionMask::all(),
            Projection::Parts(projection) => {
                let mut fixed_projection: Vec<usize> = [0, 1, primary_key_index]
                    .into_iter()
                    .chain(projection.into_iter().map(|name| {
                        schema
                            .index_of(name)
                            .unwrap_or_else(|_| panic!("unexpected field {}", name))
                    }))
                    .collect();
                fixed_projection.dedup();

                ProjectionMask::roots(
                    &ArrowSchemaConverter::new().convert(schema).unwrap(),
                    fixed_projection,
                )
            }
        };

        if let Some(entry) = self.mutable.get(key, ts) {
            return Ok(Some(Entry::Projection((
                Box::new(Entry::Mutable(entry)),
                Arc::new(projection),
            ))));
        }

        for (_, immutable) in self.immutables.iter().rev() {
            if let Some(entry) = immutable.get(key, ts, projection.clone()) {
                return Ok(Some(Entry::RecordBatch(entry)));
            }
        }

        Ok(version
            .query(
                ctx.storage_manager(),
                TsRef::new(key, ts),
                projection,
                ctx.cache().clone(),
            )
            .await?
            .map(|entry| Entry::RecordBatch(entry)))
    }

    fn check_conflict(&self, key: &<R::Schema as Schema>::Key, ts: Timestamp) -> bool {
        self.mutable.check_conflict(key, ts)
            || self
                .immutables
                .iter()
                .rev()
                .any(|(_, immutable)| immutable.check_conflict(key, ts))
    }

    async fn flush_wal(&self) -> Result<(), DbError<R>> {
        self.mutable.flush_wal().await?;
        Ok(())
    }

    async fn destroy(&mut self, manager: &StoreManager) -> Result<(), DbError<R>> {
        self.mutable.destroy().await?;

        let base_fs = manager.base_fs();
        let wal_dir_path = self.option.wal_dir_path();
        let mut wal_stream = base_fs.list(&wal_dir_path).await?;
        let fs = manager.base_fs();

        while let Some(file_meta) = wal_stream.next().await {
            fs.remove(&file_meta?.path).await?;
        }
        Ok(())
    }
}

/// scan configuration intermediate structure
pub struct Scan<'scan, 'range, R>
where
    R: Record,
    'range: 'scan,
{
    schema: &'scan DbStorage<R>,
    lower: Bound<&'range <R::Schema as Schema>::Key>,
    upper: Bound<&'range <R::Schema as Schema>::Key>,
    ts: Timestamp,

    version: &'scan Version<R>,
    fn_pre_stream:
        Box<dyn FnOnce(Option<ProjectionMask>) -> Option<ScanStream<'scan, R>> + Send + 'scan>,

    limit: Option<usize>,
    projection_indices: Option<Vec<usize>>,
    projection: ProjectionMask,
    ctx: Arc<Context<R>>,
}

impl<'scan, 'range, R> Scan<'scan, 'range, R>
where
    R: Record + Send,
{
    fn new(
        schema: &'scan DbStorage<R>,
        (lower, upper): (
            Bound<&'range <R::Schema as Schema>::Key>,
            Bound<&'range <R::Schema as Schema>::Key>,
        ),
        ts: Timestamp,
        version: &'scan Version<R>,
        fn_pre_stream: Box<
            dyn FnOnce(Option<ProjectionMask>) -> Option<ScanStream<'scan, R>> + Send + 'scan,
        >,
        ctx: Arc<Context<R>>,
    ) -> Self {
        Self {
            schema,
            lower,
            upper,
            ts,
            version,
            fn_pre_stream,
            limit: None,
            projection_indices: None,
            projection: ProjectionMask::all(),
            ctx,
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
    pub fn projection(self, projection: &[&str]) -> Self {
        let schema = self.schema.record_schema.arrow_schema();
        let mut projection = projection
            .iter()
            .map(|name| {
                schema
                    .index_of(name)
                    .unwrap_or_else(|_| panic!("unexpected field {}", name))
            })
            .collect::<Vec<usize>>();
        let primary_key_index = self.schema.record_schema.primary_key_index();
        let mut fixed_projection = vec![0, 1, primary_key_index];
        fixed_projection.append(&mut projection);
        fixed_projection.dedup();

        let mask = ProjectionMask::roots(
            &ArrowSchemaConverter::new().convert(schema).unwrap(),
            fixed_projection.clone(),
        );

        Self {
            projection: mask,
            projection_indices: Some(fixed_projection),
            ..self
        }
    }

    /// fields in projection Record by field indices
    pub fn projection_with_index(self, mut projection: Vec<usize>) -> Self {
        // skip two columns: _null and _ts
        for p in &mut projection {
            *p += USER_COLUMN_OFFSET;
        }
        let primary_key_index = self.schema.record_schema.primary_key_index();
        let mut fixed_projection = vec![0, 1, primary_key_index];
        fixed_projection.append(&mut projection);
        fixed_projection.dedup();

        let mask = ProjectionMask::roots(
            &ArrowSchemaConverter::new()
                .convert(self.schema.record_schema.arrow_schema())
                .unwrap(),
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
                &self.ctx,
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
    ) -> Result<
        impl Stream<Item = Result<<R::Schema as Schema>::Columns, ParquetError>> + 'scan,
        DbError<R>,
    > {
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
                &self.ctx,
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
            self.ctx.arrow_schema().clone(),
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
    #[error("write log error: {0}")]
    Logger(#[from] fusio_log::error::LogError),
}

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

pub enum Projection<'r> {
    All,
    Parts(Vec<&'r str>),
}

pub type ParquetLru = Arc<dyn DynLruCache<FileId> + Send + Sync>;

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::{
        collections::{BTreeMap, Bound},
        mem,
        sync::Arc,
    };

    use arrow::{
        array::{Array, AsArray, RecordBatch},
        datatypes::{Schema, UInt32Type},
    };
    use async_lock::RwLock;
    use flume::{bounded, Receiver};
    use fusio::{disk::TokioFs, path::Path, DynFs, SeqRead, Write};
    use fusio_dispatch::FsOptions;
    use fusio_log::{Decode, Encode};
    use futures::StreamExt;
    use parquet::arrow::ProjectionMask;
    use parquet_lru::NoCache;
    use tempfile::TempDir;
    use tracing::error;

    use crate::{
        cast_arc_value,
        compaction::{leveled::LeveledCompactor, CompactTask, CompactionError, Compactor},
        context::Context,
        executor::{tokio::TokioExecutor, Executor},
        fs::{generate_file_id, manager::StoreManager},
        inmem::{immutable::tests::TestSchema, mutable::MutableMemTable},
        record::{
            option::OptionRecordRef,
            runtime::test::{test_dyn_item_schema, test_dyn_items},
            DataType, DynRecord, Key, RecordDecodeError, RecordEncodeError, RecordRef,
            Schema as RecordSchema, Value, F32, F64,
        },
        trigger::{TriggerFactory, TriggerType},
        version::{cleaner::Cleaner, set::tests::build_version_set, Version},
        wal::log::LogType,
        CompactionOption, DbError, DbOption, Projection, Record, DB,
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
            R: SeqRead,
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
        type Schema = TestSchema;

        type Ref<'r>
            = TestRef<'r>
        where
            Self: 'r;

        fn key(&self) -> &str {
            &self.vstring
        }

        fn as_record_ref(&self) -> Self::Ref<'_> {
            TestRef {
                vstring: &self.vstring,
                vu32: Some(self.vu32),
                vbool: self.vbool,
            }
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
            W: Write,
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

        fn key(self) -> <<<Self::Record as Record>::Schema as RecordSchema>::Key as Key>::Ref<'r> {
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
        ) -> OptionRecordRef<'r, Self> {
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
            OptionRecordRef::new(ts, record, null)
        }
    }

    pub(crate) async fn get_test_record_batch<E: Executor + Send + Sync + 'static>(
        option: DbOption,
        executor: E,
    ) -> RecordBatch {
        let db: DB<Test, E> = DB::new(option.clone(), executor, TestSchema {})
            .await
            .unwrap();
        let base_fs = db.ctx.manager.base_fs();

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
            MutableMemTable::new(&option, trigger, base_fs.clone(), Arc::new(TestSchema {}))
                .await
                .unwrap(),
        );

        mutable
            .into_immutable()
            .await
            .unwrap()
            .1
            .as_record_batch()
            .clone()
    }

    pub(crate) async fn build_schema(
        option: Arc<DbOption>,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(crate::DbStorage<Test>, Receiver<CompactTask>), fusio::Error> {
        let trigger = TriggerFactory::create(option.trigger_type);

        let mutable = MutableMemTable::new(
            &option,
            trigger.clone(),
            fs.clone(),
            Arc::new(TestSchema {}),
        )
        .await?;

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
            let trigger = TriggerFactory::create(option.trigger_type);

            let mutable: MutableMemTable<Test> =
                MutableMemTable::new(&option, trigger.clone(), fs.clone(), Arc::new(TestSchema))
                    .await?;

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
                Some(generate_file_id()),
                mutable.into_immutable().await.unwrap().1,
            )]
        };

        let (compaction_tx, compaction_rx) = bounded(1);

        Ok((
            crate::DbStorage {
                mutable,
                immutables,
                compaction_tx,
                recover_wal_ids: None,
                trigger,
                record_schema: Arc::new(TestSchema {}),
                option,
            },
            compaction_rx,
        ))
    }

    pub(crate) async fn build_db<R, E>(
        option: Arc<DbOption>,
        compaction_rx: Receiver<CompactTask>,
        executor: E,
        schema: crate::DbStorage<R>,
        record_schema: Arc<R::Schema>,
        version: Version<R>,
        manager: Arc<StoreManager>,
    ) -> Result<DB<R, E>, DbError<R>>
    where
        R: Record + Send + Sync,
        <R::Schema as RecordSchema>::Columns: Send + Sync,
        E: Executor + Send + Sync + 'static,
    {
        {
            let base_fs = manager.base_fs();

            let _ = base_fs.create_dir_all(&option.wal_dir_path()).await;
            let _ = base_fs.create_dir_all(&option.version_log_dir_path()).await;
        }

        let schema = Arc::new(RwLock::new(schema));

        let (mut cleaner, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let version_set =
            build_version_set(version, clean_sender, option.clone(), manager.clone()).await?;
        let ctx = Arc::new(Context::new(
            manager,
            Arc::new(NoCache::default()),
            version_set,
            TestSchema.arrow_schema().clone(),
        ));
        let mut compactor = match option.compaction_option {
            CompactionOption::Leveled => Compactor::Leveled(LeveledCompactor::<R>::new(
                schema.clone(),
                record_schema,
                option.clone(),
                ctx.clone(),
            )),
        };

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });
        executor.spawn(async move {
            while let Ok(task) = compaction_rx.recv_async().await {
                if let Err(err) = match task {
                    CompactTask::Freeze => compactor.check_then_compaction(false).await,
                    CompactTask::Flush(option_tx) => {
                        let mut result = compactor.check_then_compaction(true).await;
                        if let Some(tx) = option_tx {
                            let channel_result =
                                tx.send(()).map_err(|_| CompactionError::ChannelClose);
                            if result.is_ok() {
                                result = channel_result;
                            }
                        }
                        result
                    }
                } {
                    error!("[Compaction Error]: {}", err)
                }
            }
        });

        Ok(DB {
            schema,
            lock_map: Arc::new(Default::default()),
            ctx,
            _p: Default::default(),
        })
    }

    fn test_items() -> Vec<Test> {
        let mut items = Vec::new();
        for i in 0..32 {
            items.push(Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            });
        }
        items
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_from_disk() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_l0 = TempDir::new().unwrap();

        let path = Path::from_filesystem_path(temp_dir.path()).unwrap();
        let path_l0 = Path::from_filesystem_path(temp_dir_l0.path()).unwrap();

        let mut option = DbOption::new(path, &TestSchema)
            .level_path(0, path_l0, FsOptions::Local)
            .unwrap();
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(/* max_mutable_len */ 5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
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

        dbg!(db.ctx.version_set.current().await);

        let version = db.ctx.version_set.current().await;
        assert!(!version.level_slice[1].is_empty());

        assert_eq!(option1.get().vstring, "20");
        assert_eq!(option1.get().vu32, Some(20));
        assert_eq!(option1.get().vbool, Some(true));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(/* max_mutable_len */ 50);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        for item in &test_items()[0..10] {
            db.write(item.clone(), 0.into()).await.unwrap();
        }
        db.flush().await.unwrap();
        for item in &test_items()[10..20] {
            db.write(item.clone(), 0.into()).await.unwrap();
        }

        dbg!(db.ctx.version_set.current().await);
        db.flush().await.unwrap();
        dbg!(db.ctx.version_set.current().await);

        let version = db.ctx.version_set.current().await;
        assert!(!version.level_slice[0].is_empty());
    }

    #[ignore = "s3"]
    #[cfg(all(feature = "aws", feature = "tokio-http"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_s3_recover() {
        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();
        let bucket = std::env::var("BUCKET_NAME").expect("expected s3 bucket not to be empty");
        let region = Some(std::env::var("AWS_REGION").expect("expected s3 region not to be empty"));
        let token = Some(std::option_env!("AWS_SESSION_TOKEN").unwrap().to_string());

        let s3_option = FsOptions::S3 {
            bucket,
            credential: Some(fusio::remotes::aws::AwsCredential {
                key_id,
                secret_key,
                token,
            }),
            endpoint: None,
            region,
            sign_payload: None,
            checksum: None,
        };
        let option = DbOption::new(
            Path::from_url_path(format!("recover/{}", generate_file_id())).unwrap(),
            &TestSchema {},
        )
        .base_fs(s3_option.clone());
        {
            let db = DB::new(option.clone(), TokioExecutor::current(), TestSchema)
                .await
                .unwrap();

            for item in &test_items()[0..10] {
                db.insert(item.clone()).await.unwrap();
            }
            // flush to s3
            db.flush().await.unwrap();
            drop(db);
        }
        {
            // remove files in local disk
            let wal_path = option.wal_dir_path().to_string();
            let path = std::path::Path::new(wal_path.as_str());
            if path.exists() {
                std::fs::remove_dir_all(path).unwrap();
            }
            let version_path = option.version_log_dir_path().to_string();
            let path = std::path::Path::new(version_path.as_str());
            if path.exists() {
                std::fs::remove_dir_all(path).unwrap();
            }
        }
        // test recover from s3
        {
            let db: DB<Test, TokioExecutor> =
                DB::new(option.clone(), TokioExecutor::current(), TestSchema)
                    .await
                    .unwrap();
            let mut sort_items = BTreeMap::new();
            for item in test_items()[0..10].iter() {
                sort_items.insert(item.vstring.clone(), item.clone());
            }
            let tx = db.transaction().await;
            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .projection(&["vstring", "vu32", "vbool"])
                .take()
                .await
                .unwrap();

            while let Some(actual) = scan.next().await.transpose().unwrap() {
                let (expected_key, expected) = sort_items.pop_first().unwrap();
                assert_eq!(actual.key().value, expected_key);
                assert_eq!(actual.value().as_ref().unwrap().vstring, expected.vstring);
                assert_eq!(actual.value().as_ref().unwrap().vu32, Some(expected.vu32));
                assert_eq!(actual.value().as_ref().unwrap().vbool, expected.vbool);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn schema_recover() {
        let temp_dir = TempDir::new().unwrap();
        let fs = Arc::new(TokioFs) as Arc<dyn DynFs>;

        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        ));
        fs.create_dir_all(&option.wal_dir_path()).await.unwrap();

        let (task_tx, _task_rx) = bounded(1);

        let trigger = TriggerFactory::create(option.trigger_type);
        let schema: crate::DbStorage<Test> = crate::DbStorage {
            mutable: MutableMemTable::new(&option, trigger.clone(), fs, Arc::new(TestSchema))
                .await
                .unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx.clone(),
            recover_wal_ids: None,
            trigger,
            record_schema: Arc::new(TestSchema),
            option: option.clone(),
        };

        for (i, item) in test_items().into_iter().enumerate() {
            schema
                .write(LogType::Full, item, (i as u32).into())
                .await
                .unwrap();
        }
        schema.flush_wal().await.unwrap();
        drop(schema);

        let db: DB<Test, TokioExecutor> = DB::new(
            option.as_ref().to_owned(),
            TokioExecutor::current(),
            TestSchema,
        )
        .await
        .unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_items() {
            sort_items.insert(item.vstring.clone(), item);
        }
        {
            let tx = db.transaction().await;
            let mut scan = tx
                .scan((Bound::Unbounded, Bound::Unbounded))
                .take()
                .await
                .unwrap();

            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let (_, test) = sort_items.pop_first().unwrap();

                assert_eq!(entry.key().value, test.key());
                assert_eq!(entry.value().as_ref().unwrap().vstring, test.vstring);
                assert_eq!(entry.value().as_ref().unwrap().vu32, Some(test.vu32));
                assert_eq!(entry.value().as_ref().unwrap().vbool, test.vbool);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dyn_schema_recover() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();

        let dyn_schema = Arc::new(test_dyn_item_schema());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            dyn_schema.as_ref(),
        ));
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (task_tx, _task_rx) = bounded(1);

        let trigger = TriggerFactory::create(option.trigger_type);
        let schema: crate::DbStorage<DynRecord> = crate::DbStorage {
            mutable: MutableMemTable::new(
                &option,
                trigger.clone(),
                manager.base_fs().clone(),
                dyn_schema.clone(),
            )
            .await
            .unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx.clone(),
            recover_wal_ids: None,
            trigger,
            record_schema: dyn_schema.clone(),
            option,
        };

        for item in test_dyn_items().into_iter() {
            schema
                .write(LogType::Full, item, 0_u32.into())
                .await
                .unwrap();
        }
        schema.flush_wal().await.unwrap();
        drop(schema);

        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            dyn_schema.as_ref(),
        );
        let dyn_schema = test_dyn_item_schema();
        let db: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::current(), dyn_schema)
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
                .projection(&["id", "age", "height"])
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_removed() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);
        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        for (idx, item) in test_items().into_iter().enumerate() {
            if idx % 2 == 0 {
                db.write(item, 0.into()).await.unwrap();
            } else {
                db.remove(item.vstring).await.unwrap();
            }
        }

        for i in 0..32 {
            let vstring = db
                .get(&i.to_string(), |e| Some(e.get().vstring.to_string()))
                .await
                .unwrap();
            if i % 2 == 0 {
                assert!(vstring.is_some());
                assert_eq!(vstring, Some(i.to_string()));
            } else {
                assert!(vstring.is_none());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_write_dyn() {
        let temp_dir = TempDir::new().unwrap();

        let dyn_schema = test_dyn_item_schema();
        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &dyn_schema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::current(), dyn_schema)
                .await
                .unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            if i == 28 {
                db.remove(item.key()).await.unwrap();
            } else {
                db.write(item, 0.into()).await.unwrap();
            }
        }

        dbg!(db.ctx.version_set.current().await);
        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Value::new(DataType::Int64, "id".to_string(), Arc::new(i as i64), false);
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                if i == 28 {
                    assert!(option1.is_none());
                    continue;
                }
                let entry = option1.unwrap();
                let record_ref = entry.get();

                assert_eq!(
                    *cast_arc_value!(record_ref.columns.first().unwrap().value, i64),
                    i as i64
                );
                let height = cast_arc_value!(record_ref.columns.get(2).unwrap().value, Option<i16>);
                if i < 45 {
                    assert_eq!(*height, Some(20 * i as i16),);
                } else {
                    assert!(height.is_none());
                }
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(3).unwrap().value, Option<i32>),
                    Some(200 * i),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(4).unwrap().value, Option<String>),
                    Some(i.to_string()),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(5).unwrap().value, Option<String>),
                    Some(format!("{}@tonbo.io", i)),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(6).unwrap().value, Option<bool>),
                    Some(i % 2 == 0),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(7).unwrap().value, Option<Vec<u8>>),
                    Some(i.to_le_bytes().to_vec()),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(8).unwrap().value, Option<F32>),
                    Some(F32::from(i as f32 * 1.11)),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(9).unwrap().value, Option<F64>),
                    Some(F64::from(i as f64 * 1.01)),
                );
            }
            tx.commit().await.unwrap();
        }
        // test scan
        {
            let tx = db.transaction().await;
            let lower = Value::new(DataType::Int64, "id".to_owned(), Arc::new(0_i64), false);
            let upper = Value::new(DataType::Int64, "id".to_owned(), Arc::new(49_i64), false);
            let mut scan = tx
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "height", "bytes", "grade", "price"])
                .take()
                .await
                .unwrap();

            let mut i = 0_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                if i == 28 {
                    assert!(entry.value().is_none());
                    i += 1;
                    continue;
                }
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype(), DataType::Int64);
                assert_eq!(primary_key_col.desc.name, "id".to_string());
                assert_eq!(
                    *primary_key_col
                        .value
                        .as_ref()
                        .downcast_ref::<i64>()
                        .unwrap(),
                    i
                );

                let col = columns.get(2).unwrap();
                assert_eq!(col.datatype(), DataType::Int16);
                assert_eq!(col.desc.name, "height".to_string());
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

                let col = columns.get(3).unwrap();
                assert_eq!(col.datatype(), DataType::Int32);
                assert_eq!(col.desc.name, "weight".to_string());
                let weight = col.value.as_ref().downcast_ref::<Option<i32>>();
                assert!(weight.is_some());
                assert_eq!(*weight.unwrap(), None);

                let col = columns.get(4).unwrap();
                assert_eq!(col.datatype(), DataType::String);
                assert_eq!(col.desc.name, "name".to_string());
                let name = col.value.as_ref().downcast_ref::<Option<String>>();
                assert!(name.is_some());
                assert_eq!(name.unwrap(), &None);

                let col = columns.get(6).unwrap();
                assert_eq!(col.datatype(), DataType::Boolean);
                assert_eq!(col.desc.name, "enabled".to_string());
                let enabled = col.value.as_ref().downcast_ref::<Option<bool>>();
                assert!(enabled.is_some());
                assert_eq!(*enabled.unwrap(), None);

                let col = columns.get(7).unwrap();
                assert_eq!(col.datatype(), DataType::Bytes);
                assert_eq!(col.desc.name, "bytes".to_string());
                let bytes = col.value.as_ref().downcast_ref::<Option<Vec<u8>>>();
                assert!(bytes.is_some());
                assert_eq!(bytes.unwrap(), &Some((i as i32).to_le_bytes().to_vec()));

                let col = columns.get(8).unwrap();
                assert_eq!(col.datatype(), DataType::Float32);
                let v = col.value.as_ref().downcast_ref::<Option<F32>>();
                assert!(v.is_some());
                assert_eq!(v.unwrap(), &Some(F32::from(i as f32 * 1.11)));

                let col = columns.get(9).unwrap();
                assert_eq!(col.datatype(), DataType::Float64);
                let v = col.value.as_ref().downcast_ref::<Option<F64>>();
                assert!(v.is_some());
                assert_eq!(v.unwrap(), &Some(F64::from(i as f64 * 1.01)));
                i += 1
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dyn_multiple_db() {
        let temp_dir1 = TempDir::with_prefix("db1").unwrap();

        let dyn_schema = test_dyn_item_schema();
        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir1.path()).unwrap(),
            &dyn_schema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 3;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let temp_dir2 = TempDir::with_prefix("db2").unwrap();
        let mut option2 = DbOption::new(
            Path::from_filesystem_path(temp_dir2.path()).unwrap(),
            &dyn_schema,
        );
        option2.immutable_chunk_num = 1;
        option2.immutable_chunk_max_num = 1;
        option2.major_threshold_with_sst_size = 3;
        option2.major_default_oldest_table_num = 1;
        option2.trigger_type = TriggerType::Length(5);

        let temp_dir3 = TempDir::with_prefix("db3").unwrap();
        let mut option3 = DbOption::new(
            Path::from_filesystem_path(temp_dir3.path()).unwrap(),
            &dyn_schema,
        );
        option3.immutable_chunk_num = 1;
        option3.immutable_chunk_max_num = 1;
        option3.major_threshold_with_sst_size = 3;
        option3.major_default_oldest_table_num = 1;
        option3.trigger_type = TriggerType::Length(5);

        let db1: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::current(), test_dyn_item_schema())
                .await
                .unwrap();
        let db2: DB<DynRecord, TokioExecutor> =
            DB::new(option2, TokioExecutor::current(), test_dyn_item_schema())
                .await
                .unwrap();
        let db3: DB<DynRecord, TokioExecutor> =
            DB::new(option3, TokioExecutor::current(), test_dyn_item_schema())
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
                let key = Value::new(DataType::Int64, "id".to_string(), Arc::new(i as i64), false);
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
                    *cast_arc_value!(record_ref.columns.first().unwrap().value, i64),
                    i as i64
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(3).unwrap().value, Option<i32>),
                    Some(200 * i),
                );
                assert_eq!(
                    *cast_arc_value!(record_ref.columns.get(4).unwrap().value, Option<String>),
                    Some(i.to_string()),
                );
            }
            tx1.commit().await.unwrap();
        }
        // test scan
        {
            let tx1 = db1.transaction().await;
            let lower = Value::new(DataType::Int64, "id".to_owned(), Arc::new(8_i64), false);
            let upper = Value::new(DataType::Int64, "id".to_owned(), Arc::new(43_i64), false);
            let mut scan = tx1
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "age"])
                .take()
                .await
                .unwrap();

            let mut i = 8_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype(), DataType::Int64);
                assert_eq!(primary_key_col.desc.name, "id".to_string());
                assert_eq!(*cast_arc_value!(primary_key_col.value, i64), i);

                i += 2
            }
            assert_eq!(i, 40);
            let tx2 = db2.transaction().await;
            let mut scan = tx2
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "age"])
                .take()
                .await
                .unwrap();

            let mut i = 9_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype(), DataType::Int64);
                assert_eq!(primary_key_col.desc.name, "id".to_string());
                assert_eq!(*cast_arc_value!(primary_key_col.value, i64), i);

                i += 2
            }
            assert_eq!(i, 41);
            let tx3 = db3.transaction().await;
            let mut scan = tx3
                .scan((Bound::Included(&lower), Bound::Included(&upper)))
                .projection(&["id", "age"])
                .take()
                .await
                .unwrap();

            let mut i = 40_i64;
            while let Some(entry) = scan.next().await.transpose().unwrap() {
                let columns = entry.value().unwrap().columns;

                let primary_key_col = columns.first().unwrap();
                assert_eq!(primary_key_col.datatype(), DataType::Int64);
                assert_eq!(primary_key_col.desc.name, "id".to_string());
                assert_eq!(*cast_arc_value!(primary_key_col.value, i64), i);

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
