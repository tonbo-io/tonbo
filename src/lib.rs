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
//!     let db = DB::new(options, TokioExecutor::default(), UserSchema)
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
pub mod compaction;
pub mod context;
pub mod executor;
pub mod fs;
pub mod inmem;
pub(crate) mod magic;
mod manifest;
mod ondisk;
pub mod option;
pub mod query;
pub mod record;
pub mod scope;
pub(crate) mod snapshot;
pub mod stream;
pub mod transaction;
mod trigger;
pub mod version;
mod wal;

use std::{
    collections::HashMap,
    future::Future,
    io,
    marker::PhantomData,
    mem,
    ops::Bound,
    pin::pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub use arrow;
use async_stream::stream;
use context::Context;
use flume::{bounded, Sender};
use fs::FileId;
use fusio::{MaybeSend, MaybeSync};
pub use fusio::{SeqRead, Write};
pub use fusio_log::{Decode, Encode};
use futures::channel::oneshot;
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{
    immutable::ImmutableMemTable,
    mutable::{MutableMemTable, WriteResult},
};
use lockable::LockableHashMap;
use magic::USER_COLUMN_OFFSET;
use manifest::ManifestStorageError;
pub use once_cell;
pub use parquet;
use parquet::{
    arrow::{ArrowSchemaConverter, ProjectionMask},
    errors::ParquetError,
};
use parquet_lru::{DynLruCache, NoCache};
use record::Record;
use thiserror::Error;
pub use tonbo_macros::{KeyAttributes, Record};
use tracing::error;
use transaction::{CommitError, Transaction, TransactionEntry};
use trigger::FreezeTrigger;
use version::timestamp::{Timestamp, TsRef};
use wal::log::Log;

#[doc(hidden)]
pub use crate::magic::TS;
// Re-export items needed by macros and tests
#[doc(hidden)]
pub use crate::record::{ArrowArrays, ArrowArraysBuilder};
#[doc(hidden)]
pub use crate::version::timestamp::Ts;
use crate::{
    compaction::{error::CompactionError, leveled::LeveledCompactor, CompactTask, Compactor},
    executor::{Executor, RwLock as ExecutorRwLock},
    fs::{manager::StoreManager, parse_file_id, FileType},
    inmem::flush::minor_flush,
    manifest::ManifestStorage,
    record::Schema,
    snapshot::Snapshot,
    stream::{
        mem_projection::MemProjectionStream, merge::MergeStream, package::PackageStream, ScanStream,
    },
    trigger::TriggerFactory,
    version::{cleaner::Cleaner, error::VersionError, set::VersionSet, Version, VersionRef},
    wal::{log::LogType, RecoverError, WalFile},
};
pub use crate::{option::*, stream::Entry};

pub trait CompactionExecutor<R: Record>: MaybeSend + MaybeSync {
    fn check_then_compaction<'a>(
        &'a self,
        batches: Option<
            &'a [(
                Option<FileId>,
                ImmutableMemTable<<R::Schema as Schema>::Columns>,
            )],
        >,
        recover_wal_ids: Option<Vec<FileId>>,
        is_manual: bool,
    ) -> impl Future<Output = Result<(), CompactionError<R>>> + MaybeSend + 'a;
}

// Implementation for custom compactors (Box<dyn Compactor<R>>)
impl<R: Record> CompactionExecutor<R> for Box<dyn Compactor<R>> {
    fn check_then_compaction<'a>(
        &'a self,
        batches: Option<
            &'a [(
                Option<FileId>,
                ImmutableMemTable<<R::Schema as Schema>::Columns>,
            )],
        >,
        recover_wal_ids: Option<Vec<FileId>>,
        is_manual: bool,
    ) -> impl Future<Output = Result<(), CompactionError<R>>> + MaybeSend + 'a {
        self.as_ref()
            .check_then_compaction(batches, recover_wal_ids, is_manual)
    }
}

/// Wrapper of [`DbStorage`] for handling concurrent operations
pub struct DB<R, E>
where
    R: Record,
    <R::Schema as Schema>::Columns: Send + Sync,
    E: Executor,
{
    mem_storage: Arc<E::RwLock<DbStorage<R>>>,
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
    pub async fn new(option: DbOption, executor: E, schema: R::Schema) -> Result<Self, DbError> {
        Self::build(
            Arc::new(option),
            executor,
            schema,
            Arc::new(NoCache::default()),
        )
        .await
    }

    /// Open [`DB`] with a custom compactor factory. This provides completely static dispatch.
    pub async fn new_with_compactor_factory<C, F>(
        option: DbOption,
        executor: E,
        schema: R::Schema,
        factory: F,
    ) -> Result<Self, DbError>
    where
        C: CompactionExecutor<R> + Send + Sync + 'static,
        F: FnOnce(Arc<DbOption>, Arc<R::Schema>, Arc<Context<R>>) -> C,
    {
        Self::build_with_compactor_factory(
            Arc::new(option),
            executor,
            schema,
            Arc::new(NoCache::default()),
            factory,
        )
        .await
    }

    async fn build(
        option: Arc<DbOption>,
        executor: E,
        schema: R::Schema,
        lru_cache: ParquetLru,
    ) -> Result<Self, DbError> {
        let (record_schema, _manager, cleaner, task_rx, mem_storage, ctx) =
            Self::build_common_setup::<E>(option.clone(), schema, lru_cache).await?;

        match &option.compaction_option {
            CompactionOption::Leveled(opt) => {
                let compactor = LeveledCompactor::<R>::new(
                    opt.clone(),
                    record_schema.clone(),
                    option.clone(),
                    ctx.clone(),
                );
                Self::finish_build(executor, mem_storage, ctx, compactor, cleaner, task_rx).await
            }
        }
    }

    async fn build_with_compactor_factory<C, F>(
        option: Arc<DbOption>,
        executor: E,
        schema: R::Schema,
        lru_cache: ParquetLru,
        factory: F,
    ) -> Result<Self, DbError>
    where
        C: CompactionExecutor<R> + Send + Sync + 'static,
        F: FnOnce(Arc<DbOption>, Arc<R::Schema>, Arc<Context<R>>) -> C,
    {
        let (record_schema, _, cleaner, task_rx, mem_storage, ctx) =
            Self::build_common_setup::<E>(option.clone(), schema, lru_cache).await?;

        let compactor = factory(option, record_schema, ctx.clone());
        Self::finish_build(executor, mem_storage, ctx, compactor, cleaner, task_rx).await
    }

    async fn build_common_setup<Ex>(
        option: Arc<DbOption>,
        schema: R::Schema,
        lru_cache: ParquetLru,
    ) -> Result<
        (
            Arc<R::Schema>,
            Arc<StoreManager>,
            Cleaner,
            flume::Receiver<CompactTask>,
            Arc<Ex::RwLock<DbStorage<R>>>,
            Arc<Context<R>>,
        ),
        DbError,
    >
    where
        Ex: Executor + Send + Sync,
    {
        let record_schema = Arc::new(schema);
        let manager = Arc::new(StoreManager::new(
            option.base_fs.clone(),
            option.level_paths.clone(),
        )?);
        {
            // Ensure both the WAL and version-log paths exist on the local file system
            // and base (default) file system
            manager
                .local_fs()
                .create_dir_all(&option.wal_dir_path())
                .await
                .map_err(DbError::Fusio)?;
            manager
                .local_fs()
                .create_dir_all(&option.version_log_dir_path())
                .await
                .map_err(DbError::Fusio)?;
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
        let (cleaner, clean_sender) = Cleaner::new(option.clone(), manager.clone());

        let manifest = Box::new(
            VersionSet::<R, Ex>::new(clean_sender, option.clone(), manager.clone())
                .await
                .map_err(ManifestStorageError::Version)?,
        );
        let mem_storage = Arc::new(Ex::rw_lock(
            DbStorage::new(
                option.clone(),
                task_tx,
                manifest.as_ref(),
                record_schema.clone(),
                &manager,
            )
            .await?,
        ));

        let ctx = Arc::new(Context::new(
            manager.clone(),
            lru_cache.clone(),
            manifest,
            record_schema.arrow_schema().clone(),
        ));

        Ok((record_schema, manager, cleaner, task_rx, mem_storage, ctx))
    }

    async fn finish_build<C>(
        executor: E,
        mem_storage: Arc<E::RwLock<DbStorage<R>>>,
        ctx: Arc<Context<R>>,
        compactor: C,
        mut cleaner: Cleaner,
        task_rx: flume::Receiver<CompactTask>,
    ) -> Result<Self, DbError>
    where
        C: CompactionExecutor<R> + MaybeSend + MaybeSync + 'static,
        E: Executor + Send + Sync + 'static,
    {
        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });

        let mem_storage_task = mem_storage.clone();
        let ctx_task = ctx.clone();
        executor.spawn(async move {
            // Waits to receive compaction task. `CompactTask::Freeze` will perform automatic
            // compaction and `Compact::Flush` will perform manual compaction
            while let Ok(task) = task_rx.recv_async().await {
                if let Err(err) = match task {
                    CompactTask::Freeze => {
                        // Handle minor flush; drain owned immutables under short lock
                        let mut guard = mem_storage_task.write().await;

                        let immutable_chunk_num = guard.option.immutable_chunk_num;
                        let immutable_chunk_max_num = guard.option.immutable_chunk_max_num;
                        let base_fs = ctx_task.manager.base_fs().clone();

                        let batches_and_wal_ids = minor_flush(
                            &mut *guard,
                            base_fs,
                            immutable_chunk_num,
                            immutable_chunk_max_num,
                            false,
                        )
                        .await;

                        match batches_and_wal_ids {
                            Ok(Some((mut batches, recover_wal_ids))) => {
                                // Mark compaction window before releasing lock
                                guard.compaction_in_progress.store(true, Ordering::Release);
                                // Release lock before heavy work
                                drop(guard);
                                // Keep a copy for potential rollback
                                let rollback_wal_ids = recover_wal_ids.clone();

                                let compaction_result = compactor
                                    .check_then_compaction(
                                        Some(&batches[..]),
                                        recover_wal_ids,
                                        false,
                                    )
                                    .await;

                                // Finalize: clear window and possibly rollback
                                let mut g = mem_storage_task.write().await;
                                if compaction_result.is_err() {
                                    for item in batches.drain(..).rev() {
                                        g.immutables.insert(0, item);
                                    }
                                    if let Some(ids) = rollback_wal_ids {
                                        if let Some(existing) = &mut g.recover_wal_ids {
                                            existing.extend(ids);
                                        } else {
                                            g.recover_wal_ids = Some(ids);
                                        }
                                    }
                                }
                                g.compaction_in_progress.store(false, Ordering::Release);
                                drop(g);
                                compaction_result
                            }
                            Ok(None) => compactor.check_then_compaction(None, None, false).await,
                            Err(e) => {
                                error!("[Minor Flush Error]: {}", e);
                                Ok(())
                            }
                        }
                    }
                    CompactTask::Flush(option_tx) => {
                        // Handle manual flush; drain owned immutables under short lock
                        let mut guard = mem_storage_task.write().await;

                        let immutable_chunk_num = guard.option.immutable_chunk_num;
                        let immutable_chunk_max_num = guard.option.immutable_chunk_max_num;
                        let base_fs = ctx_task.manager.base_fs().clone();

                        let batches_and_wal_ids = minor_flush(
                            &mut *guard,
                            base_fs,
                            immutable_chunk_num,
                            immutable_chunk_max_num,
                            true,
                        )
                        .await;

                        let res = match batches_and_wal_ids {
                            Ok(Some((mut batches, recover_wal_ids))) => {
                                // Mark compaction window before releasing lock
                                guard.compaction_in_progress.store(true, Ordering::Release);
                                // Release lock before heavy work
                                drop(guard);
                                let rollback_wal_ids = recover_wal_ids.clone();
                                let compaction_result = compactor
                                    .check_then_compaction(
                                        Some(&batches[..]),
                                        recover_wal_ids,
                                        true,
                                    )
                                    .await;
                                let mut g = mem_storage_task.write().await;
                                if compaction_result.is_err() {
                                    for item in batches.drain(..).rev() {
                                        g.immutables.insert(0, item);
                                    }
                                    if let Some(ids) = rollback_wal_ids {
                                        if let Some(existing) = &mut g.recover_wal_ids {
                                            existing.extend(ids);
                                        } else {
                                            g.recover_wal_ids = Some(ids);
                                        }
                                    }
                                }
                                g.compaction_in_progress.store(false, Ordering::Release);
                                drop(g);
                                compaction_result
                            }
                            Ok(None) => compactor.check_then_compaction(None, None, true).await,
                            Err(e) => {
                                error!("[Minor Flush Error]: {}", e);
                                Ok(())
                            }
                        };

                        if let Some(tx) = option_tx {
                            // Always notify the caller to avoid hanging flush() even on error
                            let _ = tx.send(());
                        }
                        res
                    }
                } {
                    error!("[Compaction Error]: {}", err);
                }
            }
        });

        Ok(Self {
            mem_storage,
            lock_map: Arc::new(Default::default()),
            ctx,
            _p: Default::default(),
        })
    }

    /// Returns the current manifest version
    pub async fn current_manifest(&self) -> VersionRef<R> {
        self.ctx.current_manifest().await
    }

    /// Open an optimistic ACID transaction
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
    pub async fn transaction(&self) -> Transaction<'_, R, E> {
        // Wait out any compaction window where immutables were drained
        loop {
            let guard = self.mem_storage.read().await;
            if guard.compaction_in_progress.load(Ordering::Acquire) {
                // Drop guard and retry after compaction finalizes
                drop(guard);
                continue;
            }
            let version_ref = self.ctx.manifest().current().await;
            let snapshot: Snapshot<'_, R, E> = Snapshot::new(guard, version_ref, self.ctx.clone());
            break Transaction::new(snapshot, self.lock_map.clone());
        }
    }

    /// Returns a snapshot of the database
    pub async fn snapshot(&self) -> Snapshot<'_, R, E> {
        // Avoid building a snapshot while immutables are drained for compaction
        loop {
            let guard = ExecutorRwLock::read(&*self.mem_storage).await;
            if guard.compaction_in_progress.load(Ordering::Acquire) {
                drop(guard);
                continue;
            }
            break Snapshot::new(guard, self.ctx.manifest().current().await, self.ctx.clone());
        }
    }

    /// Insert a single tonbo record
    pub async fn insert(&self, record: R) -> Result<(), CommitError<R>> {
        Ok(self.write(record, self.ctx.increase_ts()).await?)
    }

    /// Insert a sequence of data as a single batch
    pub async fn insert_batch(
        &self,
        records: impl ExactSizeIterator<Item = R>,
    ) -> Result<(), CommitError<R>> {
        Ok(self.write_batch(records, self.ctx.increase_ts()).await?)
    }

    /// Delete the record with the primary key as the `key`
    pub async fn remove(
        &self,
        key: <R::Schema as Schema>::Key,
    ) -> Result<WriteResult, CommitError<R>> {
        Ok(self
            .mem_storage
            .read()
            .await
            .remove(LogType::Full, key, self.ctx.increase_ts())
            .await?)
    }

    /// Trigger compaction manually. This will flush the WAL and trigger compaction
    pub async fn flush(&self) -> Result<(), CommitError<R>> {
        let (tx, rx) = oneshot::channel();
        let compaction_tx = { self.mem_storage.read().await.compaction_tx.clone() };
        compaction_tx
            .send_async(CompactTask::Flush(Some(tx)))
            .await?;

        rx.await.map_err(|_| CommitError::ChannelClose)?;

        Ok(())
    }

    /// Get the record with `key` as the primary key and process it using closure `f`
    pub async fn get<T>(
        &self,
        key: &<R::Schema as Schema>::Key,
        mut f: impl FnMut(TransactionEntry<'_, R>) -> Option<T>,
    ) -> Result<Option<T>, CommitError<R>> {
        loop {
            let guard = self.mem_storage.read().await;
            if guard.compaction_in_progress.load(Ordering::Acquire) {
                drop(guard);
                continue;
            }
            break Ok(guard
                .get(
                    &self.ctx,
                    &*self.ctx.manifest().current().await,
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
                }));
        }
    }

    /// Scan records with primary keys in the `range` and process them using closure `f`
    pub async fn scan<'scan, T: 'scan>(
        &'scan self,
        range: (
            Bound<&'scan <R::Schema as Schema>::Key>,
            Bound<&'scan <R::Schema as Schema>::Key>,
        ),
        mut f: impl FnMut(TransactionEntry<'_, R>) -> T + 'scan,
    ) -> impl Stream<Item = Result<T, CommitError<R>>> + 'scan {
        stream! {
            // Delay stream construction while compaction window is active
            let schema = loop {
                let guard = self.mem_storage.read().await;
                if guard.compaction_in_progress.load(Ordering::Acquire) {
                    drop(guard);
                    continue;
                }
                break guard;
            };
            let current = self.ctx.manifest().current().await;
            let mut scan = Scan::new(
                &schema,
                range,
                self.ctx.load_ts(),
                &*current,
                Box::new(|_, _| None),
                self.ctx.clone(),
            ).take().await?;

            while let Some(record) = scan.next().await {
                yield Ok(f(TransactionEntry::Stream(record?)))
            }
        }
    }

    pub(crate) async fn write(&self, record: R, ts: Timestamp) -> Result<(), DbError> {
        let mem_storage = self.mem_storage.read().await;

        let write_result = mem_storage.write(LogType::Full, record, ts).await?;
        if write_result.needs_compaction() {
            let _ = mem_storage.compaction_tx.try_send(CompactTask::Freeze);
        };
        Ok(())
    }

    // Write a batch of records
    pub(crate) async fn write_batch(
        &self,
        mut records: impl ExactSizeIterator<Item = R>,
        ts: Timestamp,
    ) -> Result<(), DbError> {
        let mem_storage = self.mem_storage.read().await;

        if let Some(first) = records.next() {
            let is_excess = if let Some(record) = records.next() {
                mem_storage.write(LogType::First, first, ts).await?;

                let mut last_buf = record;

                for record in records {
                    mem_storage
                        .write(LogType::Middle, mem::replace(&mut last_buf, record), ts)
                        .await?;
                }
                mem_storage.write(LogType::Last, last_buf, ts).await?
            } else {
                mem_storage.write(LogType::Full, first, ts).await?
            };
            if is_excess.needs_compaction() {
                let _ = mem_storage.compaction_tx.try_send(CompactTask::Freeze);
            };
        };

        Ok(())
    }

    /// Flush WAL to the stable storage. If WAL is disabled, this method will do nothing.
    ///
    /// There is no guarantee that the data will be flushed to WAL because of the buffer. So it is
    /// necessary to call this method before exiting if data loss is not acceptable. See also
    /// [`DbOption::disable_wal`] and [`DbOption::wal_buffer_size`].
    pub async fn flush_wal(&self) -> Result<(), DbError> {
        self.mem_storage.write().await.flush_wal().await?;
        Ok(())
    }

    /// Destroy [`DB`].
    ///
    /// **Note:** This will remove all wal and manifest file in the directory.
    pub async fn destroy(self) -> Result<(), DbError> {
        self.mem_storage
            .write()
            .await
            .destroy(&self.ctx.manager)
            .await?;
        if let Some(mut ctx) = Arc::into_inner(self.ctx) {
            ctx.manifest.destroy().await?;
        }

        Ok(())
    }
}

/// DbStorage state for coordinating in-memory + on-disk operations
pub struct DbStorage<R>
where
    R: Record,
{
    #[allow(private_interfaces)]
    pub mutable: MutableMemTable<R>,
    pub immutables: Vec<(
        Option<FileId>,
        ImmutableMemTable<<R::Schema as Schema>::Columns>,
    )>,
    compaction_tx: Sender<CompactTask>,
    recover_wal_ids: Option<Vec<FileId>>,
    trigger: Arc<dyn FreezeTrigger<R>>,
    record_schema: Arc<R::Schema>,
    option: Arc<DbOption>,
    // Indicates a compaction window where immutables are drained and not yet visible in manifest
    compaction_in_progress: AtomicBool,
}

impl<R> DbStorage<R>
where
    R: Record + Send,
{
    /// Creates a new instane of 'DbStorage'. If there are write ahead logs in the directory this
    /// function will reconstruct the record batches and recovery the version before crash.
    async fn new(
        option: Arc<DbOption>,
        compaction_tx: Sender<CompactTask>,
        manifest: &dyn ManifestStorage<R>,
        record_schema: Arc<R::Schema>,
        manager: &StoreManager,
    ) -> Result<Self, DbError> {
        let base_fs = manager.base_fs();
        let wal_dir_path = option.wal_dir_path();
        let mut transaction_map = HashMap::new();

        // Collect all write ahead logs in the WAL directory
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
        let mut mem_storage = DbStorage {
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
            compaction_in_progress: AtomicBool::new(false),
        };

        let mut wal_ids = Vec::new();

        // Iterates through all the write ahead logs and reconstructs each record for every batch.
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
                            mem_storage
                                .recover_append(key, manifest.increase_ts(), value)
                                .await?
                        }
                        LogType::First => {
                            transaction_map.insert(ts, vec![(key, value)]);
                            WriteResult::Continue
                        }
                        LogType::Middle => {
                            transaction_map.get_mut(&ts).unwrap().push((key, value));
                            WriteResult::Continue
                        }
                        LogType::Last => {
                            let mut is_excess = WriteResult::Continue;
                            let mut records = transaction_map.remove(&ts).unwrap();
                            records.push((key, value));

                            // Increase timestamp for each multipart record.
                            let ts = manifest.increase_ts();
                            for (key, value_option) in records {
                                is_excess =
                                    mem_storage.recover_append(key, ts, value_option).await?;
                            }
                            is_excess
                        }
                    };

                    // Compact during recovery if exceeded memory threshold
                    if is_excess.needs_compaction() {
                        let _ = mem_storage.compaction_tx.try_send(CompactTask::Freeze);
                    };
                }
            }
        }
        mem_storage.recover_wal_ids = Some(wal_ids);

        Ok(mem_storage)
    }

    // Write individual record to mutable memtable
    async fn write(
        &self,
        log_ty: LogType,
        record: R,
        ts: Timestamp,
    ) -> Result<WriteResult, DbError> {
        self.mutable.insert(log_ty, record, ts).await
    }

    // Remove individual record from mutable memtable
    async fn remove(
        &self,
        log_ty: LogType,
        key: <R::Schema as Schema>::Key,
        ts: Timestamp,
    ) -> Result<WriteResult, DbError> {
        self.mutable.remove(log_ty, key, ts).await
    }

    // Make a recovery append
    async fn recover_append(
        &self,
        key: <R::Schema as Schema>::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<WriteResult, DbError> {
        // Passes in None as we do not need it to be durably logged
        self.mutable.append(None, key, ts, value).await
    }

    // Retrieve record using primary key
    async fn get<'get>(
        &'get self,
        ctx: &Context<R>,
        version: &'get Version<R>,
        key: &'get <R::Schema as Schema>::Key,
        ts: Timestamp,
        projection: Projection<'get>,
    ) -> Result<Option<Entry<'get, R>>, DbError> {
        let pk_indices = self.record_schema.primary_key_indices();
        let schema = ctx.arrow_schema();

        let projection = match projection {
            Projection::All => ProjectionMask::all(),
            Projection::Parts(projection) => {
                let mut fixed_projection: Vec<usize> =
                    Vec::with_capacity(2 + pk_indices.len() + projection.len());
                fixed_projection.push(0);
                fixed_projection.push(1);
                fixed_projection.extend_from_slice(pk_indices);
                fixed_projection.extend(projection.into_iter().map(|name| {
                    schema
                        .index_of(name)
                        .unwrap_or_else(|_| panic!("unexpected field {name}"))
                }));
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

        // Returns a table query with a projection
        Ok(version
            .query(
                ctx.storage_manager(),
                TsRef::new(key, ts),
                projection,
                ctx.cache().clone(),
                self.record_schema.primary_key_indices(),
            )
            .await?
            .map(|entry| Entry::RecordBatch(entry)))
    }

    // Performs a concurrency check to make sure a write hasn't already happend before the current
    // one
    fn check_conflict(&self, key: &<R::Schema as Schema>::Key, ts: Timestamp) -> bool {
        self.mutable.check_conflict(key, ts)
            || self
                .immutables
                .iter()
                .rev()
                .any(|(_, immutable)| immutable.check_conflict(key, ts))
    }

    // Flush the write ahead log to disk
    async fn flush_wal(&self) -> Result<(), DbError> {
        self.mutable.flush_wal().await?;
        Ok(())
    }

    // Remove all WALs from the filesystem
    async fn destroy(&mut self, manager: &StoreManager) -> Result<(), DbError> {
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

/// Scan configuration intermediate structure. Calling `take` will execute a scan on
/// the memtable, immutable memtables, and SSTables on disk in that order.
///
/// To initialize you can call `DB.transaction().scan()` using your `DB` instance.
/// For more information on how to use refer to the doc in [`DB::transaction()`]
pub struct Scan<'scan, 'range, R>
where
    R: Record,
    'range: 'scan,
{
    // Storage to scan
    mem_storage: &'scan DbStorage<R>,
    // Lower and upper bound for the scan
    lower: Bound<&'range <R::Schema as Schema>::Key>,
    upper: Bound<&'range <R::Schema as Schema>::Key>,
    // Current `Snapshot`'s timestamp
    ts: Timestamp,
    // DB Version that is being scanned
    version: &'scan Version<R>,
    fn_pre_stream: Box<
        dyn FnOnce(Option<ProjectionMask>, Option<Order>) -> Option<ScanStream<'scan, R>>
            + Send
            + 'scan,
    >,
    // Row limit for query
    limit: Option<usize>,
    // Specifies the direction a scan will take
    order: Option<Order>,
    projection_indices: Option<Vec<usize>>,
    projection: ProjectionMask,
    ctx: Arc<Context<R>>,
}

impl<'scan, 'range, R> Scan<'scan, 'range, R>
where
    R: Record + Send,
{
    fn new(
        mem_storage: &'scan DbStorage<R>,
        (lower, upper): (
            Bound<&'range <R::Schema as Schema>::Key>,
            Bound<&'range <R::Schema as Schema>::Key>,
        ),
        ts: Timestamp,
        version: &'scan Version<R>,
        fn_pre_stream: Box<
            dyn FnOnce(Option<ProjectionMask>, Option<Order>) -> Option<ScanStream<'scan, R>>
                + Send
                + 'scan,
        >,
        ctx: Arc<Context<R>>,
    ) -> Self {
        Self {
            mem_storage,
            lower,
            upper,
            ts,
            version,
            fn_pre_stream,
            limit: None,
            order: None,
            projection_indices: None,
            projection: ProjectionMask::all(),
            ctx,
        }
    }

    /// Limit for the scan (number of rows returned)
    pub fn limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// Configures the scan to return results in descending order (reverse order).
    ///
    /// By default, scans return results in ascending order. Use this method to scan
    /// from highest to lowest key values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Scan users in reverse order (newest first)
    /// let upper = "Blob".into();
    /// let mut scan = txn
    ///     .scan((Bound::Included(&name), Bound::Excluded(&upper)))
    ///     .reverse() // scan in descending order
    ///     .limit(10) // get last 10 records
    ///     .take()
    ///     .await
    ///     .unwrap();
    ///
    /// while let Some(entry) = scan.next().await.transpose().unwrap() {
    ///     println!("User in reverse order: {:?}", entry.value());
    /// }
    /// ```
    pub fn reverse(self) -> Self {
        Self {
            order: Some(Order::Desc),
            ..self
        }
    }

    /// fields in projection Record by field indices
    pub fn projection(self, projection: &[&str]) -> Self {
        let schema = self.mem_storage.record_schema.arrow_schema();
        let mut projection = projection
            .iter()
            .map(|name| {
                schema.index_of(name).unwrap_or_else(|_| {
                    panic!("Field in projection does not exist in schema: {name}")
                })
            })
            .collect::<Vec<usize>>();

        let pk_indices = self.mem_storage.record_schema.primary_key_indices();

        // The scan uses a fixed projection of 0: `_null` field, 1: `_ts` field, and all primary key
        // columns
        let mut fixed_projection = vec![0, 1];
        fixed_projection.extend_from_slice(pk_indices);
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

    /// Fields in projection Record by field indices
    pub fn projection_with_index(self, mut projection: Vec<usize>) -> Self {
        // skip two columns: _null and _ts
        for p in &mut projection {
            *p += USER_COLUMN_OFFSET;
        }

        let pk_indices = self.mem_storage.record_schema.primary_key_indices();

        // The scan uses a fixed projection of 0: `_null` field, 1: `_ts` field, and all primary key
        // columns
        let mut fixed_projection = vec![0, 1];
        fixed_projection.extend_from_slice(pk_indices);
        fixed_projection.append(&mut projection);
        fixed_projection.dedup();

        let mask = ProjectionMask::roots(
            &ArrowSchemaConverter::new()
                .convert(self.mem_storage.record_schema.arrow_schema())
                .unwrap(),
            fixed_projection.clone(),
        );

        Self {
            projection: mask,
            projection_indices: Some(fixed_projection),
            ..self
        }
    }

    /// Get a Stream that returns single row of Record
    pub async fn take(
        self,
    ) -> Result<impl Stream<Item = Result<Entry<'scan, R>, ParquetError>>, DbError> {
        let mut streams = Vec::new();
        let is_projection = self.projection_indices.is_some();

        if let Some(pre_stream) =
            (self.fn_pre_stream)(is_projection.then(|| self.projection.clone()), self.order)
        {
            streams.push(pre_stream);
        }

        // In-memory memtable scan
        {
            let mut mutable_scan = self
                .mem_storage
                .mutable
                .scan((self.lower, self.upper), self.ts, self.order)
                .into();
            if is_projection {
                mutable_scan =
                    MemProjectionStream::new(mutable_scan, self.projection.clone()).into();
            }
            streams.push(mutable_scan);
        }

        // Iterates through immutable memtables
        for (_, immutable) in self.mem_storage.immutables.iter().rev() {
            streams.push(
                immutable
                    .scan(
                        (self.lower, self.upper),
                        self.ts,
                        self.projection.clone(),
                        self.order,
                    )
                    .into(),
            );
        }

        // Scans all SSTables in the coreresponding version
        self.version
            .streams(
                &self.ctx,
                &mut streams,
                (self.lower, self.upper),
                self.ts,
                self.limit,
                self.projection,
                self.order,
                self.mem_storage.record_schema.primary_key_indices(),
            )
            .await?;

        let mut merge_stream = MergeStream::from_vec(streams, self.ts, self.order).await?;
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
        DbError,
    > {
        let mut streams = Vec::new();
        let is_projection = self.projection_indices.is_some();

        if let Some(pre_stream) =
            (self.fn_pre_stream)(is_projection.then(|| self.projection.clone()), self.order)
        {
            streams.push(pre_stream);
        }

        {
            let mut mutable_scan = self
                .mem_storage
                .mutable
                .scan((self.lower, self.upper), self.ts, self.order)
                .into();
            if is_projection {
                mutable_scan =
                    MemProjectionStream::new(mutable_scan, self.projection.clone()).into();
            }
            streams.push(mutable_scan);
        }

        for (_, immutable) in self.mem_storage.immutables.iter().rev() {
            streams.push(
                immutable
                    .scan(
                        (self.lower, self.upper),
                        self.ts,
                        self.projection.clone(),
                        self.order,
                    )
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
                self.order,
                self.mem_storage.record_schema.primary_key_indices(),
            )
            .await?;
        let merge_stream = MergeStream::from_vec(streams, self.ts, self.order).await?;

        Ok(PackageStream::new(
            batch_size,
            merge_stream,
            self.projection_indices,
            self.ctx.arrow_schema().clone(),
        ))
    }
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("write io error: {0}")]
    Io(#[from] io::Error),
    #[error("write version error: {0}")]
    Version(#[from] VersionError),
    #[error("write manifest storage error: {0}")]
    Manifest(#[from] ManifestStorageError),
    #[error("write parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("write ulid decode error: {0}")]
    UlidDecode(#[from] ulid::DecodeError),
    #[error("write fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    // #[error("write encode error: {0}")]
    // Encode(<<R as Record>::Ref as Encode>::Error),
    #[error("write recover error: {0}")]
    Recover(#[from] RecoverError<fusio::Error>),
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
        sync::Arc,
    };

    use flume::{bounded, Receiver};
    use fusio::{disk::TokioFs, path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use futures::StreamExt;
    use parquet_lru::NoCache;
    use tempfile::TempDir;

    pub use crate::record::test::{Test, TestRef};
    use crate::{
        compaction::{
            leveled::{LeveledCompactor, LeveledOptions},
            CompactTask,
        },
        context::Context,
        executor::{tokio::TokioExecutor, Executor},
        fs::{generate_file_id, manager::StoreManager},
        inmem::{immutable::tests::TestSchema, mutable::MutableMemTable},
        manifest::ManifestStorageError,
        record::{
            dynamic::test::{test_dyn_item_schema, test_dyn_items},
            DynRecord, KeyRef, Schema as RecordSchema, Value, ValueRef,
        },
        trigger::{TriggerFactory, TriggerType},
        version::{cleaner::Cleaner, set::tests::build_version_set, Version},
        wal::log::LogType,
        CompactionOption, DbError, DbOption, Projection, Record, DB,
    };

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
                compaction_in_progress: std::sync::atomic::AtomicBool::new(false),
            },
            compaction_rx,
        ))
    }

    use crate::record::test::test_items;

    pub(crate) async fn build_db<R, E>(
        option: Arc<DbOption>,
        compaction_rx: Receiver<CompactTask>,
        executor: E,
        mem_storage: crate::DbStorage<R>,
        record_schema: Arc<R::Schema>,
        version: Version<R>,
        manager: Arc<StoreManager>,
    ) -> Result<DB<R, E>, DbError>
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

        let mem_storage = Arc::new(E::rw_lock(mem_storage));

        let (cleaner, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let manifest = Box::new(
            build_version_set::<R, E>(version, clean_sender, option.clone(), manager.clone())
                .await
                .map_err(ManifestStorageError::Version)?,
        );
        let ctx = Arc::new(Context::new(
            manager,
            Arc::new(NoCache::default()),
            manifest,
            TestSchema.arrow_schema().clone(),
        ));
        // Create built-in compactor for tests
        match &option.compaction_option {
            CompactionOption::Leveled(opt) => {
                let compactor = LeveledCompactor::<R>::new(
                    opt.clone(),
                    record_schema.clone(),
                    option.clone(),
                    ctx.clone(),
                );
                DB::finish_build(
                    executor,
                    mem_storage,
                    ctx,
                    compactor,
                    cleaner,
                    compaction_rx,
                )
                .await
            }
        }
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
        option = option
            .immutable_chunk_num(1)
            .immutable_chunk_max_num(1)
            .leveled_compaction(
                LeveledOptions::default()
                    .major_threshold_with_sst_size(3)
                    .level_sst_magnification(10)
                    .major_default_oldest_table_num(1),
            )
            .max_sst_file_size(2 * 1024 * 1024);
        option.trigger_type = TriggerType::Length(/* max_mutable_len */ 5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
            .await
            .unwrap();

        for (i, item) in test_items(0u32..32).enumerate() {
            db.write(item, 0.into()).await.unwrap();
            if i % 5 == 0 {
                db.flush().await.unwrap();
            }
        }

        let tx = db.transaction().await;
        let key = 20.to_string();
        let option1 = tx.get(&key, Projection::All).await.unwrap().unwrap();

        dbg!(db.ctx.manifest.current().await);

        let version = db.ctx.manifest.current().await;
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
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(1)
        .leveled_compaction(
            LeveledOptions::default()
                .major_threshold_with_sst_size(3)
                .level_sst_magnification(10)
                .major_default_oldest_table_num(1),
        )
        .max_sst_file_size(2 * 1024 * 1024);
        option.trigger_type = TriggerType::Length(/* max_mutable_len */ 50);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
            .await
            .unwrap();

        for item in test_items(0u32..10) {
            db.write(item.clone(), 0.into()).await.unwrap();
        }
        db.flush().await.unwrap();
        for item in test_items(10u32..20) {
            db.write(item.clone(), 0.into()).await.unwrap();
        }

        dbg!(db.ctx.manifest.current().await);
        db.flush().await.unwrap();
        dbg!(db.ctx.manifest.current().await);

        let version = db.ctx.manifest.current().await;
        assert!(!version.level_slice[0].is_empty());
    }

    #[ignore = "s3"]
    #[cfg(all(feature = "aws", feature = "tokio-http"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_s3_recover() {
        let temp_dir = TempDir::new().unwrap();

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
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema {},
        )
        .base_fs(s3_option.clone());
        {
            let db = DB::new(option.clone(), TokioExecutor::default(), TestSchema)
                .await
                .unwrap();

            for item in test_items(0u32..10) {
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
                DB::new(option.clone(), TokioExecutor::default(), TestSchema)
                    .await
                    .unwrap();
            let mut sort_items = BTreeMap::new();
            for item in test_items(0u32..10) {
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
        let mem_storage: crate::DbStorage<Test> = crate::DbStorage {
            mutable: MutableMemTable::new(&option, trigger.clone(), fs, Arc::new(TestSchema))
                .await
                .unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx.clone(),
            recover_wal_ids: None,
            trigger,
            record_schema: Arc::new(TestSchema),
            option: option.clone(),
            compaction_in_progress: std::sync::atomic::AtomicBool::new(false),
        };

        for (i, item) in test_items(0u32..32).enumerate() {
            mem_storage
                .write(LogType::Full, item, (i as u32).into())
                .await
                .unwrap();
        }
        mem_storage.flush_wal().await.unwrap();
        drop(mem_storage);

        let db: DB<Test, TokioExecutor> = DB::new(
            option.as_ref().to_owned(),
            TokioExecutor::default(),
            TestSchema,
        )
        .await
        .unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_items(0u32..32) {
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
        let mem_storage: crate::DbStorage<DynRecord> = crate::DbStorage {
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
            compaction_in_progress: std::sync::atomic::AtomicBool::new(false),
        };

        for item in test_dyn_items().into_iter() {
            mem_storage
                .write(LogType::Full, item, 0_u32.into())
                .await
                .unwrap();
        }
        mem_storage.flush_wal().await.unwrap();
        drop(mem_storage);

        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            dyn_schema.as_ref(),
        );
        let dyn_schema = test_dyn_item_schema();
        let db: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::default(), dyn_schema)
                .await
                .unwrap();

        let mut sort_items = BTreeMap::new();
        for item in test_dyn_items() {
            sort_items.insert(item.key().to_key(), item);
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
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(1)
        .leveled_compaction(
            LeveledOptions::default()
                .major_threshold_with_sst_size(3)
                .major_default_oldest_table_num(1),
        );
        option.trigger_type = TriggerType::Length(5);
        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
            .await
            .unwrap();

        for (idx, item) in test_items(0u32..32).enumerate() {
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
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(1)
        .leveled_compaction(
            LeveledOptions::default()
                .major_threshold_with_sst_size(3)
                .level_sst_magnification(10)
                .major_default_oldest_table_num(1),
        )
        .max_sst_file_size(2 * 1024 * 1024);
        option.trigger_type = TriggerType::Length(5);

        let db: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::default(), dyn_schema)
                .await
                .unwrap();

        for (i, item) in test_dyn_items().into_iter().enumerate() {
            if i == 28 {
                db.remove(item.key().to_key()).await.unwrap();
            } else {
                db.write(item, 0.into()).await.unwrap();
            }
        }

        // test get
        {
            let tx = db.transaction().await;

            for i in 0..50 {
                let key = Value::Int64(i as i64);
                let option1 = tx.get(&key, Projection::All).await.unwrap();
                if i == 28 {
                    assert!(option1.is_none());
                    continue;
                }
                let entry = option1.unwrap();
                let record_ref = entry.get();

                assert_eq!(
                    record_ref.columns.first().unwrap(),
                    &ValueRef::Int64(i as i64),
                );
                let height = record_ref.columns.get(2).unwrap();
                if i < 45 {
                    assert_eq!(*height, ValueRef::Int16(20 * i as i16),);
                } else {
                    assert_eq!(*height, ValueRef::Null);
                }
                assert_eq!(
                    record_ref.columns.get(3).unwrap(),
                    &ValueRef::Int32(200 * i),
                );
                assert_eq!(
                    record_ref.columns.get(4).unwrap(),
                    &ValueRef::String(i.to_string().as_str()),
                );
                assert_eq!(
                    record_ref.columns.get(5).unwrap(),
                    &ValueRef::String(format!("{}@tonbo.io", i).as_str()),
                );
                assert_eq!(
                    record_ref.columns.get(6).unwrap(),
                    &ValueRef::Boolean(i % 2 == 0),
                );
                assert_eq!(
                    record_ref.columns.get(7).unwrap(),
                    &ValueRef::Binary(i.to_le_bytes().as_slice()),
                );
                assert_eq!(
                    record_ref.columns.get(8).unwrap(),
                    &ValueRef::Float32(i as f32 * 1.11),
                );
                assert_eq!(
                    record_ref.columns.get(9).unwrap(),
                    &ValueRef::Float64(i as f64 * 1.01),
                );
            }
            tx.commit().await.unwrap();
        }
        // test scan
        {
            let tx = db.transaction().await;
            let lower = Value::Int64(0_i64);
            let upper = Value::Int64(49_i64);
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
                assert_eq!(primary_key_col, &ValueRef::Int64(i));

                let height = columns.get(2).unwrap();
                if i < 45 {
                    assert_eq!(height, &ValueRef::Int16(i as i16 * 20));
                } else {
                    assert_eq!(height, &ValueRef::Null);
                }

                let weight = columns.get(3).unwrap();
                assert_eq!(*weight, ValueRef::Null);

                let name = columns.get(4).unwrap();
                assert_eq!(name, &ValueRef::Null);

                let enabled = columns.get(6).unwrap();
                assert_eq!(*enabled, ValueRef::Null);

                let bytes = columns.get(7).unwrap();
                assert_eq!(
                    bytes,
                    &ValueRef::Binary((i as i32).to_le_bytes().as_slice())
                );

                let col = columns.get(8).unwrap();
                assert_eq!(*col, ValueRef::Float32(i as f32 * 1.11));

                let col = columns.get(9).unwrap();
                assert_eq!(*col, ValueRef::Float64(i as f64 * 1.01));
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
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(1)
        .leveled_compaction(
            LeveledOptions::default()
                .major_threshold_with_sst_size(3)
                .major_default_oldest_table_num(1),
        );
        option.trigger_type = TriggerType::Length(5);

        let temp_dir2 = TempDir::with_prefix("db2").unwrap();
        let mut option2 = DbOption::new(
            Path::from_filesystem_path(temp_dir2.path()).unwrap(),
            &dyn_schema,
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(1)
        .leveled_compaction(
            LeveledOptions::default()
                .major_threshold_with_sst_size(3)
                .major_default_oldest_table_num(1),
        );
        option2.trigger_type = TriggerType::Length(5);

        let temp_dir3 = TempDir::with_prefix("db3").unwrap();
        let mut option3 = DbOption::new(
            Path::from_filesystem_path(temp_dir3.path()).unwrap(),
            &dyn_schema,
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(1)
        .leveled_compaction(
            LeveledOptions::default()
                .major_threshold_with_sst_size(3)
                .major_default_oldest_table_num(1),
        );
        option3.trigger_type = TriggerType::Length(5);

        let db1: DB<DynRecord, TokioExecutor> =
            DB::new(option, TokioExecutor::default(), test_dyn_item_schema())
                .await
                .unwrap();
        let db2: DB<DynRecord, TokioExecutor> =
            DB::new(option2, TokioExecutor::default(), test_dyn_item_schema())
                .await
                .unwrap();
        let db3: DB<DynRecord, TokioExecutor> =
            DB::new(option3, TokioExecutor::default(), test_dyn_item_schema())
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
                let key = Value::Int64(i as i64);
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
                    record_ref.columns.first().unwrap(),
                    &ValueRef::Int64(i as i64),
                );
                assert_eq!(
                    record_ref.columns.get(3).unwrap(),
                    &ValueRef::Int32(200 * i),
                );
                assert_eq!(
                    record_ref.columns.get(4).unwrap(),
                    &ValueRef::String(i.to_string().as_str()),
                );
            }
            tx1.commit().await.unwrap();
        }
        // test scan
        {
            let tx1 = db1.transaction().await;
            let lower = Value::Int64(8_i64);
            let upper = Value::Int64(43_i64);
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
                assert_eq!(primary_key_col, &ValueRef::Int64(i));

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
                assert_eq!(primary_key_col, &ValueRef::Int64(i));

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
                assert_eq!(primary_key_col, &ValueRef::Int64(i));

                i += 1
            }
        }
    }
}
