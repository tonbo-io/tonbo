mod compaction;
pub mod executor;
pub mod fs;
pub mod inmem;
mod ondisk;
pub mod option;
pub mod record;
mod scope;
pub mod serdes;
mod stream;
pub mod timestamp;
mod transaction;
mod version;
mod wal;

use std::{
    collections::{HashMap, VecDeque},
    io,
    marker::PhantomData,
    mem,
    ops::Bound,
    pin::pin,
    sync::Arc,
};

use async_lock::{RwLock, RwLockReadGuard};
use async_stream::stream;
use flume::{bounded, Sender};
use fs::FileProvider;
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{immutable::Immutable, mutable::Mutable};
use lockable::LockableHashMap;
use parquet::{
    arrow::{arrow_to_parquet_schema, ProjectionMask},
    errors::ParquetError,
};
use record::Record;
use thiserror::Error;
use timestamp::Timestamp;
use tracing::error;
use transaction::{CommitError, Transaction, TransactionEntry};

pub use crate::option::*;
use crate::{
    compaction::{CompactTask, Compactor},
    executor::Executor,
    fs::{FileId, FileType},
    serdes::Decode,
    stream::{merge::MergeStream, package::PackageStream, Entry, ScanStream},
    timestamp::Timestamped,
    version::{cleaner::Cleaner, set::VersionSet, Version, VersionError},
    wal::{log::LogType, RecoverError, WalFile},
};

pub struct DB<R, E>
where
    R: Record,
    E: Executor,
{
    schema: Arc<RwLock<Schema<R, E>>>,
    version_set: VersionSet<R, E>,
    lock_map: LockMap<R::Key>,
    _p: PhantomData<E>,
}

impl<R, E> DB<R, E>
where
    R: Record + Send + Sync,
    R::Columns: Send + Sync,
    E: Executor + Send + Sync + 'static,
{
    pub async fn new(option: DbOption, executor: E) -> Result<Self, DbError<R>> {
        let option = Arc::new(option);
        E::create_dir_all(&option.path).await?;
        E::create_dir_all(&option.wal_dir_path()).await?;

        let (task_tx, task_rx) = bounded(1);

        let (mut cleaner, clean_sender) = Cleaner::<E>::new(option.clone());

        let version_set = VersionSet::new(clean_sender, option.clone()).await?;
        let schema = Arc::new(RwLock::new(
            Schema::new(option.clone(), task_tx, &version_set).await?,
        ));
        let mut compactor =
            Compactor::<R, E>::new(schema.clone(), option.clone(), version_set.clone());

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });
        executor.spawn(async move {
            while let Ok(task) = task_rx.recv_async().await {
                match task {
                    CompactTask::Freeze => {
                        if let Err(err) = compactor.check_then_compaction().await {
                            // todo!();
                            error!("[Compaction Error]: {}", err)
                        }
                    }
                }
            }
        });
        // TODO: Recover

        Ok(Self {
            schema,
            version_set,
            lock_map: Arc::new(Default::default()),
            _p: Default::default(),
        })
    }

    pub async fn transaction(&self) -> Transaction<'_, R, E> {
        Transaction::new(
            self.version_set.current().await,
            self.schema.read().await,
            self.lock_map.clone(),
        )
    }

    pub async fn insert(&self, record: R) -> Result<(), CommitError<R>> {
        Ok(self
            .write(record, self.version_set.transaction_ts())
            .await?)
    }

    pub async fn insert_batch(
        &self,
        records: impl ExactSizeIterator<Item = R>,
    ) -> Result<(), CommitError<R>> {
        Ok(self
            .write_batch(records, self.version_set.transaction_ts())
            .await?)
    }

    pub async fn remove(&self, key: R::Key) -> Result<bool, CommitError<R>> {
        Ok(self
            .schema
            .read()
            .await
            .remove(LogType::Full, key, self.version_set.transaction_ts())
            .await?)
    }

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
                key,
                self.version_set.transaction_ts(),
                Projection::All,
            )
            .await?
            .map(|e| f(TransactionEntry::Stream(e))))
    }

    pub async fn scan<'scan, T: 'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        mut f: impl FnMut(TransactionEntry<'_, R>) -> T + 'scan,
    ) -> impl Stream<Item = Result<T, CommitError<R>>> + 'scan {
        stream! {
            let schema = self.schema.read().await;
            let current = self.version_set.current().await;
            let mut scan = Scan::new(
                &schema,
                range,
                self.version_set.transaction_ts(),
                &*current,
                Vec::new(),
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

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, Schema<R, E>> {
        self.schema.read().await
    }
}

pub(crate) struct Schema<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    mutable: Mutable<R, FP>,
    immutables: VecDeque<(FileId, Immutable<R::Columns>)>,
    compaction_tx: Sender<CompactTask>,
    option: Arc<DbOption>,
    recover_wal_ids: Option<Vec<FileId>>,
}

impl<R, FP> Schema<R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    async fn new(
        option: Arc<DbOption>,
        compaction_tx: Sender<CompactTask>,
        version_set: &VersionSet<R, FP>,
    ) -> Result<Self, DbError<R>> {
        let mut schema = Schema {
            mutable: Mutable::new(&option).await?,
            immutables: Default::default(),
            compaction_tx,
            option: option.clone(),
            recover_wal_ids: None,
        };

        let mut transaction_map = HashMap::new();
        let mut wal_stream = pin!(FP::list(option.wal_dir_path(), FileType::Wal)?);
        let mut wal_ids = Vec::new();

        while let Some(wal) = wal_stream.next().await {
            let (file, wal_id) = wal?;
            let mut wal = WalFile::<FP::File, R>::new(file, wal_id);
            wal_ids.push(wal_id);

            let mut recover_stream = pin!(wal.recover());
            while let Some(record) = recover_stream.next().await {
                let (log_type, Timestamped { ts, value: key }, value_option) = record?;

                let is_excess = match log_type {
                    LogType::Full => {
                        schema
                            .recover_append(key, version_set.transaction_ts(), value_option)
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

                        let ts = version_set.transaction_ts();
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
        Ok(self.mutable.insert(log_ty, record, ts).await? > self.option.max_mem_table_size)
    }

    async fn remove(
        &self,
        log_ty: LogType,
        key: R::Key,
        ts: Timestamp,
    ) -> Result<bool, DbError<R>> {
        Ok(self.mutable.remove(log_ty, key, ts).await? > self.option.max_mem_table_size)
    }

    async fn recover_append(
        &self,
        key: R::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<bool, DbError<R>> {
        Ok(self.mutable.append(None, key, ts, value).await? > self.option.max_mem_table_size)
    }

    async fn get<'get>(
        &'get self,
        version: &'get Version<R, FP>,
        key: &'get R::Key,
        ts: Timestamp,
        projection: Projection,
    ) -> Result<Option<Entry<'get, R>>, DbError<R>>
    where
        FP: FileProvider,
    {
        let mut scan = Scan::new(
            self,
            (Bound::Included(key), Bound::Unbounded),
            ts,
            version,
            vec![],
        );

        if let Projection::Parts(mask) = projection {
            scan = scan.projection(mask);
        }
        Ok(scan.take().await?.next().await.transpose()?)
    }

    fn check_conflict(&self, key: &R::Key, ts: Timestamp) -> bool {
        self.mutable.check_conflict(key, ts)
            || self
                .immutables
                .iter()
                .any(|(_, immutable)| immutable.check_conflict(key, ts))
    }
}

pub struct Scan<'scan, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    schema: &'scan Schema<R, FP>,
    lower: Bound<&'scan R::Key>,
    upper: Bound<&'scan R::Key>,
    ts: Timestamp,

    version: &'scan Version<R, FP>,
    streams: Vec<ScanStream<'scan, R, FP>>,

    limit: Option<usize>,
    projection_indices: Option<Vec<usize>>,
    projection: ProjectionMask,
}

impl<'scan, R, FP> Scan<'scan, R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    fn new(
        schema: &'scan Schema<R, FP>,
        (lower, upper): (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
        version: &'scan Version<R, FP>,
        streams: Vec<ScanStream<'scan, R, FP>>,
    ) -> Self {
        Self {
            schema,
            lower,
            upper,
            ts,
            version,
            streams,
            limit: None,
            projection_indices: None,
            projection: ProjectionMask::all(),
        }
    }

    pub fn limit(self, limit: Option<usize>) -> Self {
        Self { limit, ..self }
    }

    pub fn projection(self, mut projection: Vec<usize>) -> Self {
        // skip two columns: _null and _ts
        for p in &mut projection {
            *p += 2;
        }
        projection.extend([0, 1, R::primary_key_index()]);
        let mask = ProjectionMask::roots(
            &arrow_to_parquet_schema(R::arrow_schema()).unwrap(),
            projection.clone(),
        );

        Self {
            projection: mask,
            projection_indices: Some(projection),
            ..self
        }
    }

    pub async fn take(
        mut self,
    ) -> Result<impl Stream<Item = Result<Entry<'scan, R>, ParquetError>>, DbError<R>> {
        self.streams.push(
            self.schema
                .mutable
                .scan((self.lower, self.upper), self.ts)
                .into(),
        );
        for (_, immutable) in &self.schema.immutables {
            self.streams.push(
                immutable
                    .scan((self.lower, self.upper), self.ts, self.projection.clone())
                    .into(),
            );
        }
        self.version
            .streams(
                &mut self.streams,
                (self.lower, self.upper),
                self.ts,
                self.limit,
                self.projection,
            )
            .await?;

        Ok(MergeStream::from_vec(self.streams).await?)
    }

    pub async fn package(
        mut self,
        batch_size: usize,
    ) -> Result<impl Stream<Item = Result<R::Columns, ParquetError>> + 'scan, DbError<R>> {
        self.streams.push(
            self.schema
                .mutable
                .scan((self.lower, self.upper), self.ts)
                .into(),
        );
        for (_, immutable) in &self.schema.immutables {
            self.streams.push(
                immutable
                    .scan((self.lower, self.upper), self.ts, self.projection.clone())
                    .into(),
            );
        }
        self.version
            .streams(
                &mut self.streams,
                (self.lower, self.upper),
                self.ts,
                self.limit,
                self.projection,
            )
            .await?;
        let merge_stream = MergeStream::from_vec(self.streams).await?;

        Ok(PackageStream::new(
            batch_size,
            merge_stream,
            self.projection_indices,
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
    // #[error("write encode error: {0}")]
    // Encode(<<R as Record>::Ref as Encode>::Error),
    #[error("write recover error: {0}")]
    Recover(#[from] RecoverError<<R as Decode>::Error>),
}

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

pub enum Projection {
    All,
    Parts(Vec<usize>),
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::{BTreeMap, Bound, VecDeque},
        mem,
        sync::Arc,
    };

    use arrow::{
        array::{Array, AsArray, RecordBatch},
        datatypes::{DataType, Field, Schema, UInt32Type},
    };
    use async_lock::RwLock;
    use flume::{bounded, Receiver};
    use futures_util::io;
    use once_cell::sync::Lazy;
    use parquet::arrow::ProjectionMask;
    use tempfile::TempDir;
    use tracing::error;

    use crate::{
        compaction::{CompactTask, Compactor},
        executor::{tokio::TokioExecutor, Executor},
        fs::{FileId, FileProvider},
        inmem::{immutable::tests::TestImmutableArrays, mutable::Mutable},
        record::{internal::InternalRecordRef, RecordDecodeError, RecordEncodeError, RecordRef},
        serdes::{Decode, Encode},
        version::{cleaner::Cleaner, set::tests::build_version_set, Version},
        wal::log::LogType,
        DbError, DbOption, Immutable, Projection, Record, DB,
    };

    #[derive(Debug, PartialEq, Eq)]
    pub struct Test {
        pub vstring: String,
        pub vu32: u32,
        pub vbool: Option<bool>,
    }

    impl Decode for Test {
        type Error = RecordDecodeError;

        async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
        where
            R: futures_io::AsyncRead + Unpin,
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

        type Ref<'r> = TestRef<'r>
        where
            Self: 'r;

        fn key(&self) -> &str {
            &self.vstring
        }

        fn primary_key_index() -> usize {
            2
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
            W: io::AsyncWrite + Unpin + Send,
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

        fn from_record_batch(
            record_batch: &'r RecordBatch,
            offset: usize,
            projection_mask: &'r ProjectionMask,
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
        option: DbOption,
        executor: E,
    ) -> RecordBatch {
        let db: DB<Test, E> = DB::new(option.clone(), executor).await.unwrap();

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

        let mutable = mem::replace(&mut schema.mutable, Mutable::new(&option).await.unwrap());

        Immutable::<<Test as Record>::Columns>::from(mutable.data)
            .as_record_batch()
            .clone()
    }

    pub(crate) async fn build_schema(
        option: Arc<DbOption>,
    ) -> io::Result<(crate::Schema<Test, TokioExecutor>, Receiver<CompactTask>)> {
        let mutable = Mutable::new(&option).await?;

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
            let mutable: Mutable<Test, TokioExecutor> = Mutable::new(&option).await?;

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

            VecDeque::from(vec![(FileId::new(), Immutable::from(mutable.data))])
        };

        let (compaction_tx, compaction_rx) = bounded(1);

        Ok((
            crate::Schema {
                mutable,
                immutables,
                compaction_tx,
                option,
                recover_wal_ids: None,
            },
            compaction_rx,
        ))
    }

    pub(crate) async fn build_db<R, E>(
        option: Arc<DbOption>,
        compaction_rx: Receiver<CompactTask>,
        executor: E,
        schema: crate::Schema<R, E>,
        version: Version<R, E>,
    ) -> Result<DB<R, E>, DbError<R>>
    where
        R: Record + Send + Sync,
        R::Columns: Send + Sync,
        E: Executor + Send + Sync + 'static,
    {
        E::create_dir_all(&option.path).await?;

        let schema = Arc::new(RwLock::new(schema));

        let (mut cleaner, clean_sender) = Cleaner::<E>::new(option.clone());
        let version_set = build_version_set(version, clean_sender, option.clone()).await?;
        let mut compactor =
            Compactor::<R, E>::new(schema.clone(), option.clone(), version_set.clone());

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });
        executor.spawn(async move {
            while let Ok(task) = compaction_rx.recv_async().await {
                match task {
                    CompactTask::Freeze => {
                        if let Err(err) = compactor.check_then_compaction().await {
                            error!("[Compaction Error]: {}", err)
                        }
                    }
                }
            }
        });

        Ok(DB {
            schema,
            version_set,
            lock_map: Arc::new(Default::default()),
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

        let mut option = DbOption::from(temp_dir.path());
        option.max_mem_table_size = 5;
        option.immutable_chunk_num = 1;
        option.major_threshold_with_sst_size = 5;
        option.level_sst_magnification = 10;
        option.max_sst_file_size = 2 * 1024 * 1024;

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::new()).await.unwrap();

        for item in test_items() {
            db.write(item, 0.into()).await.unwrap();
        }

        let tx = db.transaction().await;
        let key = 20.to_string();
        let option1 = tx.get(&key, Projection::All).await.unwrap().unwrap();

        assert_eq!(option1.get().vstring, "20");
        assert_eq!(option1.get().vu32, Some(0));
        assert_eq!(option1.get().vbool, Some(true));

        dbg!(db.version_set.current().await);
    }

    #[tokio::test]
    async fn schema_recover() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::from(temp_dir.path()));
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (task_tx, _task_rx) = bounded(1);

        let schema: crate::Schema<Test, TokioExecutor> = crate::Schema {
            mutable: Mutable::new(&option).await.unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx.clone(),
            option: option.clone(),
            recover_wal_ids: None,
        };

        for (i, item) in test_items().into_iter().enumerate() {
            schema
                .write(LogType::Full, item, (i as u32).into())
                .await
                .unwrap();
        }
        drop(schema);

        let schema: crate::Schema<Test, TokioExecutor> = crate::Schema {
            mutable: Mutable::new(&option).await.unwrap(),
            immutables: Default::default(),
            compaction_tx: task_tx,
            option: option.clone(),
            recover_wal_ids: None,
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
}
