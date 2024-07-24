#![allow(dead_code)]
pub(crate) mod arrows;
mod compaction;
pub mod executor;
pub mod fs;
mod inmem;
mod ondisk;
mod record;
mod scope;
pub mod serdes;
mod stream;
mod timestamp;
mod transaction;
pub(crate) mod version;

use std::{
    collections::VecDeque, io, marker::PhantomData, mem, ops::Bound, path::PathBuf, sync::Arc,
};

use async_lock::{RwLock, RwLockReadGuard};
use fs::FileProvider;
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{immutable::Immutable, mutable::Mutable};
use lockable::LockableHashMap;
use parquet::{
    arrow::{arrow_to_parquet_schema, ProjectionMask},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use record::Record;
use thiserror::Error;
use timestamp::Timestamp;
use tracing::error;
use transaction::Transaction;

use crate::{
    executor::Executor,
    fs::{FileId, FileType},
    stream::{merge::MergeStream, Entry, ScanStream},
    version::{cleaner::Cleaner, set::VersionSet, Version, VersionError},
};

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

pub enum Projection {
    All,
    Parts(Vec<usize>),
}

#[derive(Debug)]
pub struct DbOption {
    pub path: PathBuf,
    pub max_mem_table_size: usize,
    pub immutable_chunk_num: usize,
    pub major_threshold_with_sst_size: usize,
    pub level_sst_magnification: usize,
    pub max_sst_file_size: usize,
    pub clean_channel_buffer: usize,
    pub write_parquet_option: Option<WriterProperties>,
}

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

impl DbOption {
    pub fn new(path: impl Into<PathBuf> + Send) -> Self {
        DbOption {
            path: path.into(),
            max_mem_table_size: 8 * 1024 * 1024,
            immutable_chunk_num: 3,
            major_threshold_with_sst_size: 10,
            level_sst_magnification: 10,
            max_sst_file_size: 24 * 1024 * 1024,
            clean_channel_buffer: 10,
            write_parquet_option: None,
        }
    }

    pub(crate) fn table_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.{}", gen, FileType::Parquet))
    }

    pub(crate) fn wal_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.{}", gen, FileType::Wal))
    }

    pub(crate) fn version_path(&self) -> PathBuf {
        self.path.join(format!("version.{}", FileType::Log))
    }

    pub(crate) fn is_threshold_exceeded_major<R, E>(
        &self,
        version: &Version<R, E>,
        level: usize,
    ) -> bool
    where
        R: Record,
        E: FileProvider,
    {
        Version::<R, E>::tables_len(version, level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}

impl<R, E> DB<R, E>
where
    R: Record + Send,
    E: Executor,
{
    pub async fn new(option: Arc<DbOption>, executor: E) -> Result<Self, WriteError<R>> {
        E::create_dir_all(&option.path).await?;

        let schema = Arc::new(RwLock::new(Schema::default()));

        let (mut cleaner, clean_sender) = Cleaner::new(option.clone());

        let version_set = VersionSet::new(clean_sender, option.clone()).await?;

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });

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

    pub(crate) async fn write(&self, record: R, ts: Timestamp) -> io::Result<()> {
        let schema = self.schema.read().await;
        schema.write(record, ts).await
    }

    pub(crate) async fn write_batch(
        &self,
        records: impl Iterator<Item = R>,
        ts: Timestamp,
    ) -> io::Result<()> {
        let columns = self.schema.read().await;
        for record in records {
            columns.write(record, ts).await?;
        }
        Ok(())
    }

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, Schema<R, E>> {
        self.schema.read().await
    }
}

pub(crate) struct Schema<R, FP>
where
    R: Record,
{
    mutable: Mutable<R>,
    immutables: VecDeque<Immutable<R::Columns>>,
    _marker: PhantomData<FP>,
}

impl<R, FP> Default for Schema<R, FP>
where
    R: Record,
{
    fn default() -> Self {
        Self {
            mutable: Mutable::default(),
            immutables: VecDeque::default(),
            _marker: Default::default(),
        }
    }
}

impl<R, FP> Schema<R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    async fn write(&self, record: R, ts: Timestamp) -> io::Result<()> {
        self.mutable.insert(record, ts);
        Ok(())
    }

    async fn remove(&self, key: R::Key, ts: Timestamp) -> io::Result<()> {
        self.mutable.remove(key, ts);
        Ok(())
    }

    async fn get<'get>(
        &'get self,
        key: &'get R::Key,
        ts: Timestamp,
        projection: Projection,
    ) -> Result<Option<Entry<'get, R>>, ParquetError> {
        let mut scan = self.scan(Bound::Included(key), Bound::Unbounded, ts);

        if let Projection::Parts(projection) = projection {
            scan = scan.projection(projection)
        }
        scan.take().await?.next().await.transpose()
    }

    fn scan<'scan>(
        &'scan self,
        lower: Bound<&'scan R::Key>,
        uppwer: Bound<&'scan R::Key>,
        ts: Timestamp,
    ) -> Scan<'scan, R, FP> {
        Scan::new(self, lower, uppwer, ts)
    }

    fn check_conflict(&self, key: &R::Key, ts: Timestamp) -> bool {
        self.mutable.check_conflict(key, ts)
            || self
                .immutables
                .iter()
                .any(|immutable| immutable.check_conflict(key, ts))
    }

    fn freeze(&mut self) {
        let mutable = mem::replace(&mut self.mutable, Mutable::new());
        let immutable = Immutable::from(mutable);
        self.immutables.push_front(immutable);
    }
}

pub struct Scan<'scan, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    schema: &'scan Schema<R, FP>,
    lower: Bound<&'scan R::Key>,
    uppwer: Bound<&'scan R::Key>,
    ts: Timestamp,

    projection: ProjectionMask,
}

impl<'scan, R, FP> Scan<'scan, R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    fn new(
        schema: &'scan Schema<R, FP>,
        lower: Bound<&'scan R::Key>,
        uppwer: Bound<&'scan R::Key>,
        ts: Timestamp,
    ) -> Self {
        Self {
            schema,
            lower,
            uppwer,
            ts,
            projection: ProjectionMask::all(),
        }
    }

    pub fn projection(self, mut projection: Vec<usize>) -> Self {
        // skip two columns: _null and _ts
        for p in &mut projection {
            *p += 2;
        }

        let mask = ProjectionMask::roots(
            &arrow_to_parquet_schema(R::arrow_schema()).unwrap(),
            projection,
        );

        Self {
            projection: mask,
            ..self
        }
    }

    pub async fn take(
        self,
    ) -> Result<impl Stream<Item = Result<Entry<'scan, R>, ParquetError>>, ParquetError> {
        let mut streams = Vec::<ScanStream<R, FP>>::with_capacity(self.schema.immutables.len() + 1);
        streams.push(
            self.schema
                .mutable
                .scan((self.lower, self.uppwer), self.ts)
                .into(),
        );
        for immutable in &self.schema.immutables {
            streams.push(
                immutable
                    .scan((self.lower, self.uppwer), self.ts, self.projection.clone())
                    .into(),
            );
        }
        // TODO: sstable scan

        MergeStream::from_vec(streams).await
    }
}

#[derive(Debug, Error)]
pub enum WriteError<R>
where
    R: Record,
{
    #[error("write io error: {0}")]
    Io(#[from] io::Error),
    #[error("write version error: {0}")]
    Version(#[from] VersionError<R>),
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, AsArray, RecordBatch},
        datatypes::{DataType, Field, Schema, UInt32Type},
    };
    use once_cell::sync::Lazy;
    use parquet::arrow::ProjectionMask;

    use crate::{
        executor::Executor,
        inmem::immutable::tests::TestImmutableArrays,
        record::{internal::InternalRecordRef, RecordRef},
        DbOption, Record, DB,
    };

    #[derive(Debug, PartialEq, Eq)]
    pub struct Test {
        pub vstring: String,
        pub vu32: u32,
        pub vobool: Option<bool>,
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

        fn as_record_ref(&self) -> Self::Ref<'_> {
            TestRef {
                vstring: &self.vstring,
                vu32: Some(self.vu32),
                vbool: self.vobool,
            }
        }

        fn arrow_schema() -> &'static Arc<Schema> {
            static SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
                Arc::new(Schema::new(vec![
                    Field::new("_null", DataType::Boolean, false),
                    Field::new("_ts", DataType::UInt32, false),
                    Field::new("vstring", DataType::Utf8, false),
                    Field::new("vu32", DataType::UInt32, false),
                    Field::new("vobool", DataType::Boolean, true),
                ]))
            });

            &SCHEMA
        }
    }

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub struct TestRef<'r> {
        // primary key cannot be projected
        pub vstring: &'r str,
        pub vu32: Option<u32>,
        // Kould: two layer option can be a single layer option
        pub vbool: Option<bool>,
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
            let null = record_batch.column(0).as_boolean().value(offset);

            let ts = record_batch
                .column(1)
                .as_primitive::<UInt32Type>()
                .value(offset)
                .into();
            let vstring = record_batch.column(2).as_string::<i32>();

            let mut vu32 = None;
            let mut vbool = None;
            let mut column_i = 3;

            if projection_mask.leaf_included(3) {
                vu32 = Some(
                    record_batch
                        .column(column_i)
                        .as_primitive::<UInt32Type>()
                        .value(offset),
                );
                column_i += 1;
            }
            if projection_mask.leaf_included(4) {
                let vbool_array = record_batch.column(column_i).as_boolean();

                if !vbool_array.is_null(offset) {
                    vbool = Some(vbool_array.value(offset));
                }
            }

            let record = TestRef {
                vstring: vstring.value(offset),
                vu32,
                vbool,
            };
            InternalRecordRef::new(ts, record, null)
        }
    }

    pub(crate) async fn get_test_record_batch<E: Executor>(
        option: Arc<DbOption>,
        executor: E,
    ) -> RecordBatch {
        let db: DB<Test, E> = DB::new(option, executor).await.unwrap();

        db.write(
            Test {
                vstring: "hello".to_string(),
                vu32: 12,
                vobool: Some(true),
            },
            1.into(),
        )
        .await
        .unwrap();
        db.write(
            Test {
                vstring: "world".to_string(),
                vu32: 12,
                vobool: None,
            },
            1.into(),
        )
        .await
        .unwrap();

        let mut schema = db.schema.write().await;

        schema.freeze();

        schema.immutables[0].as_record_batch().clone()
    }
}
