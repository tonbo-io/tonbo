#![allow(dead_code)]
pub(crate) mod arrows;
pub mod executor;
pub mod fs;
mod inmem;
mod ondisk;
mod oracle;
mod record;
mod scope;
pub mod serdes;
mod stream;
mod transaction;
mod version;

use std::{
    collections::VecDeque, io, marker::PhantomData, mem, ops::Bound, path::PathBuf, sync::Arc,
};

use async_lock::{RwLock, RwLockReadGuard};
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{immutable::Immutable, mutable::Mutable};
use oracle::Timestamp;
use parquet::errors::ParquetError;
use record::Record;

use crate::{
    executor::Executor,
    fs::{FileId, FileType},
    stream::{merge::MergeStream, Entry},
    version::Version,
};

#[derive(Debug)]
pub struct DbOption {
    pub path: PathBuf,
    pub max_mem_table_size: usize,
    pub immutable_chunk_num: usize,
    pub major_threshold_with_sst_size: usize,
    pub level_sst_magnification: usize,
    pub max_sst_file_size: usize,
    pub clean_channel_buffer: usize,
}

pub struct DB<R, E>
where
    R: Record,
    E: Executor,
{
    schema: Arc<RwLock<Schema<R>>>,
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
        }
    }

    pub(crate) fn table_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.{}", gen, FileType::PARQUET))
    }

    pub(crate) fn wal_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.{}", gen, FileType::WAL))
    }

    pub(crate) fn version_path(&self) -> PathBuf {
        self.path.join(format!("version.{}", FileType::LOG))
    }

    pub(crate) fn is_threshold_exceeded_major<R, E>(
        &self,
        version: &Version<R, E>,
        level: usize,
    ) -> bool
    where
        R: Record,
        E: Executor,
    {
        Version::<R, E>::tables_len(version, level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}

impl<R, E> Default for DB<R, E>
where
    R: Record,
    E: Executor,
{
    fn default() -> Self {
        Self {
            schema: Arc::new(RwLock::new(Schema::default())),
            _p: Default::default(),
        }
    }
}

impl<R, E> DB<R, E>
where
    R: Record + Send + Sync,
    R::Key: Send,
    E: Executor,
{
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(RwLock::new(Schema::default())),
            _p: Default::default(),
        }
    }

    pub(crate) async fn write(&self, record: R, ts: Timestamp) -> io::Result<()> {
        let columns = self.schema.read().await;
        columns.write(record, ts).await
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

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, Schema<R>> {
        self.schema.read().await
    }
}

pub(crate) struct Schema<R>
where
    R: Record,
{
    mutable: Mutable<R>,
    immutables: VecDeque<Immutable<R::Columns>>,
}

impl<R> Default for Schema<R>
where
    R: Record,
{
    fn default() -> Self {
        Self {
            mutable: Mutable::default(),
            immutables: VecDeque::default(),
        }
    }
}

impl<R> Schema<R>
where
    R: Record + Send + Sync,
    R::Key: Send,
{
    async fn write(&self, record: R, ts: Timestamp) -> io::Result<()> {
        self.mutable.insert(record, ts);
        Ok(())
    }

    async fn get<'get>(
        &'get self,
        key: &'get R::Key,
        ts: Timestamp,
    ) -> Result<Option<Entry<'get, R>>, ParquetError> {
        self.scan(Bound::Included(key), Bound::Unbounded, ts)
            .await?
            .next()
            .await
            .transpose()
    }

    async fn scan<'scan>(
        &'scan self,
        lower: Bound<&'scan R::Key>,
        uppwer: Bound<&'scan R::Key>,
        ts: Timestamp,
    ) -> Result<impl Stream<Item = Result<Entry<'scan, R>, ParquetError>>, ParquetError> {
        let mut streams = Vec::with_capacity(self.immutables.len() + 1);
        streams.push(self.mutable.scan((lower, uppwer), ts).into());
        for immutable in &self.immutables {
            streams.push(immutable.scan((lower, uppwer), ts).into());
        }
        // TODO: sstable scan

        MergeStream::from_vec(streams).await
    }

    fn freeze(&mut self) {
        let mutable = mem::replace(&mut self.mutable, Mutable::new());
        let immutable = Immutable::from(mutable);
        self.immutables.push_front(immutable);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{AsArray, RecordBatch},
        datatypes::{DataType, Field, Schema, UInt32Type},
    };
    use once_cell::sync::Lazy;

    use crate::{
        executor::Executor,
        inmem::immutable::tests::TestImmutableArrays,
        record::{internal::InternalRecordRef, RecordRef},
        Record, DB,
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
                vu32: self.vu32,
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
        pub vstring: &'r str,
        pub vu32: u32,
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
        ) -> InternalRecordRef<Self> {
            let null = record_batch.column(0).as_boolean().value(offset);

            let ts = record_batch
                .column(1)
                .as_primitive::<UInt32Type>()
                .value(offset)
                .into();
            let vstring = record_batch.column(2).as_string::<i32>();
            let vu32 = record_batch.column(3).as_primitive::<UInt32Type>();
            let vobool = record_batch.column(4).as_boolean();

            let record = TestRef {
                vstring: vstring.value(offset),
                vu32: vu32.value(offset),
                vbool: vobool.value(offset).into(),
            };
            InternalRecordRef::new(ts, record, null)
        }
    }

    pub(crate) async fn get_test_record_batch<E: Executor>() -> RecordBatch {
        let db: DB<Test, E> = DB::empty();

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
