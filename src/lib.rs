#![allow(dead_code)]
mod compaction;
pub mod executor;
pub mod fs;
mod inmem;
mod ondisk;
pub mod option;
mod record;
mod scope;
pub mod serdes;
mod stream;
mod timestamp;
mod transaction;
mod version;
mod wal;

use std::{collections::VecDeque, io, marker::PhantomData, mem, ops::Bound, sync::Arc};

use async_lock::{RwLock, RwLockReadGuard};
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
use transaction::Transaction;

pub use crate::option::*;
use crate::{
    executor::Executor,
    stream::{merge::MergeStream, Entry, ScanStream},
    version::{cleaner::Cleaner, set::VersionSet, Version, VersionError},
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
        version: &'get Version<R, FP>,
        key: &'get R::Key,
        ts: Timestamp,
        projection: Projection,
    ) -> Result<Option<Entry<'get, R>>, WriteError<R>>
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
    upper: Bound<&'scan R::Key>,
    ts: Timestamp,

    version: &'scan Version<R, FP>,
    streams: Vec<ScanStream<'scan, R, FP>>,

    limit: Option<usize>,
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
            projection,
        );

        Self {
            projection: mask,
            ..self
        }
    }

    pub async fn take(
        mut self,
    ) -> Result<impl Stream<Item = Result<Entry<'scan, R>, ParquetError>>, WriteError<R>> {
        self.streams.push(
            self.schema
                .mutable
                .scan((self.lower, self.upper), self.ts)
                .into(),
        );
        for immutable in &self.schema.immutables {
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
    #[error("write parquet error: {0}")]
    Parquet(#[from] ParquetError),
}

type LockMap<K> = Arc<LockableHashMap<K, ()>>;

pub enum Projection {
    All,
    Parts(Vec<usize>),
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use arrow::{
        array::{
            Array, AsArray, BooleanArray, BooleanBufferBuilder, BooleanBuilder, PrimitiveBuilder,
            RecordBatch, StringArray, StringBuilder, UInt32Array, UInt32Builder,
        },
        datatypes::{DataType, Field, Schema, UInt32Type},
    };
    use async_lock::RwLock;
    use futures_util::io;
    use morseldb_marco::morsel_record;
    use once_cell::sync::Lazy;
    use parquet::arrow::ProjectionMask;
    use tracing::error;

    use crate::{
        executor::{tokio::TokioExecutor, Executor},
        inmem::{
            immutable::{ArrowArrays, Builder},
            mutable::Mutable,
        },
        record::{internal::InternalRecordRef, Key, RecordRef},
        serdes::{
            option::{DecodeError, EncodeError},
            Decode, Encode,
        },
        timestamp::Timestamped,
        version::{cleaner::Cleaner, set::tests::build_version_set, Version},
        DbOption, Immutable, Record, WriteError, DB,
    };

    #[morsel_record]
    pub struct Test {
        #[primary_key]
        pub vstring: String,
        pub vu32: u32,
        pub vbool: Option<bool>,
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

        schema.freeze();

        schema.immutables[0].as_record_batch().clone()
    }

    pub(crate) async fn build_schema() -> crate::Schema<Test, TokioExecutor> {
        let mutable = Mutable::new();

        mutable.insert(
            Test {
                vstring: "alice".to_string(),
                vu32: 1,
                vbool: Some(true),
            },
            1_u32.into(),
        );
        mutable.insert(
            Test {
                vstring: "ben".to_string(),
                vu32: 2,
                vbool: Some(true),
            },
            1_u32.into(),
        );
        mutable.insert(
            Test {
                vstring: "carl".to_string(),
                vu32: 3,
                vbool: Some(true),
            },
            1_u32.into(),
        );

        let immutables = {
            let mutable = Mutable::new();

            mutable.insert(
                Test {
                    vstring: "dice".to_string(),
                    vu32: 4,
                    vbool: Some(true),
                },
                1_u32.into(),
            );
            mutable.insert(
                Test {
                    vstring: "erika".to_string(),
                    vu32: 5,
                    vbool: Some(true),
                },
                1_u32.into(),
            );
            mutable.insert(
                Test {
                    vstring: "funk".to_string(),
                    vu32: 6,
                    vbool: Some(true),
                },
                1_u32.into(),
            );

            VecDeque::from(vec![Immutable::from(mutable)])
        };

        crate::Schema {
            mutable,
            immutables,
            _marker: Default::default(),
        }
    }

    pub(crate) async fn build_db<R, E>(
        option: Arc<DbOption>,
        executor: E,
        schema: crate::Schema<R, E>,
        version: Version<R, E>,
    ) -> Result<DB<R, E>, WriteError<R>>
    where
        R: Record,
        E: Executor,
    {
        E::create_dir_all(&option.path).await?;

        let schema = Arc::new(RwLock::new(schema));

        let (mut cleaner, clean_sender) = Cleaner::new(option.clone());
        let version_set = build_version_set(version, clean_sender, option.clone()).await?;

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });

        Ok(DB {
            schema,
            version_set,
            lock_map: Arc::new(Default::default()),
            _p: Default::default(),
        })
    }
}
