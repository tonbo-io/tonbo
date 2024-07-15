#![allow(dead_code)]
pub(crate) mod arrows;
mod executor;
pub mod fs;
mod inmem;
mod ondisk;
mod oracle;
mod record;
mod stream;
mod transaction;

use std::{collections::VecDeque, io, mem, ops::Bound, sync::Arc};

use async_lock::{RwLock, RwLockReadGuard};
use futures_core::Stream;
use futures_util::StreamExt;
use inmem::{immutable::Immutable, mutable::Mutable};
use oracle::Timestamp;
use parquet::errors::ParquetError;
use record::Record;
use stream::{merge::MergeStream, Entry, ScanStream};

pub struct DB<R>
where
    R: Record,
{
    schema: Arc<RwLock<Schema<R>>>,
}

impl<R> Default for DB<R>
where
    R: Record,
{
    fn default() -> Self {
        Self {
            schema: Arc::new(RwLock::new(Schema::default())),
        }
    }
}

impl<R> DB<R>
where
    R: Record + Send + Sync,
    R::Key: Send,
{
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
    R::Key: Send + Sync,
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
        let mut streams = Vec::<ScanStream<R>>::with_capacity(self.immutables.len() + 1);
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

    pub(crate) async fn get_test_record_batch() -> RecordBatch {
        let db = DB::default();

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
