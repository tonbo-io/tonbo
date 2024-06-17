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

use std::{
    any::TypeId,
    collections::{hash_map::Entry, HashMap, VecDeque},
    io, mem,
    ops::Bound,
    sync::Arc,
};

use async_lock::RwLock;
use inmem::{immutable::Immutable, mutable::Mutable};
use oracle::{timestamp::Timestamped, Timestamp};
use record::Record;

#[derive(Debug)]
pub struct DB {
    schemas: std::sync::RwLock<HashMap<TypeId, *const ()>>,
}

impl DB {
    pub fn empty() -> Self {
        Self {
            schemas: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub(crate) async fn write<R>(&self, record: R, ts: Timestamp) -> io::Result<()>
    where
        R: Record + Send + Sync,
        R::Key: Send,
    {
        let columns = self.get_schema::<R>();
        let columns = columns.read().await;
        columns.write(record, ts).await
    }

    pub(crate) async fn write_batch<R>(
        &self,
        records: impl Iterator<Item = R>,
        ts: Timestamp,
    ) -> io::Result<()>
    where
        R: Record + Send + Sync,
        R::Key: Send,
    {
        let columns = self.get_schema::<R>();
        let columns = columns.read().await;
        for record in records {
            columns.write(record, ts).await?;
        }
        Ok(())
    }

    pub(crate) async fn get<R: Record>(&self, key: Timestamped<R::Key>) -> io::Result<Option<&R>> {
        let columns = self.get_schema::<R>();
        let columns = columns.read().await;
        // columns.get(key, ts).await
        todo!()
    }

    pub async fn range_scan<T: Record>(&self, start: Bound<&T::Key>, end: Bound<&T::Key>) {}

    fn get_schema<R>(&self) -> Arc<RwLock<Schema<R>>>
    where
        R: Record,
    {
        let schemas = self.schemas.read().unwrap();
        match schemas.get(&TypeId::of::<R>()) {
            Some(schema) => {
                let inner = unsafe { Arc::from_raw(*schema as *const RwLock<Schema<R>>) };
                let schema = inner.clone();
                std::mem::forget(inner);
                schema
            }
            None => {
                drop(schemas);
                let mut schemas = self.schemas.write().unwrap();
                match schemas.entry(TypeId::of::<R>()) {
                    Entry::Occupied(o) => unsafe {
                        let inner = Arc::from_raw(*o.get() as *const RwLock<Schema<R>>);
                        let schema = inner.clone();
                        std::mem::forget(inner);
                        schema
                    },
                    Entry::Vacant(v) => {
                        let schema = Schema {
                            mutable: Mutable::new(),
                            immutables: VecDeque::new(),
                        };
                        let columns = Arc::new(RwLock::new(schema));
                        v.insert(Arc::into_raw(columns.clone()) as *const ());
                        columns
                    }
                }
            }
        }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.schemas
            .write()
            .unwrap()
            .values()
            .for_each(|schema| unsafe {
                Arc::from_raw(*schema as *const RwLock<()>);
            });
    }
}

pub(crate) struct Schema<R>
where
    R: Record,
{
    mutable: Mutable<R>,
    immutables: VecDeque<Immutable<R::Columns>>,
}

impl<R> Schema<R>
where
    R: Record + Send + Sync,
    R::Key: Send + Sync,
{
    async fn write(&self, record: R, ts: Timestamp) -> io::Result<()> {
        self.mutable.insert(Timestamped::new(record, ts));
        Ok(())
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
        let db = DB::empty();

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

        let schema = db.get_schema::<Test>();

        let mut schema = schema.write().await;

        schema.freeze();

        schema.immutables[0].as_record_batch().clone()
    }
}
