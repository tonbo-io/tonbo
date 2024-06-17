use std::{
    collections::{btree_map::Range, BTreeMap},
    mem::transmute,
    ops::Bound,
};

use arrow::array::RecordBatch;
use futures_util::stream::{self, Iter};

use super::mutable::Mutable;
use crate::{
    oracle::{
        timestamp::{Timestamped, TimestampedRef},
        Timestamp, EPOCH,
    },
    record::{internal::InternalRecordRef, Record, RecordRef},
    stream::record_batch::RecordBatchEntry,
};

pub trait ArrowArrays: Sized {
    type Record: Record;

    type Buider: Builder<Self>;

    fn builder(capacity: usize) -> Self::Buider;

    fn get(&self, offset: u32) -> Option<Option<<Self::Record as Record>::Ref<'_>>>;

    fn as_record_batch(&self) -> &RecordBatch;
}

pub trait Builder<S>
where
    S: ArrowArrays,
{
    fn push(&mut self, key: &Timestamped<<S::Record as Record>::Key>, row: &Option<S::Record>);

    fn finish(&mut self) -> S;
}

pub(crate) struct Immutable<A>
where
    A: ArrowArrays,
{
    data: A,
    index: BTreeMap<Timestamped<<A::Record as Record>::Key>, u32>,
}

impl<A> From<Mutable<A::Record>> for Immutable<A>
where
    A: ArrowArrays,
    A::Record: Send + Sync,
{
    fn from(mutable: Mutable<A::Record>) -> Self {
        let mut index = BTreeMap::new();
        let mut builder = A::builder(mutable.len());

        for (offset, (key, value)) in mutable.into_iter().enumerate() {
            builder.push(&key, &value);
            index.insert(key, offset as u32);
        }

        let data = builder.finish();

        Self { data, index }
    }
}

impl<A> Immutable<A>
where
    A: ArrowArrays,
{
    pub(crate) fn as_record_batch(&self) -> &RecordBatch {
        self.data.as_record_batch()
    }

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (
            Bound<&'scan <A::Record as Record>::Key>,
            Bound<&'scan <A::Record as Record>::Key>,
        ),
        ts: Timestamp,
    ) -> ImmutableScan<'scan, A::Record> {
        let lower = range.0.map(|key| TimestampedRef::new(key, ts));
        let upper = range.1.map(|key| TimestampedRef::new(key, EPOCH));

        let range = self
            .index
            .range::<TimestampedRef<<A::Record as Record>::Key>, _>((lower, upper));

        ImmutableScan::<A::Record>::new(range, self.data.as_record_batch())
    }
}

pub struct ImmutableScan<'iter, R>
where
    R: Record,
{
    range: Range<'iter, Timestamped<R::Key>, u32>,
    record_batch: &'iter RecordBatch,
}

impl<'iter, R> ImmutableScan<'iter, R>
where
    R: Record,
{
    fn new(
        range: Range<'iter, Timestamped<R::Key>, u32>,
        record_batch: &'iter RecordBatch,
    ) -> Self {
        Self {
            range,
            record_batch,
        }
    }
}

impl<'iter, R> Iterator for ImmutableScan<'iter, R>
where
    R: Record,
{
    type Item = RecordBatchEntry<R>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|(_, &offset)| {
            let record_ref = R::Ref::from_record_batch(self.record_batch, offset as usize);
            // TODO: remove cloning record batch
            RecordBatchEntry::new(self.record_batch.clone(), unsafe {
                transmute::<InternalRecordRef<R::Ref<'_>>, InternalRecordRef<R::Ref<'static>>>(
                    record_ref,
                )
            })
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, BooleanArray, BooleanBufferBuilder, BooleanBuilder, PrimitiveBuilder,
            RecordBatch, StringArray, StringBuilder, UInt32Array, UInt32Builder,
        },
        datatypes::{ArrowPrimitiveType, UInt32Type},
    };

    use super::{ArrowArrays, Builder};
    use crate::{
        oracle::timestamp::Timestamped,
        record::Record,
        tests::{Test, TestRef},
    };

    #[derive(Debug)]
    pub struct TestImmutableArrays {
        _null: Arc<BooleanArray>,
        _ts: Arc<UInt32Array>,
        vstring: Arc<StringArray>,
        vu32: Arc<UInt32Array>,
        vbool: Arc<BooleanArray>,

        record_batch: RecordBatch,
    }

    impl ArrowArrays for TestImmutableArrays {
        type Record = Test;

        type Buider = TestBuilder;

        fn builder(capacity: usize) -> Self::Buider {
            TestBuilder {
                vstring: StringBuilder::with_capacity(capacity, 0),
                vu32: PrimitiveBuilder::<UInt32Type>::with_capacity(capacity),
                vobool: BooleanBuilder::with_capacity(capacity),
                _null: BooleanBufferBuilder::new(capacity),
                _ts: UInt32Builder::with_capacity(capacity),
            }
        }

        fn get(&self, offset: u32) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
            let offset = offset as usize;

            if offset >= self.vstring.len() {
                return None;
            }

            let vstring = self.vstring.value(offset);
            let vu32 = self.vu32.value(offset);
            let vbool = if self.vbool.is_null(offset) {
                None
            } else {
                Some(self.vbool.value(offset))
            };

            let is_null = self._null.value(offset);
            Some(if !is_null {
                Some(TestRef {
                    vstring,
                    vu32,
                    vbool,
                })
            } else {
                None
            })
        }

        fn as_record_batch(&self) -> &RecordBatch {
            &self.record_batch
        }
    }

    pub struct TestBuilder {
        vstring: StringBuilder,
        vu32: PrimitiveBuilder<UInt32Type>,
        vobool: BooleanBuilder,
        _null: BooleanBufferBuilder,
        _ts: UInt32Builder,
    }

    impl Builder<TestImmutableArrays> for TestBuilder {
        fn push(&mut self, key: &Timestamped<String>, row: &Option<Test>) {
            self.vstring.append_value(&key.value);
            match row {
                Some(row) => {
                    self.vu32.append_value(row.vu32);
                    match row.vobool {
                        Some(vobool) => self.vobool.append_value(vobool),
                        None => self.vobool.append_null(),
                    }
                    self._null.append(false);
                    self._ts.append_value(key.ts.into());
                }
                None => {
                    self.vu32
                        .append_value(<UInt32Type as ArrowPrimitiveType>::Native::default());
                    self.vobool.append_null();
                    self._null.append(true);
                    self._ts.append_value(key.ts.into());
                }
            }
        }

        fn finish(&mut self) -> TestImmutableArrays {
            let vstring = Arc::new(self.vstring.finish());
            let vu32 = Arc::new(self.vu32.finish());
            let vbool = Arc::new(self.vobool.finish());
            let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
            let _ts = Arc::new(self._ts.finish());
            let record_batch = RecordBatch::try_new(
                Arc::clone(
                    <<TestImmutableArrays as ArrowArrays>::Record as Record>::arrow_schema(),
                ),
                vec![
                    Arc::clone(&_null) as Arc<dyn Array>,
                    Arc::clone(&_ts) as Arc<dyn Array>,
                    Arc::clone(&vstring) as Arc<dyn Array>,
                    Arc::clone(&vu32) as Arc<dyn Array>,
                    Arc::clone(&vbool) as Arc<dyn Array>,
                ],
            )
            .expect("create record batch must be successful");

            TestImmutableArrays {
                vstring,
                vu32,
                vbool,
                _null,
                _ts,
                record_batch,
            }
        }
    }
}
