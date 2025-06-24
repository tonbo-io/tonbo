use std::{
    collections::{btree_map::Range, BTreeMap},
    mem::transmute,
    ops::Bound,
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};
use common::Key;
use crossbeam_skiplist::SkipMap;
use parquet::arrow::ProjectionMask;

use crate::{
    record::{option::OptionRecordRef, Record, RecordRef},
    stream::record_batch::RecordBatchEntry,
    timestamp::{Timestamp, Ts, TsRef, EPOCH},
};

pub trait ArrowArrays: Sized + Sync {
    type Record: Record;

    type Builder: Builder<Self>;

    fn builder(schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder;

    fn get(
        &self,
        offset: u32,
        projection_mask: &ProjectionMask,
    ) -> Option<Option<<Self::Record as Record>::Ref<'_>>>;

    fn as_record_batch(&self) -> &RecordBatch;
}

pub trait Builder<S>: Send
where
    S: ArrowArrays,
{
    fn push(
        &mut self,
        key: Ts<<<S::Record as Record>::Key as Key>::Ref<'_>>,
        row: Option<<S::Record as Record>::Ref<'_>>,
    );

    fn written_size(&self) -> usize;

    fn finish(&mut self, indices: Option<&[usize]>) -> S;
}

pub(crate) struct Immutable<A>
where
    A: ArrowArrays,
{
    data: A,
    index: BTreeMap<Ts<<A::Record as Record>::Key>, u32>,
}

impl<A> Immutable<A>
where
    A: ArrowArrays,
    A::Record: Send,
{
    pub(crate) fn new(
        mutable: SkipMap<Ts<<A::Record as Record>::Key>, Option<A::Record>>,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        let mut index = BTreeMap::new();
        let mut builder = A::builder(schema, mutable.len());

        for (offset, (key, value)) in mutable.into_iter().enumerate() {
            builder.push(
                Ts::new(key.value.as_key_ref(), key.ts),
                value.as_ref().map(Record::as_record_ref),
            );
            index.insert(key, offset as u32);
        }

        let data = builder.finish(None);

        Self { data, index }
    }
}

impl<A> Immutable<A>
where
    A: ArrowArrays,
{
    pub(crate) fn scope(
        &self,
    ) -> (
        Option<&<A::Record as Record>::Key>,
        Option<&<A::Record as Record>::Key>,
    ) {
        (
            self.index.first_key_value().map(|(key, _)| key.value()),
            self.index.last_key_value().map(|(key, _)| key.value()),
        )
    }

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
        projection_mask: ProjectionMask,
    ) -> ImmutableScan<'scan, A::Record> {
        let lower = match range.0 {
            Bound::Included(key) => Bound::Included(TsRef::new(key, ts)),
            Bound::Excluded(key) => Bound::Excluded(TsRef::new(key, EPOCH)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match range.1 {
            Bound::Included(key) => Bound::Included(TsRef::new(key, EPOCH)),
            Bound::Excluded(key) => Bound::Excluded(TsRef::new(key, ts)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let range = self
            .index
            .range::<TsRef<<A::Record as Record>::Key>, _>((lower, upper));

        ImmutableScan::<A::Record>::new(range, self.data.as_record_batch(), projection_mask)
    }

    pub(crate) fn get(
        &self,
        key: &<A::Record as Record>::Key,
        ts: Timestamp,
        projection_mask: ProjectionMask,
    ) -> Option<RecordBatchEntry<A::Record>> {
        self.scan(
            (Bound::Included(key), Bound::Included(key)),
            ts,
            projection_mask,
        )
        .next()
    }

    pub(crate) fn check_conflict(&self, key: &<A::Record as Record>::Key, ts: Timestamp) -> bool {
        self.index
            .range::<TsRef<<A::Record as Record>::Key>, _>((
                Bound::Excluded(TsRef::new(key, u32::MAX.into())),
                Bound::Excluded(TsRef::new(key, ts)),
            ))
            .next()
            .is_some()
    }
}

pub(crate) struct ImmutableScan<'iter, R>
where
    R: Record,
{
    range: Range<'iter, Ts<R::Key>, u32>,
    record_batch: &'iter RecordBatch,
    projection_mask: ProjectionMask,
}

impl<'iter, R> ImmutableScan<'iter, R>
where
    R: Record,
{
    fn new(
        range: Range<'iter, Ts<R::Key>, u32>,
        record_batch: &'iter RecordBatch,
        projection_mask: ProjectionMask,
    ) -> Self {
        Self {
            range,
            record_batch,
            projection_mask,
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
            let schema = self.record_batch.schema();
            let record_ref = R::Ref::from_record_batch(
                self.record_batch,
                offset as usize,
                &self.projection_mask,
                &schema,
            );
            // TODO: remove cloning record batch
            RecordBatchEntry::new(self.record_batch.clone(), {
                // Safety: record_ref self-references the record batch
                unsafe {
                    transmute::<OptionRecordRef<R::Ref<'_>>, OptionRecordRef<R::Ref<'static>>>(
                        record_ref,
                    )
                }
            })
        })
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::{mem, sync::Arc};

    use arrow::{
        array::{
            Array, BooleanArray, BooleanBufferBuilder, BooleanBuilder, PrimitiveBuilder,
            RecordBatch, StringArray, StringBuilder, UInt32Array, UInt32Builder,
        },
        datatypes::{ArrowPrimitiveType, Schema as ArrowSchema, UInt32Type},
    };
    use parquet::arrow::ProjectionMask;

    use super::{ArrowArrays, Builder};
    use crate::{
        magic,
        record::{Record, Schema},
        tests::{Test, TestRef},
        timestamp::Ts,
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

        type Builder = TestBuilder;

        fn builder(_schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
            TestBuilder {
                vstring: StringBuilder::with_capacity(capacity, 0),
                vu32: PrimitiveBuilder::<UInt32Type>::with_capacity(capacity),
                vobool: BooleanBuilder::with_capacity(capacity),
                _null: BooleanBufferBuilder::new(capacity),
                _ts: UInt32Builder::with_capacity(capacity),
            }
        }

        fn get(
            &self,
            offset: u32,
            projection_mask: &ProjectionMask,
        ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
            let offset = offset as usize;

            if offset >= self.vstring.len() {
                return None;
            }
            if self._null.value(offset) {
                return Some(None);
            }

            let vstring = self.vstring.value(offset);
            let vu32 = projection_mask
                .leaf_included(3)
                .then(|| self.vu32.value(offset));
            let vbool = (!self.vbool.is_null(offset) && projection_mask.leaf_included(4))
                .then(|| self.vbool.value(offset));

            Some(Some(TestRef {
                vstring,
                vu32,
                vbool,
            }))
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
        fn push(&mut self, key: Ts<&str>, row: Option<TestRef>) {
            self.vstring.append_value(key.value);
            match row {
                Some(row) => {
                    self.vu32.append_value(row.vu32.unwrap());
                    match row.vbool {
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

        fn written_size(&self) -> usize {
            self._null.as_slice().len()
                + mem::size_of_val(self._ts.values_slice())
                + mem::size_of_val(self.vstring.values_slice())
                + mem::size_of_val(self.vu32.values_slice())
                + mem::size_of_val(self.vobool.values_slice())
        }

        fn finish(&mut self, indices: Option<&[usize]>) -> TestImmutableArrays {
            let vstring = Arc::new(self.vstring.finish());
            let vu32 = Arc::new(self.vu32.finish());
            let vbool = Arc::new(self.vobool.finish());
            let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
            let _ts = Arc::new(self._ts.finish());
            let schema = Test::schema();
            let mut record_batch = RecordBatch::try_new(
                schema.arrow_schema().clone(),
                vec![
                    Arc::clone(&_null) as Arc<dyn Array>,
                    Arc::clone(&_ts) as Arc<dyn Array>,
                    Arc::clone(&vstring) as Arc<dyn Array>,
                    Arc::clone(&vu32) as Arc<dyn Array>,
                    Arc::clone(&vbool) as Arc<dyn Array>,
                ],
            )
            .expect("create record batch must be successful");
            if let Some(indices) = indices {
                record_batch = record_batch
                    .project(indices)
                    .expect("projection indices must be successful");
            }

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
