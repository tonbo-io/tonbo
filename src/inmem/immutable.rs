use std::{
    collections::{btree_map::Range, BTreeMap},
    mem::transmute,
    ops::Bound,
};

use arrow::array::RecordBatch;
use parquet::arrow::ProjectionMask;

use super::mutable::Mutable;
use crate::{
    record::{internal::InternalRecordRef, Key, Record, RecordRef},
    stream::record_batch::RecordBatchEntry,
    timestamp::{Timestamp, Timestamped, TimestampedRef, EPOCH},
};

pub trait ArrowArrays: Sized {
    type Record: Record;

    type Builder: Builder<Self>;

    fn builder(capacity: usize) -> Self::Builder;

    fn get(
        &self,
        offset: u32,
        projection_mask: &ProjectionMask,
    ) -> Option<Option<<Self::Record as Record>::Ref<'_>>>;

    fn as_record_batch(&self) -> &RecordBatch;
}

pub trait Builder<S>
where
    S: ArrowArrays,
{
    fn push(
        &mut self,
        key: Timestamped<<<S::Record as Record>::Key as Key>::Ref<'_>>,
        row: Option<<S::Record as Record>::Ref<'_>>,
    );

    fn written_size(&self) -> usize;

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
    A::Record: Send,
{
    fn from(mutable: Mutable<A::Record>) -> Self {
        let mut index = BTreeMap::new();
        let mut builder = A::builder(mutable.len());

        for (offset, (key, value)) in mutable.into_iter().enumerate() {
            builder.push(
                Timestamped::new(key.value.as_key_ref(), key.ts),
                value.as_ref().map(Record::as_record_ref),
            );
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
        let lower = range.0.map(|key| TimestampedRef::new(key, ts));
        let upper = range.1.map(|key| TimestampedRef::new(key, EPOCH));

        let range = self
            .index
            .range::<TimestampedRef<<A::Record as Record>::Key>, _>((lower, upper));

        ImmutableScan::<A::Record>::new(range, self.data.as_record_batch(), projection_mask)
    }

    pub(crate) fn check_conflict(&self, key: &<A::Record as Record>::Key, ts: Timestamp) -> bool {
        self.index
            .range::<TimestampedRef<<A::Record as Record>::Key>, _>((
                Bound::Excluded(TimestampedRef::new(key, u32::MAX.into())),
                Bound::Excluded(TimestampedRef::new(key, ts)),
            ))
            .next()
            .is_some()
    }
}

pub struct ImmutableScan<'iter, R>
where
    R: Record,
{
    range: Range<'iter, Timestamped<R::Key>, u32>,
    record_batch: &'iter RecordBatch,
    projection_mask: ProjectionMask,
}

impl<'iter, R> ImmutableScan<'iter, R>
where
    R: Record,
{
    fn new(
        range: Range<'iter, Timestamped<R::Key>, u32>,
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
            let record_ref = R::Ref::from_record_batch(
                self.record_batch,
                offset as usize,
                &self.projection_mask,
            );
            // TODO: remove cloning record batch
            RecordBatchEntry::new(self.record_batch.clone(), {
                // Safety: record_ref self-references the record batch
                unsafe {
                    transmute::<InternalRecordRef<R::Ref<'_>>, InternalRecordRef<R::Ref<'static>>>(
                        record_ref,
                    )
                }
            })
        })
    }
}
