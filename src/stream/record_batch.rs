use std::{
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    mem::transmute,
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use common::PrimaryKey;
use parquet::arrow::ProjectionMask;

use crate::{
    record::{option::OptionRecordRef, Record, RecordRef},
    timestamp::Ts,
};

pub struct RecordBatchEntry<R>
where
    R: Record,
{
    _record_batch: RecordBatch,
    record_ref: OptionRecordRef<'static, R::Ref<'static>>,
}

impl<R> RecordBatchEntry<R>
where
    R: Record,
{
    pub(crate) fn new(
        _record_batch: RecordBatch,
        record_ref: OptionRecordRef<'static, R::Ref<'static>>,
    ) -> Self {
        Self {
            _record_batch,
            record_ref,
        }
    }

    pub(crate) fn internal_key(&self) -> Ts<PrimaryKey> {
        self.record_ref.key()
    }

    // pub fn key(&self) -> <R::Key as Key>::Ref<'_> {
    //     self.internal_key().value().clone()
    // }
    pub fn key(&self) -> PrimaryKey {
        self.internal_key().value().clone()
    }

    pub fn get(&self) -> Option<R::Ref<'_>> {
        // Safety: shorter lifetime of the key must be safe
        unsafe { transmute(self.record_ref.get()) }
    }
}

impl<R> Debug for RecordBatchEntry<R>
where
    R: Record + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordBatchEntry").finish()
    }
}

#[derive(Debug)]
pub struct RecordBatchIterator<R> {
    record_batch: RecordBatch,
    offset: usize,
    projection_mask: ProjectionMask,
    full_schema: Arc<Schema>,
    _marker: PhantomData<R>,
}

impl<R> RecordBatchIterator<R>
where
    R: Record,
{
    pub(crate) fn new(
        record_batch: RecordBatch,
        projection_mask: ProjectionMask,
        full_schema: Arc<Schema>,
    ) -> Self {
        Self {
            record_batch,
            offset: 0,
            projection_mask,
            full_schema,
            _marker: PhantomData,
        }
    }
}

impl<R> Iterator for RecordBatchIterator<R>
where
    R: Record,
{
    type Item = RecordBatchEntry<R>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.record_batch.num_rows() {
            return None;
        }

        let record_batch = self.record_batch.clone();
        let record = R::Ref::from_record_batch(
            &self.record_batch,
            self.offset,
            &self.projection_mask,
            &self.full_schema,
        );
        let entry = RecordBatchEntry::new(record_batch, unsafe {
            // Safety: self-referring lifetime is safe
            transmute::<OptionRecordRef<'_, R::Ref<'_>>, OptionRecordRef<'static, R::Ref<'static>>>(
                record,
            )
        });
        self.offset += 1;
        Some(entry)
    }
}

#[cfg(test)]
mod tests {}
