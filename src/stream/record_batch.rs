use std::{
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    mem::transmute,
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use parquet::arrow::ProjectionMask;

use crate::{
    option::Order,
    record::{option::OptionRecordRef, Key, Record, RecordRef, Schema as RecordSchema},
    version::timestamp::Ts,
};

pub struct RecordBatchEntry<R>
where
    R: Record,
{
    record_batch: RecordBatch,
    record_ref: OptionRecordRef<'static, R::Ref<'static>>,
}

impl<R> RecordBatchEntry<R>
where
    R: Record,
{
    pub(crate) fn new(
        record_batch: RecordBatch,
        record_ref: OptionRecordRef<'static, R::Ref<'static>>,
    ) -> Self {
        Self {
            record_batch,
            record_ref,
        }
    }

    pub fn record_batch(self) -> RecordBatch {
        self.record_batch
    }

    pub(crate) fn internal_key(&self) -> Ts<<<R::Schema as RecordSchema>::Key as Key>::Ref<'_>> {
        self.record_ref.key()
    }

    pub fn key(&self) -> <<R::Schema as RecordSchema>::Key as Key>::Ref<'_> {
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
    remaining: usize,
    step: isize,
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
        order: Option<Order>,
    ) -> Self {
        let num_rows = record_batch.num_rows();
        let (offset, step) = if matches!(order, Some(Order::Desc)) {
            // Start from the last row for descending order
            (num_rows.saturating_sub(1), -1)
        } else {
            (0, 1)
        };

        Self {
            record_batch,
            offset,
            remaining: num_rows,
            step,
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
        // Check if we have remaining items
        if self.remaining == 0 {
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

        // Update offset and remaining count
        self.offset = (self.offset as isize + self.step) as usize;
        self.remaining -= 1;

        Some(entry)
    }
}

#[cfg(test)]
mod tests {}
