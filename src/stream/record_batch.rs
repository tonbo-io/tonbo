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
    projection_mask: ProjectionMask,
    full_schema: Arc<Schema>,
    order: Option<Order>,
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
        let offset = if order == Some(Order::Desc) {
            // Start from the last row for descending order
            record_batch.num_rows().saturating_sub(1)
        } else {
            0
        };

        Self {
            record_batch,
            offset,
            projection_mask,
            full_schema,
            order,
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
        let num_rows = self.record_batch.num_rows();

        // Check bounds based on order
        if self.order == Some(Order::Desc) {
            if num_rows == 0 || self.offset >= num_rows {
                return None;
            }
        } else {
            if self.offset >= num_rows {
                return None;
            }
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

        // Update offset based on order
        if self.order == Some(Order::Desc) {
            if self.offset == 0 {
                self.offset = num_rows; // Set to out of bounds to stop iteration
            } else {
                self.offset -= 1;
            }
        } else {
            self.offset += 1;
        }

        Some(entry)
    }
}

#[cfg(test)]
mod tests {}
