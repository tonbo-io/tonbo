use std::{
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
    mem::transmute,
};

use arrow::array::RecordBatch;

use crate::{
    record::{internal::InternalRecordRef, Key, Record, RecordRef},
    timestamp::Timestamped,
};

pub struct RecordBatchEntry<R>
where
    R: Record,
{
    record_batch: RecordBatch,
    record_ref: InternalRecordRef<'static, R::Ref<'static>>,
}

impl<R> RecordBatchEntry<R>
where
    R: Record,
{
    pub(crate) fn new(
        record_batch: RecordBatch,
        record_ref: InternalRecordRef<'static, R::Ref<'static>>,
    ) -> Self {
        Self {
            record_batch,
            record_ref,
        }
    }

    pub(crate) fn internal_key(&self) -> Timestamped<<R::Key as Key>::Ref<'_>> {
        self.record_ref.value()
    }

    pub fn key(&self) -> <R::Key as Key>::Ref<'_> {
        *self.record_ref.value().value()
    }

    pub fn get(&self) -> R::Ref<'_> {
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
    _marker: PhantomData<R>,
}

impl<R> RecordBatchIterator<R>
where
    R: Record,
{
    pub(crate) fn new(record_batch: RecordBatch) -> Self {
        Self {
            record_batch,
            offset: 0,
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
        let record = R::Ref::from_record_batch(&self.record_batch, self.offset);
        let entry = RecordBatchEntry::new(record_batch, unsafe {
            // Safety: self-referring lifetime is safe
            transmute::<
                InternalRecordRef<'_, R::Ref<'_>>,
                InternalRecordRef<'static, R::Ref<'static>>,
            >(record)
        });
        self.offset += 1;
        Some(entry)
    }
}
