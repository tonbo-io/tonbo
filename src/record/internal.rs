use std::{marker::PhantomData, mem::transmute};

use super::{Key, Record, RecordRef};
use crate::timestamp::{Timestamp, Timestamped};

#[derive(Debug)]
pub(crate) struct InternalRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    ts: Timestamp,
    record: R,
    null: bool,
    _marker: PhantomData<&'r ()>,
}

impl<'r, R> InternalRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    pub(crate) fn new(ts: Timestamp, record: R, null: bool) -> Self {
        Self {
            ts,
            record,
            null,
            _marker: PhantomData,
        }
    }
}

impl<'r, R> InternalRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    pub(crate) fn value(&self) -> Timestamped<<<R::Record as Record>::Key as Key>::Ref<'_>> {
        // Safety: shorter lifetime of the value must be safe
        unsafe { transmute(Timestamped::new(self.record.key(), self.ts)) }
    }

    pub(crate) fn get(&self) -> R {
        self.record
    }
}
