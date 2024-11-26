use std::{marker::PhantomData, mem::transmute};

use super::{Key, Record, RecordRef, Schema};
use crate::timestamp::{Timestamp, Timestamped};

#[derive(Debug)]
pub struct InternalRecordRef<'r, R>
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
    pub fn new(ts: Timestamp, record: R, null: bool) -> Self {
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
    pub fn value(
        &self,
    ) -> Timestamped<<<<R::Record as Record>::Schema as Schema>::Key as Key>::Ref<'_>> {
        // Safety: shorter lifetime of the value must be safe
        unsafe { transmute(Timestamped::new(self.record.clone().key(), self.ts)) }
    }

    pub fn get(&self) -> Option<R> {
        if self.null {
            return None;
        }

        Some(self.record.clone())
    }
}
