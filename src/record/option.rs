use std::{marker::PhantomData, mem::transmute};

use super::{Key, Record, RecordRef};
use crate::timestamp::{Timestamp, Ts};

#[derive(Debug)]
pub struct OptionRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    record: Ts<R>,
    null: bool,
    _marker: PhantomData<&'r ()>,
}

impl<'r, R> OptionRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    pub fn new(ts: Timestamp, record: R, null: bool) -> Self {
        Self {
            record: Ts::new(record, ts),
            null,
            _marker: PhantomData,
        }
    }
}

impl<'r, R> OptionRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    pub fn key(&self) -> Ts<<<R::Record as Record>::Key as Key>::Ref<'_>> {
        // Safety: shorter lifetime of the value must be safe
        unsafe { transmute(Ts::new(self.record.value().clone().key(), self.record.ts())) }
    }

    pub fn get(&self) -> Option<R> {
        if self.null {
            return None;
        }

        Some(self.record.value().clone())
    }
}
