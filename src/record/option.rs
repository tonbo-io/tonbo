use std::{marker::PhantomData, mem::transmute};

use super::{Key, Record, RecordRef, Schema};
use crate::timestamp::{Timestamp, Timestamped};

#[derive(Debug)]
pub struct OptionRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    record: Timestamped<R>,
    null: bool,
    _marker: PhantomData<&'r ()>,
}

impl<'r, R> OptionRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    pub fn new(ts: Timestamp, record: R, null: bool) -> Self {
        Self {
            record: Timestamped::new(record, ts),
            null,
            _marker: PhantomData,
        }
    }
}

impl<'r, R> OptionRecordRef<'r, R>
where
    R: RecordRef<'r>,
{
    pub fn key(
        &self,
    ) -> Timestamped<<<<R::Record as Record>::Schema as Schema>::Key as Key>::Ref<'_>> {
        // Safety: shorter lifetime of the value must be safe
        unsafe {
            transmute(Timestamped::new(
                self.record.value().clone().key(),
                self.record.ts(),
            ))
        }
    }

    pub fn get(&self) -> Option<R> {
        if self.null {
            return None;
        }

        Some(self.record.value().clone())
    }
}
