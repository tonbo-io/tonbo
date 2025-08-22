use std::{mem::transmute, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema as ArrowSchema};

use super::{record::DynRecord, record_ref::DynRecordRef, ta_adapter::TaDynBuilders};
use crate::record::{ArrowArrays, ArrowArraysBuilder, Record, RecordRef, Schema};

pub struct DynRecordImmutableArrays {
    record_batch: RecordBatch,
}

pub struct DynRecordBuilder {
    adapter: TaDynBuilders,
}

impl ArrowArrays for DynRecordImmutableArrays {
    type Record = DynRecord;
    type Builder = DynRecordBuilder;

    fn builder(schema: Arc<ArrowSchema>, capacity: usize) -> Self::Builder {
        let adapter = TaDynBuilders::new(schema, capacity);
        DynRecordBuilder { adapter }
    }

    fn get(
        &self,
        offset: u32,
        projection_mask: &parquet::arrow::ProjectionMask,
    ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
        if offset as usize >= self.record_batch.num_rows() {
            return None;
        }
        let full_schema = self.record_batch.schema();
        let rr = <DynRecordRef as RecordRef>::from_record_batch(
            &self.record_batch,
            offset as usize,
            projection_mask,
            &full_schema,
        );
        let val = rr.get();
        let val: Option<<Self::Record as Record>::Ref<'_>> = unsafe { transmute(val) };
        Some(val)
    }

    fn as_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }
}

impl ArrowArraysBuilder<DynRecordImmutableArrays> for DynRecordBuilder {
    fn push(
        &mut self,
        key: crate::version::timestamp::Ts<
            <<<DynRecord as Record>::Schema as Schema>::Key as crate::record::Key>::Ref<'_>,
        >,
        row: Option<DynRecordRef>,
    ) {
        let ts: u32 = key.ts().into();
        // Clone ValueRef because DynRecordRef consumes it and we also need to store owned key.
        let key_ref = key.value().clone();
        self.adapter.append(ts, key_ref, row);
    }

    fn written_size(&self) -> usize {
        self.adapter.written_size()
    }

    fn finish(&mut self, indices: Option<&[usize]>) -> DynRecordImmutableArrays {
        let mut batch = std::mem::replace(
            &mut self.adapter,
            TaDynBuilders::new(Arc::new(ArrowSchema::empty()), 0),
        )
        .finish();
        if let Some(idxs) = indices {
            batch = batch.project(idxs).expect("valid projection indices");
        }
        DynRecordImmutableArrays {
            record_batch: batch,
        }
    }
}
