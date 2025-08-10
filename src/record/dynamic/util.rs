use arrow::array::RecordBatch;

use crate::{record::{DynRecord, DynRecordImmutableArrays, DynSchema, Record, Schema}, ArrowArrays, ArrowArraysBuilder, Ts};

// Converts an iterator over `DynRecord` into record batch
pub fn records_to_record_batch<I>(
    schema: &DynSchema,
    rows: I,
) -> RecordBatch
where
    I: IntoIterator<Item = (u32, DynRecord)>,
{
    let rows_vec: Vec<(u32, DynRecord)> = rows.into_iter().collect();
    let mut builder = DynRecordImmutableArrays::builder(
        schema.arrow_schema().clone(),
        rows_vec.len(),
    );

    for (ts, rec) in rows_vec {
        let key = Ts { ts: ts.into(), value: rec.key() };
        builder.push(key, Some(rec.as_record_ref()));
    }

    let arrays = builder.finish(None);
    arrays.as_record_batch().clone()
}