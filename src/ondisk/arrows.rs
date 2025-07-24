use std::ops::Bound;

use arrow::{
    array::{BooleanArray, Datum},
    buffer::BooleanBuffer,
    compute::kernels::cmp::{gt, gt_eq, lt_eq},
    error::ArrowError,
};
use parquet::{
    arrow::{
        arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter},
        ProjectionMask,
    },
    schema::types::SchemaDescriptor,
};

use crate::{
    record::{Key, Record, Schema},
    version::timestamp::Timestamp,
};

unsafe fn get_range_bound_fn<R>(
    range: Bound<&<R::Schema as Schema>::Key>,
) -> (
    Option<&'static <R::Schema as Schema>::Key>,
    &'static (dyn Fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError> + Sync),
)
where
    R: Record,
{
    let cmp: &'static (dyn Fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError> + Sync);
    let key = match range {
        Bound::Included(key) => {
            cmp = &gt_eq;
            Some(&*(key as *const _))
        }
        Bound::Excluded(key) => {
            cmp = &gt;
            Some(&*(key as *const _))
        }
        Bound::Unbounded => {
            cmp = &|this, _| {
                let len = this.get().0.len();
                Ok(BooleanArray::new(
                    BooleanBuffer::collect_bool(len, |_| true),
                    None,
                ))
            };
            None
        }
    };
    (key, cmp)
}

pub(crate) unsafe fn get_range_filter<R>(
    schema_descriptor: &SchemaDescriptor,
    range: (
        Bound<&<R::Schema as Schema>::Key>,
        Bound<&<R::Schema as Schema>::Key>,
    ),
    ts: Timestamp,
) -> RowFilter
where
    R: Record,
{
    let (lower_key, lower_cmp) = get_range_bound_fn::<R>(range.0);
    let (upper_key, upper_cmp) = get_range_bound_fn::<R>(range.1);

    let mut predictions: Vec<Box<dyn ArrowPredicate>> = vec![Box::new(ArrowPredicateFn::new(
        ProjectionMask::roots(schema_descriptor, [1]),
        move |record_batch| lt_eq(record_batch.column(0), &ts.to_arrow_scalar() as &dyn Datum),
    ))];
    if let Some(lower_key) = lower_key {
        // For now, we'll temporarily go back to single key filtering
        // TODO: Implement proper composite key filtering
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                let key_fields = lower_key.to_arrow_fields();
                if key_fields.is_empty() {
                    Ok(BooleanArray::new(
                        BooleanBuffer::new_set(record_batch.num_rows()),
                        None,
                    ))
                } else {
                    lower_cmp(record_batch.column(0), key_fields[0].as_ref())
                }
            },
        )));
    }
    if let Some(upper_key) = upper_key {
        // For now, we'll temporarily go back to single key filtering
        // TODO: Implement proper composite key filtering
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                let key_fields = upper_key.to_arrow_fields();
                if key_fields.is_empty() {
                    Ok(BooleanArray::new(
                        BooleanBuffer::new_set(record_batch.num_rows()),
                        None,
                    ))
                } else {
                    upper_cmp(key_fields[0].as_ref(), record_batch.column(0))
                }
            },
        )));
    }

    RowFilter::new(predictions)
}
