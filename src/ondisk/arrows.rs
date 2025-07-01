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
    record::{Key, Record},
    timestamp::Timestamp,
};

unsafe fn get_range_bound_fn<R>(
    range: Bound<&R::Key>,
) -> (
    Option<&'static R::Key>,
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
    range: (Bound<&R::Key>, Bound<&R::Key>),
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
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                lower_cmp(record_batch.column(0), lower_key.to_arrow_datum().as_ref())
            },
        )));
    }
    if let Some(upper_key) = upper_key {
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                upper_cmp(upper_key.to_arrow_datum().as_ref(), record_batch.column(0))
            },
        )));
    }

    RowFilter::new(predictions)
}
