use std::ops::Bound;

use aisle::{
    filter::{Filter, RowFilter},
    ord::{gt, gt_eq, inf, lt_eq},
    predicate::{AislePredicate, AislePredicateFn},
};
use arrow::{array::Datum, error::ArrowError};
use parquet::{arrow::ProjectionMask, schema::types::SchemaDescriptor};

use crate::{
    record::{Key, Record, Schema},
    timestamp::Timestamp,
};

unsafe fn get_range_bound_fn<R>(
    range: Bound<&<R::Schema as Schema>::Key>,
) -> (
    Option<&'static <R::Schema as Schema>::Key>,
    &'static (dyn Fn(&dyn Datum, &dyn Datum) -> Result<Filter, ArrowError> + Sync),
)
where
    R: Record,
{
    let cmp: &'static (dyn Fn(&dyn Datum, &dyn Datum) -> Result<Filter, ArrowError> + Sync);
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
            cmp = &|this, _| inf(this);
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

    let mut predictions: Vec<Box<dyn AislePredicate>> = vec![Box::new(AislePredicateFn::new(
        ProjectionMask::roots(schema_descriptor, [1]),
        move |record_batch| lt_eq(record_batch.column(0), &ts.to_arrow_scalar() as &dyn Datum),
    ))];
    if let Some(lower_key) = lower_key {
        predictions.push(Box::new(AislePredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                lower_cmp(record_batch.column(0), lower_key.to_arrow_datum().as_ref())
            },
        )));
    }
    if let Some(upper_key) = upper_key {
        predictions.push(Box::new(AislePredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                upper_cmp(upper_key.to_arrow_datum().as_ref(), record_batch.column(0))
            },
        )));
    }

    RowFilter::new(predictions)
}
