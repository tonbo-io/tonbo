use std::ops::Bound;

use arrow::{
    array::{BooleanArray, Datum},
    buffer::BooleanBuffer,
    compute::kernels::cmp::{gt, gt_eq, lt_eq},
    error::ArrowError,
};
use common::{PrimaryKey, Value};
use parquet::{
    arrow::{
        arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter},
        ProjectionMask,
    },
    schema::types::SchemaDescriptor,
};

use crate::timestamp::Timestamp;

unsafe fn get_range_bound_fn(
    range: Bound<&dyn Value>,
) -> (
    Option<&'static dyn Value>,
    &'static (dyn Fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError> + Sync),
) {
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

pub(crate) unsafe fn get_range_filter(
    schema_descriptor: &SchemaDescriptor,
    range: (Bound<&dyn Value>, Bound<&dyn Value>),
    ts: Timestamp,
) -> RowFilter {
    let (lower_key, lower_cmp) = get_range_bound_fn(range.0);
    let (upper_key, upper_cmp) = get_range_bound_fn(range.1);

    let mut predictions: Vec<Box<dyn ArrowPredicate>> = vec![Box::new(ArrowPredicateFn::new(
        ProjectionMask::roots(schema_descriptor, [1]),
        move |record_batch| lt_eq(record_batch.column(0), &ts.to_arrow_scalar() as &dyn Datum),
    ))];
    if let Some(lower_key) = lower_key {
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                let pk = PrimaryKey::new(vec![lower_key.clone_arc()]);
                lower_cmp(record_batch.column(0), pk.arrow_datum(0).unwrap().as_ref())
            },
        )));
    }
    if let Some(upper_key) = upper_key {
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, [2]),
            move |record_batch| {
                let pk = PrimaryKey::new(vec![upper_key.clone_arc()]);
                upper_cmp(pk.arrow_datum(0).unwrap().as_ref(), record_batch.column(0))
            },
        )));
    }

    RowFilter::new(predictions)
}
