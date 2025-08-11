use std::ops::Bound;

use arrow::{
    array::{BooleanArray, Datum},
    compute::kernels::{
        boolean::{and_kleene, or_kleene},
        cmp::{eq, gt, gt_eq, lt, lt_eq},
    },
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

enum BoundKind {
    Lower { inclusive: bool },
    Upper { inclusive: bool },
}

unsafe fn get_lower_bound<R>(
    range: Bound<&<R::Schema as Schema>::Key>,
) -> (Option<&'static <R::Schema as Schema>::Key>, BoundKind)
where
    R: Record,
{
    match range {
        Bound::Included(key) => (
            Some(&*(key as *const _)),
            BoundKind::Lower { inclusive: true },
        ),
        Bound::Excluded(key) => (
            Some(&*(key as *const _)),
            BoundKind::Lower { inclusive: false },
        ),
        Bound::Unbounded => (None, BoundKind::Lower { inclusive: true }),
    }
}

unsafe fn get_upper_bound<R>(
    range: Bound<&<R::Schema as Schema>::Key>,
) -> (Option<&'static <R::Schema as Schema>::Key>, BoundKind)
where
    R: Record,
{
    match range {
        Bound::Included(key) => (
            Some(&*(key as *const _)),
            BoundKind::Upper { inclusive: true },
        ),
        Bound::Excluded(key) => (
            Some(&*(key as *const _)),
            BoundKind::Upper { inclusive: false },
        ),
        Bound::Unbounded => (None, BoundKind::Upper { inclusive: true }),
    }
}

pub(crate) unsafe fn get_range_filter<R>(
    schema_descriptor: &SchemaDescriptor,
    range: (
        Bound<&<R::Schema as Schema>::Key>,
        Bound<&<R::Schema as Schema>::Key>,
    ),
    ts: Timestamp,
    pk_indices: &[usize],
) -> RowFilter
where
    R: Record,
{
    let (lower_key, lower_kind) = get_lower_bound::<R>(range.0);
    let (upper_key, upper_kind) = get_upper_bound::<R>(range.1);

    let mut predictions: Vec<Box<dyn ArrowPredicate>> = vec![Box::new(ArrowPredicateFn::new(
        ProjectionMask::roots(schema_descriptor, [1]),
        move |record_batch| lt_eq(record_batch.column(0), &ts.to_arrow_scalar() as &dyn Datum),
    ))];
    if let Some(lower_key) = lower_key {
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, pk_indices.to_vec()),
            move |record_batch| {
                let datums = lower_key.to_arrow_datums();
                let n = datums.len();
                let mut acc: Option<BooleanArray> = None;
                for i in 0..n {
                    let cmp_i = if i == n - 1 {
                        match lower_kind {
                            BoundKind::Lower { inclusive: true } => {
                                gt_eq(record_batch.column(i), datums[i].as_ref())?
                            }
                            BoundKind::Lower { inclusive: false } => {
                                gt(record_batch.column(i), datums[i].as_ref())?
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        gt(record_batch.column(i), datums[i].as_ref())?
                    };
                    let mut term = cmp_i;
                    for (j, d) in datums.iter().enumerate().take(i) {
                        let eq_j = eq(record_batch.column(j), d.as_ref())?;
                        term = and_kleene(&term, &eq_j)?;
                    }
                    acc = Some(match acc {
                        None => term,
                        Some(prev) => or_kleene(&prev, &term)?,
                    });
                }
                Ok(acc.expect("at least one key component"))
            },
        )));
    }
    if let Some(upper_key) = upper_key {
        let pk_indices = pk_indices.to_vec();
        predictions.push(Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(schema_descriptor, pk_indices),
            move |record_batch| {
                let datums = upper_key.to_arrow_datums();
                let n = datums.len();
                let mut acc: Option<BooleanArray> = None;
                for i in 0..n {
                    let cmp_i = if i == n - 1 {
                        match upper_kind {
                            BoundKind::Upper { inclusive: true } => {
                                lt_eq(record_batch.column(i), datums[i].as_ref())?
                            }
                            BoundKind::Upper { inclusive: false } => {
                                lt(record_batch.column(i), datums[i].as_ref())?
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        lt(record_batch.column(i), datums[i].as_ref())?
                    };
                    let mut term = cmp_i;
                    for (j, d) in datums.iter().enumerate().take(i) {
                        let eq_j = eq(record_batch.column(j), d.as_ref())?;
                        term = and_kleene(&term, &eq_j)?;
                    }
                    acc = Some(match acc {
                        None => term,
                        Some(prev) => or_kleene(&prev, &term)?,
                    });
                }
                Ok(acc.expect("at least one key component"))
            },
        )));
    }

    RowFilter::new(predictions)
}
