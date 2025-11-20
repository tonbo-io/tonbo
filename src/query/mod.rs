//! Key predicate helpers built on the shared `predicate` crate.
//!
//! `extract_key_ranges` evaluates as much of a `Predicate` tree as possible
//! against a set of key columns and returns both the derived `RangeSet` and a
//! residual predicate describing the portion that could not be pushed into the
//! key-range scan.

pub(crate) mod scan;
pub(crate) mod stream;

use std::{convert::TryFrom, marker::PhantomData};

pub use predicate::{
    ColumnRef, ComparisonOp, Operand, Predicate, PredicateBuilder, PredicateLeaf, ScalarValue,
};
use predicate::{PredicateVisitor, VisitOutcome};

use crate::key::{KeyOwned, KeyRange, RangeSet};

/// Trait describing key types that can be derived from predicate scalar literals.
pub trait KeyPredicateValue: Ord + Clone {
    /// Convert a predicate scalar literal into the key type.
    fn from_scalar(value: &ScalarValue) -> Option<Self>;
}

impl KeyPredicateValue for i32 {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Int64(v) => i32::try_from(*v).ok(),
            ScalarValue::UInt64(v) => i32::try_from(*v).ok(),
            _ => None,
        }
    }
}

impl KeyPredicateValue for i64 {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Int64(v) => Some(*v),
            ScalarValue::UInt64(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }
}

impl KeyPredicateValue for KeyOwned {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Null => None,
            ScalarValue::Boolean(v) => Some((*v).into()),
            ScalarValue::Int64(v) => Some((*v).into()),
            ScalarValue::UInt64(v) => Some((*v).into()),
            ScalarValue::Float64(v) => Some((*v).into()),
            ScalarValue::Utf8(v) => Some(v.as_str().into()),
            ScalarValue::Binary(v) => Some(v.clone().into()),
        }
    }
}

/// Compute the key ranges implied by `predicate` for the provided `key_columns`.
///
/// The returned tuple contains:
/// - The derived `RangeSet`, or `RangeSet::all()` when no portion of the predicate can be evaluated
///   against the supplied key columns.
/// - An optional residual predicate representing the clauses that could not be converted into key
///   ranges.
pub fn extract_key_ranges<K>(
    predicate: &Predicate,
    key_columns: &[ColumnRef],
) -> (RangeSet<K>, Option<Predicate>)
where
    K: KeyPredicateValue,
{
    let mut visitor = RangeSetVisitor::<K>::new(key_columns);
    let outcome = predicate
        .accept(&mut visitor)
        .expect("range set visitor is infallible");
    let residual = outcome.residual.map(Predicate::simplify);
    let range = outcome.value.unwrap_or_else(RangeSet::all);
    (range, residual)
}

struct RangeSetVisitor<'a, K> {
    key_columns: &'a [ColumnRef],
    active: Option<usize>,
    _marker: PhantomData<K>,
}

impl<'a, K> RangeSetVisitor<'a, K> {
    fn new(key_columns: &'a [ColumnRef]) -> Self {
        Self {
            key_columns,
            active: None,
            _marker: PhantomData,
        }
    }

    fn record_column(&mut self, column: &ColumnRef) -> bool {
        let Some(idx) = self
            .key_columns
            .iter()
            .position(|candidate| candidate == column)
        else {
            return false;
        };

        if idx != 0 {
            return false;
        }

        match self.active {
            Some(active) => active == idx,
            None => {
                self.active = Some(idx);
                true
            }
        }
    }

    fn range_from_compare(
        &mut self,
        left: &Operand,
        op: ComparisonOp,
        right: &Operand,
    ) -> Option<RangeSet<K>>
    where
        K: KeyPredicateValue,
    {
        match (left, right) {
            (Operand::Column(column), Operand::Literal(value)) if self.record_column(column) => {
                range_set_from_literal::<K>(op, value)
            }
            (Operand::Literal(value), Operand::Column(column)) if self.record_column(column) => {
                range_set_from_literal::<K>(op.flipped(), value)
            }
            _ => None,
        }
    }

    fn range_from_in_list(
        &mut self,
        expr: &Operand,
        list: &[ScalarValue],
        negated: bool,
    ) -> Option<RangeSet<K>>
    where
        K: KeyPredicateValue,
    {
        let Operand::Column(column) = expr else {
            return None;
        };
        if !self.record_column(column) {
            return None;
        }

        if list.is_empty() {
            return Some(if negated {
                RangeSet::all()
            } else {
                RangeSet::empty()
            });
        }

        let mut range_acc: Option<RangeSet<K>> = None;
        for value in list {
            let eq_range = range_set_from_literal::<K>(ComparisonOp::Equal, value)?;
            range_acc = Some(match range_acc {
                Some(existing) => existing.union(eq_range),
                None => eq_range,
            });
        }

        range_acc.map(|range| if negated { range.complement() } else { range })
    }
}

impl<'a, K> PredicateVisitor for RangeSetVisitor<'a, K>
where
    K: KeyPredicateValue,
{
    type Error = ();
    type Value = RangeSet<K>;

    fn visit_leaf(
        &mut self,
        leaf: &PredicateLeaf,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        let range = match leaf {
            PredicateLeaf::Compare { left, op, right } => self.range_from_compare(left, *op, right),
            PredicateLeaf::InList {
                expr,
                list,
                negated,
            } => self.range_from_in_list(expr, list, *negated),
            PredicateLeaf::IsNull { .. } => None,
        };

        if let Some(range) = range {
            Ok(VisitOutcome::value(range))
        } else {
            Ok(VisitOutcome::residual(Predicate::from_leaf(leaf.clone())))
        }
    }

    fn combine_not(
        &mut self,
        _original: &Predicate,
        child: VisitOutcome<Self::Value>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        match (child.value, child.residual) {
            (Some(value), Some(residual)) => Ok(VisitOutcome {
                value: Some(value.complement()),
                residual: Some(residual.negate()),
            }),
            (Some(value), None) => Ok(VisitOutcome::value(value.complement())),
            (None, Some(residual)) => Ok(VisitOutcome::residual(residual.negate())),
            (None, None) => Ok(VisitOutcome::empty()),
        }
    }

    fn combine_and(
        &mut self,
        _original: &Predicate,
        children: Vec<VisitOutcome<Self::Value>>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        let mut residuals = Vec::new();
        let mut values = Vec::new();
        for child in children {
            if let Some(value) = child.value {
                values.push(value);
            }
            if let Some(residual) = child.residual {
                residuals.push(residual);
            }
        }
        let residual = Predicate::conjunction(residuals);
        let value = values.into_iter().reduce(|acc, range| acc.intersect(range));
        Ok(VisitOutcome { value, residual })
    }

    fn combine_or(
        &mut self,
        original: &Predicate,
        children: Vec<VisitOutcome<Self::Value>>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        let mut values = Vec::new();
        for child in children {
            match (child.value, child.residual) {
                (Some(value), None) => values.push(value),
                _ => return Ok(VisitOutcome::residual(original.clone())),
            }
        }

        let value = values.into_iter().reduce(|acc, range| acc.union(range));
        Ok(VisitOutcome {
            value,
            residual: None,
        })
    }
}

fn range_set_from_literal<K>(op: ComparisonOp, value: &ScalarValue) -> Option<RangeSet<K>>
where
    K: KeyPredicateValue,
{
    use std::ops::Bound as B;

    let key = K::from_scalar(value)?;
    let range = match op {
        ComparisonOp::Equal => single_value_range(key),
        ComparisonOp::NotEqual => single_value_range(key).complement(),
        ComparisonOp::LessThan => range_from_bounds(B::Unbounded, B::Excluded(key)),
        ComparisonOp::LessThanOrEqual => range_from_bounds(B::Unbounded, B::Included(key)),
        ComparisonOp::GreaterThan => range_from_bounds(B::Excluded(key), B::Unbounded),
        ComparisonOp::GreaterThanOrEqual => range_from_bounds(B::Included(key), B::Unbounded),
    };
    Some(range)
}

fn single_value_range<K: Ord + Clone>(value: K) -> RangeSet<K> {
    use std::ops::Bound as B;
    RangeSet::from_ranges(vec![KeyRange::new(
        B::Included(value.clone()),
        B::Included(value),
    )])
}

fn range_from_bounds<K: Ord>(start: std::ops::Bound<K>, end: std::ops::Bound<K>) -> RangeSet<K> {
    RangeSet::from_ranges(vec![KeyRange::new(start, end)])
}
#[cfg(test)]
mod tests {
    use super::*;

    fn eq_pred(value: i64, column: &ColumnRef) -> Predicate {
        PredicateBuilder::leaf()
            .equals(column.clone(), ScalarValue::Int64(value))
            .build()
    }

    fn in_pred(column: &ColumnRef, values: &[i64]) -> Predicate {
        PredicateBuilder::leaf()
            .in_list(
                column.clone(),
                values.iter().cloned().map(ScalarValue::Int64),
            )
            .build()
    }

    fn range_pred(column: &ColumnRef, lower: (bool, i64), upper: (bool, i64)) -> Predicate {
        let lower_builder = if lower.0 {
            PredicateBuilder::leaf()
                .greater_than_or_equal(column.clone(), ScalarValue::Int64(lower.1))
                .build()
        } else {
            PredicateBuilder::leaf()
                .greater_than(column.clone(), ScalarValue::Int64(lower.1))
                .build()
        };
        let upper_builder = if upper.0 {
            PredicateBuilder::leaf()
                .less_than_or_equal(column.clone(), ScalarValue::Int64(upper.1))
                .build()
        } else {
            PredicateBuilder::leaf()
                .less_than(column.clone(), ScalarValue::Int64(upper.1))
                .build()
        };

        PredicateBuilder::and()
            .predicate(lower_builder)
            .predicate(upper_builder)
            .build()
    }

    fn contains(range_set: &RangeSet<i32>, value: i32) -> bool {
        range_set.iter().any(|range| {
            let (start, end) = range.as_borrowed_bounds();
            bounds_contains(start, end, value)
        })
    }

    fn bounds_contains(
        start: std::ops::Bound<&i32>,
        end: std::ops::Bound<&i32>,
        value: i32,
    ) -> bool {
        use std::ops::Bound as B;
        match (start, end) {
            (B::Included(sv), B::Included(ev)) => value >= *sv && value <= *ev,
            (B::Included(sv), B::Excluded(ev)) => value >= *sv && value < *ev,
            (B::Excluded(sv), B::Included(ev)) => value > *sv && value <= *ev,
            (B::Excluded(sv), B::Excluded(ev)) => value > *sv && value < *ev,
            (B::Unbounded, B::Included(ev)) => value <= *ev,
            (B::Unbounded, B::Excluded(ev)) => value < *ev,
            (B::Included(sv), B::Unbounded) => value >= *sv,
            (B::Excluded(sv), B::Unbounded) => value > *sv,
            (B::Unbounded, B::Unbounded) => true,
        }
    }

    #[test]
    fn eq_and_in_ranges() {
        let key_col = ColumnRef::new("k", None);
        let key_cols = [key_col.clone()];
        let (eq_ranges, residual) = extract_key_ranges::<i32>(&eq_pred(5, &key_col), &key_cols);
        assert!(residual.is_none());
        assert!(contains(&eq_ranges, 5));
        assert!(!contains(&eq_ranges, 4));

        let (in_ranges, residual) =
            extract_key_ranges::<i32>(&in_pred(&key_col, &[1, 3, 5]), &key_cols);
        assert!(residual.is_none());
        assert!(contains(&in_ranges, 1));
        assert!(contains(&in_ranges, 3));
        assert!(!contains(&in_ranges, 2));

        let (range_set, residual) =
            extract_key_ranges::<i32>(&range_pred(&key_col, (true, 3), (false, 7)), &key_cols);
        assert!(residual.is_none());
        assert!(contains(&range_set, 3));
        assert!(contains(&range_set, 6));
        assert!(!contains(&range_set, 7));
    }

    #[test]
    fn range_extract_and_intersect() {
        let key_col = ColumnRef::new("k", None);
        let key_cols = [key_col.clone()];
        let cond = PredicateBuilder::and()
            .predicate(range_pred(&key_col, (true, 3), (false, 10)))
            .predicate(eq_pred(7, &key_col))
            .build();

        let (ranges, residual) = extract_key_ranges::<i32>(&cond, &key_cols);
        assert!(residual.is_none());
        assert!(contains(&ranges, 7));
        assert!(!contains(&ranges, 3));
    }

    #[test]
    fn empty_in_list() {
        let key_col = ColumnRef::new("k", None);
        let empty_in = PredicateBuilder::leaf()
            .in_list(key_col.clone(), std::iter::empty::<ScalarValue>())
            .build();

        let (ranges, residual) = extract_key_ranges::<i32>(&empty_in, &[key_col]);
        assert!(residual.is_none());
        assert!(ranges.is_empty());
    }

    #[test]
    fn not_variants_to_ranges() {
        let key_col = ColumnRef::new("k", None);
        let key_cols = [key_col.clone()];
        let not_eq = PredicateBuilder::leaf()
            .not_group(|builder| builder.equals(key_col.clone(), ScalarValue::Int64(5)))
            .build();
        let (ranges, residual) = extract_key_ranges::<i32>(&not_eq, &key_cols);
        assert!(residual.is_none());
        assert!(contains(&ranges, 4));
        assert!(!contains(&ranges, 5));

        let not_range = PredicateBuilder::leaf()
            .not_group(|builder| {
                builder
                    .greater_than_or_equal(key_col.clone(), ScalarValue::Int64(3))
                    .less_than(key_col.clone(), ScalarValue::Int64(7))
            })
            .build();
        let (ranges, residual) = extract_key_ranges::<i32>(&not_range, &key_cols);
        assert!(residual.is_none());
        assert!(contains(&ranges, 2));
        assert!(contains(&ranges, 7));
        assert!(!contains(&ranges, 4));
    }

    #[test]
    fn residual_retains_non_key_clauses() {
        let key_col = ColumnRef::new("k", None);
        let key_cols = [key_col.clone()];
        let value_col = ColumnRef::new("v", None);
        // Predicate shape:
        // ( (k > 15 AND v = "b") AND k < 25 )
        // Only the key clauses contribute to the range set; the value comparison is emitted as
        // residual.
        let value_pred = PredicateBuilder::leaf()
            .equals(value_col.clone(), ScalarValue::Utf8("b".into()))
            .build();
        let predicate = PredicateBuilder::and()
            .predicate(
                PredicateBuilder::and()
                    .predicate(
                        PredicateBuilder::leaf()
                            .greater_than(key_col.clone(), ScalarValue::Int64(15))
                            .build(),
                    )
                    .predicate(value_pred.clone())
                    .build(),
            )
            .predicate(
                PredicateBuilder::leaf()
                    .less_than(key_col.clone(), ScalarValue::Int64(25))
                    .build(),
            )
            .build();

        let (ranges, residual) = extract_key_ranges::<i32>(&predicate, &key_cols);
        assert!(contains(&ranges, 20));
        assert!(!contains(&ranges, 15));
        let residual = residual.expect("residual predicate");
        assert_eq!(residual, value_pred);
    }

    #[test]
    fn not_nested_predicate_carries_value_residual() {
        let key_col = ColumnRef::new("k", Some(0));
        let value_col = ColumnRef::new("v", Some(1));
        let key_cols = [key_col.clone(), value_col.clone()];

        // Build the inner clauses that mix key and value columns so we can wrap them in a NOT.
        let greater_than_left = PredicateBuilder::and()
            .predicate(
                PredicateBuilder::leaf()
                    .greater_than(key_col.clone(), ScalarValue::Int64(15))
                    .build(),
            )
            .predicate(
                PredicateBuilder::leaf()
                    .greater_than(value_col.clone(), ScalarValue::Utf8("b".into()))
                    .build(),
            )
            .build();

        let less_than_right = PredicateBuilder::and()
            .predicate(
                PredicateBuilder::leaf()
                    .less_than(key_col.clone(), ScalarValue::Int64(25))
                    .build(),
            )
            .predicate(
                PredicateBuilder::leaf()
                    .less_than(value_col.clone(), ScalarValue::Utf8("e".into()))
                    .build(),
            )
            .build();

        let predicate = PredicateBuilder::and()
            .predicate(
                // Predicate: NOT((k>15 ∧ v>'b') ∧ (k<25 ∧ v<'e')) ∧ (v>'a')
                // In words: reject tuples that simultaneously sit inside both the key window
                // (15,25) and the value window ('b','e'), but still require v >
                // 'a'.
                PredicateBuilder::leaf()
                    .not_group(|builder| {
                        builder
                            .predicate(greater_than_left)
                            .predicate(less_than_right)
                    })
                    .build(),
            )
            .predicate(
                PredicateBuilder::leaf()
                    .greater_than(value_col.clone(), ScalarValue::Utf8("a".into()))
                    .build(),
            )
            .build();

        // Expect the key range to exclude (15,25) while the residual keeps the value-only clauses.
        let (ranges, residual) = extract_key_ranges::<i32>(&predicate, &key_cols);
        assert!(contains(&ranges, 10));
        assert!(contains(&ranges, 15));
        assert!(!contains(&ranges, 16), "ranges: {:?}", ranges.as_slice());
        assert!(!contains(&ranges, 20), "ranges: {:?}", ranges.as_slice());
        assert!(contains(&ranges, 25));
        assert!(contains(&ranges, 30));

        // Residual should express `(v <= 'b' OR v >= 'e') AND v > 'a'`.
        let residual = residual.expect("residual predicate");
        let upper_or_lower = PredicateBuilder::or()
            .predicate(
                PredicateBuilder::leaf()
                    .less_than_or_equal(value_col.clone(), ScalarValue::Utf8("b".into()))
                    .build(),
            )
            .predicate(
                PredicateBuilder::leaf()
                    .greater_than_or_equal(value_col.clone(), ScalarValue::Utf8("e".into()))
                    .build(),
            )
            .build();
        let expected_residual = PredicateBuilder::and()
            .predicate(upper_or_lower)
            .predicate(
                PredicateBuilder::leaf()
                    .greater_than(value_col.clone(), ScalarValue::Utf8("a".into()))
                    .build(),
            )
            .build()
            .simplify();
        assert_eq!(residual, expected_residual);
    }
}
