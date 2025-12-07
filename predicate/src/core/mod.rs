//! Core predicate structures shared across Tonbo adapters.
//!
//! Everything here is built on Arrow dynamic cells (`typed-arrow-dyn`). The
//! intent is to keep predicate construction and evaluation Arrow-native rather
//! than storage- or layout-agnostic.

mod builder;
mod node;
mod operand;
mod row_set;
mod value;
mod visitor;

pub use node::{ComparisonOp, Predicate, PredicateNode};
pub use operand::{ColumnRef, Operand};
pub use row_set::{BitmapRowSet, RowId, RowIdIter, RowSet};
pub use value::{ScalarValue, ScalarValueRef};
pub use visitor::{PredicateVisitor, VisitOutcome};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct SampleRow {
        id: RowId,
        a: Option<i64>,
        b: Option<i64>,
    }

    fn sample_rows() -> Vec<SampleRow> {
        vec![
            SampleRow {
                id: 0,
                a: Some(2),
                b: Some(2),
            },
            SampleRow {
                id: 1,
                a: Some(5),
                b: Some(1),
            },
            SampleRow {
                id: 2,
                a: None,
                b: Some(2),
            },
            SampleRow {
                id: 3,
                a: Some(4),
                b: Some(3),
            },
        ]
    }

    fn sample_predicate() -> Predicate {
        Predicate::and(vec![
            Predicate::gt(ColumnRef::new("a", None), ScalarValue::from(1i64)),
            Predicate::or(vec![
                Predicate::eq(ColumnRef::new("b", None), ScalarValue::from(2i64)),
                Predicate::eq(ColumnRef::new("b", None), ScalarValue::from(3i64)),
            ]),
            Predicate::is_null(ColumnRef::new("a", None)).not(),
        ])
    }

    fn universe_from_rows(rows: &[SampleRow]) -> BitmapRowSet {
        let mut set = BitmapRowSet::new();
        for row in rows {
            set.insert(row.id);
        }
        set
    }

    fn collect_row_ids(rowset: &BitmapRowSet) -> Vec<RowId> {
        rowset.iter().collect()
    }

    fn combine_row_sets<F, R>(
        children: Vec<VisitOutcome<BitmapRowSet>>,
        reducer: F,
        residual_builder: R,
    ) -> VisitOutcome<BitmapRowSet>
    where
        F: Fn(BitmapRowSet, BitmapRowSet) -> BitmapRowSet,
        R: Fn(Vec<Predicate>) -> Option<Predicate>,
    {
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
        let residual = residual_builder(residuals);
        let value = values.into_iter().reduce(reducer);
        VisitOutcome { value, residual }
    }

    struct ComparisonVisitor {
        rows: Vec<SampleRow>,
    }

    impl ComparisonVisitor {
        fn new(rows: Vec<SampleRow>) -> Self {
            Self { rows }
        }

        fn evaluate_compare(
            &self,
            left: &Operand,
            op: ComparisonOp,
            right: &Operand,
        ) -> BitmapRowSet {
            let mut result = BitmapRowSet::new();
            for row in &self.rows {
                match (
                    self.resolve_operand(left, row),
                    self.resolve_operand(right, row),
                ) {
                    (Some(Some(lhs)), Some(Some(rhs))) => {
                        if Self::compare_i64(lhs, rhs, op) {
                            result.insert(row.id);
                        }
                    }
                    _ => {}
                }
            }
            result
        }

        fn evaluate_in_list(
            &self,
            expr: &Operand,
            list: &[ScalarValue],
            negated: bool,
        ) -> BitmapRowSet {
            let normalized: Vec<Option<i64>> = list
                .iter()
                .filter_map(|value| {
                    let view = value.as_ref();
                    if view.is_null() {
                        return Some(None);
                    }
                    view.as_int_i128()
                        .and_then(|v| i64::try_from(v).ok())
                        .map(Some)
                })
                .collect();

            let mut result = BitmapRowSet::new();
            for row in &self.rows {
                if let Some(value) = self.resolve_operand(expr, row) {
                    let contains = normalized.iter().any(|candidate| candidate == &value);
                    let matches = if negated { !contains } else { contains };
                    if matches {
                        result.insert(row.id);
                    }
                }
            }
            result
        }

        fn evaluate_is_null(&self, expr: &Operand, negated: bool) -> BitmapRowSet {
            let mut result = BitmapRowSet::new();
            for row in &self.rows {
                match self.resolve_operand(expr, row) {
                    Some(None) if !negated => result.insert(row.id),
                    Some(Some(_)) if negated => result.insert(row.id),
                    _ => {}
                }
            }
            result
        }

        fn resolve_operand(&self, operand: &Operand, row: &SampleRow) -> Option<Option<i64>> {
            match operand {
                Operand::Column(column) => match column.name.as_ref() {
                    "a" => Some(row.a),
                    "b" => Some(row.b),
                    _ => None,
                },
                Operand::Literal(value) => {
                    let view = value.as_ref();
                    if view.is_null() {
                        return Some(None);
                    }
                    view.as_int_i128()
                        .and_then(|v| i64::try_from(v).ok())
                        .map(Some)
                }
            }
        }

        fn compare_i64(left: i64, right: i64, op: ComparisonOp) -> bool {
            match op {
                ComparisonOp::Equal => left == right,
                ComparisonOp::NotEqual => left != right,
                ComparisonOp::LessThan => left < right,
                ComparisonOp::LessThanOrEqual => left <= right,
                ComparisonOp::GreaterThan => left > right,
                ComparisonOp::GreaterThanOrEqual => left >= right,
            }
        }
    }

    impl ComparisonVisitor {
        fn universe(&self) -> BitmapRowSet {
            universe_from_rows(&self.rows)
        }
    }

    impl PredicateVisitor for ComparisonVisitor {
        type Error = ();
        type Value = BitmapRowSet;

        fn visit_leaf(
            &mut self,
            leaf: &PredicateNode,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            let row_set = match leaf {
                PredicateNode::True => self.universe(),
                PredicateNode::Compare { left, op, right } => {
                    self.evaluate_compare(left, *op, right)
                }
                PredicateNode::InList {
                    expr,
                    list,
                    negated,
                } => self.evaluate_in_list(expr, list, *negated),
                PredicateNode::IsNull { expr, negated } => self.evaluate_is_null(expr, *negated),
                PredicateNode::Not(_) | PredicateNode::And(_) | PredicateNode::Or(_) => {
                    unreachable!("visit_leaf only accepts terminal nodes")
                }
            };
            Ok(VisitOutcome::value(row_set))
        }

        fn combine_not(
            &mut self,
            _original: &Predicate,
            child: VisitOutcome<Self::Value>,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            if let Some(residual) = child.residual {
                Ok(VisitOutcome::residual(residual.negate()))
            } else if let Some(value) = child.value {
                let complement = self.universe().difference(&value);
                Ok(VisitOutcome::value(complement))
            } else {
                Ok(VisitOutcome::empty())
            }
        }

        fn combine_and(
            &mut self,
            _original: &Predicate,
            children: Vec<VisitOutcome<Self::Value>>,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            Ok(combine_row_sets(
                children,
                |mut acc, value| {
                    acc = acc.intersect(&value);
                    acc
                },
                Predicate::conjunction,
            ))
        }

        fn combine_or(
            &mut self,
            _original: &Predicate,
            children: Vec<VisitOutcome<Self::Value>>,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            Ok(combine_row_sets(
                children,
                |mut acc, value| {
                    acc = acc.union(&value);
                    acc
                },
                Predicate::disjunction,
            ))
        }
    }

    struct AllRowsVisitor {
        all_rows: BitmapRowSet,
    }

    impl AllRowsVisitor {
        fn new(all_rows: BitmapRowSet) -> Self {
            Self { all_rows }
        }
    }

    impl PredicateVisitor for AllRowsVisitor {
        type Error = ();
        type Value = BitmapRowSet;

        fn visit_leaf(
            &mut self,
            leaf: &PredicateNode,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            debug_assert!(leaf.is_leaf(), "AllRowsVisitor expects leaf nodes");
            Ok(VisitOutcome::value(self.all_rows.clone()))
        }

        fn combine_not(
            &mut self,
            _original: &Predicate,
            child: VisitOutcome<Self::Value>,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            if let Some(residual) = child.residual {
                Ok(VisitOutcome::residual(residual.negate()))
            } else if let Some(value) = child.value {
                let complement = self.all_rows.difference(&value);
                Ok(VisitOutcome::value(complement))
            } else {
                Ok(VisitOutcome::empty())
            }
        }

        fn combine_and(
            &mut self,
            _original: &Predicate,
            children: Vec<VisitOutcome<Self::Value>>,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            Ok(combine_row_sets(
                children,
                |mut acc, value| {
                    acc = acc.intersect(&value);
                    acc
                },
                Predicate::conjunction,
            ))
        }

        fn combine_or(
            &mut self,
            _original: &Predicate,
            children: Vec<VisitOutcome<Self::Value>>,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
            Ok(combine_row_sets(
                children,
                |mut acc, value| {
                    acc = acc.union(&value);
                    acc
                },
                Predicate::disjunction,
            ))
        }
    }

    #[test]
    fn predicate_visitors_share_traversal_logic() {
        let predicate = sample_predicate();
        let rows = sample_rows();

        let mut comparison = ComparisonVisitor::new(rows.clone());
        let comparison_outcome = predicate
            .accept(&mut comparison)
            .expect("comparison visitor succeeds");
        assert!(comparison_outcome.residual.is_none());
        let comparison_result = comparison_outcome
            .value
            .expect("comparison visitor yields row set");
        assert_eq!(collect_row_ids(&comparison_result), vec![0, 3]);

        let mut all_rows = AllRowsVisitor::new(universe_from_rows(&rows));
        let all_rows_outcome = predicate
            .accept(&mut all_rows)
            .expect("all rows visitor succeeds");
        assert!(all_rows_outcome.residual.is_none());
        let all_rows_result = all_rows_outcome
            .value
            .expect("all rows visitor yields row set");
        assert!(all_rows_result.is_empty());
    }
}
