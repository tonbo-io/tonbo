#![deny(missing_docs)]
//! Core predicate structures shared across Tonbo adapters.

mod builder;
mod row_set;

use std::{fmt, sync::Arc};

pub use builder::PredicateBuilder;
pub use row_set::{BitmapRowSet, RowId, RowIdIter, RowSet};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Literal values accepted by predicate operands.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ScalarValue {
    /// Represents SQL/Arrow `NULL`.
    Null,
    /// Boolean literal.
    Boolean(bool),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Binary blob.
    Binary(Vec<u8>),
}

impl ScalarValue {
    /// Returns true when the literal is the `Null` variant.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }
}

/// Reference identifying a column used inside predicates.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ColumnRef {
    /// Optional ordinal of the column within the projected schema.
    pub index: Option<usize>,
    /// Canonical column name.
    pub name: Arc<str>,
}

impl ColumnRef {
    /// Creates a new column reference from a name and optional index.
    #[must_use]
    pub fn new<N>(name: N, index: Option<usize>) -> Self
    where
        N: Into<Arc<str>>,
    {
        Self {
            name: name.into(),
            index,
        }
    }
}

/// Operand used by predicate comparisons and function calls.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Operand {
    /// Reference to a column.
    Column(ColumnRef),
    /// Literal value.
    Literal(ScalarValue),
}

impl From<ColumnRef> for Operand {
    fn from(value: ColumnRef) -> Self {
        Self::Column(value)
    }
}

impl From<ScalarValue> for Operand {
    fn from(value: ScalarValue) -> Self {
        Self::Literal(value)
    }
}

/// Comparison operator used by binary predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ComparisonOp {
    /// Equals (`=`).
    Equal,
    /// Not equals (`!=`).
    NotEqual,
    /// Less than (`<`).
    LessThan,
    /// Less than or equal to (`<=`).
    LessThanOrEqual,
    /// Greater than (`>`).
    GreaterThan,
    /// Greater than or equal to (`>=`).
    GreaterThanOrEqual,
}

impl ComparisonOp {
    /// Returns a textual representation of the operator.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::LessThan => "<",
            ComparisonOp::LessThanOrEqual => "<=",
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::GreaterThanOrEqual => ">=",
        }
    }

    /// Returns the operator that swaps the left/right side of the comparison.
    #[must_use]
    pub fn flipped(self) -> Self {
        match self {
            ComparisonOp::Equal => ComparisonOp::Equal,
            ComparisonOp::NotEqual => ComparisonOp::NotEqual,
            ComparisonOp::LessThan => ComparisonOp::GreaterThan,
            ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            ComparisonOp::GreaterThan => ComparisonOp::LessThan,
            ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
        }
    }
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Logical predicate shared across adapters and Tonbo's core.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Predicate {
    kind: PredicateNode,
}

impl Predicate {
    /// Returns a reference to the underlying node.
    #[must_use]
    pub fn kind(&self) -> &PredicateNode {
        &self.kind
    }

    /// Builds a conjunction from the supplied clauses.
    ///
    /// # Panics
    ///
    /// Panics if no clauses are provided.
    #[must_use]
    fn make_and<I>(clauses: I) -> Self
    where
        I: IntoIterator<Item = Predicate>,
    {
        let mut acc = Vec::new();
        for clause in clauses {
            match clause.into_kind() {
                PredicateNode::Inner(PredicateInner::And(mut nested)) => acc.append(&mut nested),
                other => acc.push(Predicate::from_kind(other)),
            }
        }

        assert!(
            !acc.is_empty(),
            "Predicate::make_and requires at least one clause"
        );

        if acc.len() == 1 {
            acc.pop().expect("length checked")
        } else {
            Self::from_kind(PredicateNode::Inner(PredicateInner::And(acc)))
        }
    }

    /// Builds a disjunction from the supplied clauses.
    ///
    /// # Panics
    ///
    /// Panics if no clauses are provided.
    #[must_use]
    fn make_or<I>(clauses: I) -> Self
    where
        I: IntoIterator<Item = Predicate>,
    {
        let mut acc = Vec::new();
        for clause in clauses {
            match clause.into_kind() {
                PredicateNode::Inner(PredicateInner::Or(mut nested)) => acc.append(&mut nested),
                other => acc.push(Predicate::from_kind(other)),
            }
        }

        assert!(
            !acc.is_empty(),
            "Predicate::make_or requires at least one clause"
        );

        if acc.len() == 1 {
            acc.pop().expect("length checked")
        } else {
            Self::from_kind(PredicateNode::Inner(PredicateInner::Or(acc)))
        }
    }

    /// Applies simple simplification rules to reduce nesting.
    #[must_use]
    pub fn simplify(self) -> Self {
        match self.kind {
            PredicateNode::Leaf(_) => self,
            PredicateNode::Inner(PredicateInner::Not(inner)) => {
                let simplified_child = inner.simplify();
                match simplified_child.into_kind() {
                    PredicateNode::Inner(PredicateInner::Not(grandchild)) => *grandchild,
                    other => Self::from_kind(PredicateNode::Inner(PredicateInner::Not(Box::new(
                        Self::from_kind(other),
                    )))),
                }
            }
            PredicateNode::Inner(PredicateInner::And(clauses)) => {
                Predicate::make_and(clauses.into_iter().map(Predicate::simplify))
            }
            PredicateNode::Inner(PredicateInner::Or(clauses)) => {
                Predicate::make_or(clauses.into_iter().map(Predicate::simplify))
            }
        }
    }

    /// Accepts a visitor that walks the predicate tree bottom-up.
    pub fn accept<V>(&self, visitor: &mut V) -> Result<V::RowSetImpl, V::Error>
    where
        V: PredicateVisitor + ?Sized,
    {
        visitor.visit_predicate(self)
    }

    fn from_kind(kind: PredicateNode) -> Self {
        Self { kind }
    }

    fn into_kind(self) -> PredicateNode {
        self.kind
    }
}

/// Categorises a predicate node as leaf or branch.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PredicateNode {
    /// Leaf predicates without child expressions.
    Leaf(PredicateLeaf),
    /// Branch predicates with one or more child predicates.
    Inner(PredicateInner),
}

/// Leaf predicates encode terminal expressions with no child nodes.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PredicateLeaf {
    /// Binary comparison.
    Compare {
        /// Left operand.
        left: Operand,
        /// Operator.
        op: ComparisonOp,
        /// Right operand.
        right: Operand,
    },
    /// Membership test against a literal list.
    InList {
        /// Value to test.
        expr: Operand,
        /// Literal candidates.
        list: Vec<ScalarValue>,
        /// True when representing `NOT IN`.
        negated: bool,
    },
    /// Null check (`IS NULL` / `IS NOT NULL`).
    IsNull {
        /// Operand under inspection.
        expr: Operand,
        /// True when representing `IS NOT NULL`.
        negated: bool,
    },
}

/// Branch predicates contain one or more child predicates.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PredicateInner {
    /// Logical negation.
    Not(Box<Predicate>),
    /// Conjunction over multiple predicates.
    And(Vec<Predicate>),
    /// Disjunction over multiple predicates.
    Or(Vec<Predicate>),
}

/// Visitor that walks predicate trees and produces row sets.
pub trait PredicateVisitor {
    /// Error type used when evaluation fails.
    type Error;
    /// Concrete row-set type produced while walking the predicate.
    type RowSetImpl: RowSet;

    /// Returns the set of all candidate rows used to evaluate negations.
    fn universe(&self) -> Self::RowSetImpl;

    /// Evaluates a leaf predicate and returns its row-set result.
    fn visit_leaf(&mut self, leaf: &PredicateLeaf) -> Result<Self::RowSetImpl, Self::Error>;

    /// Combines the result of a negated child predicate.
    fn combine_not(&mut self, child: Self::RowSetImpl) -> Result<Self::RowSetImpl, Self::Error> {
        Ok(self.universe().difference(&child))
    }

    /// Combines an `AND` clause from the supplied child results.
    fn combine_and(
        &mut self,
        children: Vec<Self::RowSetImpl>,
    ) -> Result<Self::RowSetImpl, Self::Error> {
        debug_assert!(!children.is_empty(), "AND clauses are validated");
        let mut iter = children.into_iter();
        let mut acc = iter.next().expect("non-empty AND");
        for child in iter {
            acc = acc.intersect(&child);
        }
        Ok(acc)
    }

    /// Combines an `OR` clause from the supplied child results.
    fn combine_or(
        &mut self,
        children: Vec<Self::RowSetImpl>,
    ) -> Result<Self::RowSetImpl, Self::Error> {
        debug_assert!(!children.is_empty(), "OR clauses are validated");
        let mut iter = children.into_iter();
        let mut acc = iter.next().expect("non-empty OR");
        for child in iter {
            acc = acc.union(&child);
        }
        Ok(acc)
    }

    /// Visits the supplied predicate by walking the expression tree.
    fn visit_predicate(&mut self, predicate: &Predicate) -> Result<Self::RowSetImpl, Self::Error> {
        self.visit_node(predicate.kind())
    }

    /// Internal helper that evaluates a predicate node recursively.
    fn visit_node(&mut self, node: &PredicateNode) -> Result<Self::RowSetImpl, Self::Error> {
        match node {
            PredicateNode::Leaf(leaf) => self.visit_leaf(leaf),
            PredicateNode::Inner(PredicateInner::Not(inner)) => {
                let child = self.visit_predicate(inner)?;
                self.combine_not(child)
            }
            PredicateNode::Inner(PredicateInner::And(clauses)) => {
                debug_assert!(
                    !clauses.is_empty(),
                    "Predicate::make_and enforces at least one clause"
                );
                let mut children = Vec::with_capacity(clauses.len());
                for clause in clauses {
                    children.push(self.visit_predicate(clause)?);
                }
                self.combine_and(children)
            }
            PredicateNode::Inner(PredicateInner::Or(clauses)) => {
                debug_assert!(
                    !clauses.is_empty(),
                    "Predicate::make_or enforces at least one clause"
                );
                let mut children = Vec::with_capacity(clauses.len());
                for clause in clauses {
                    children.push(self.visit_predicate(clause)?);
                }
                self.combine_or(children)
            }
        }
    }
}

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
        PredicateBuilder::conjunction()
            .compare(
                ColumnRef::new("a", None),
                ComparisonOp::GreaterThan,
                ScalarValue::Int64(1),
            )
            .or_branch(|builder| {
                builder
                    .equals(ColumnRef::new("b", None), ScalarValue::Int64(2))
                    .equals(ColumnRef::new("b", None), ScalarValue::Int64(3))
            })
            .not_branch(|builder| builder.is_null(ColumnRef::new("a", None)))
            .build()
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
                .filter_map(|value| match value {
                    ScalarValue::Null => Some(None),
                    ScalarValue::Int64(v) => Some(Some(*v)),
                    ScalarValue::UInt64(v) => i64::try_from(*v).ok().map(Some),
                    _ => None,
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
                Operand::Literal(value) => match value {
                    ScalarValue::Null => Some(None),
                    ScalarValue::Int64(v) => Some(Some(*v)),
                    ScalarValue::UInt64(v) => i64::try_from(*v).ok().map(Some),
                    _ => None,
                },
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

    impl PredicateVisitor for ComparisonVisitor {
        type Error = ();
        type RowSetImpl = BitmapRowSet;

        fn universe(&self) -> Self::RowSetImpl {
            universe_from_rows(&self.rows)
        }

        fn visit_leaf(&mut self, leaf: &PredicateLeaf) -> Result<Self::RowSetImpl, Self::Error> {
            let row_set = match leaf {
                PredicateLeaf::Compare { left, op, right } => {
                    self.evaluate_compare(left, *op, right)
                }
                PredicateLeaf::InList {
                    expr,
                    list,
                    negated,
                } => self.evaluate_in_list(expr, list, *negated),
                PredicateLeaf::IsNull { expr, negated } => self.evaluate_is_null(expr, *negated),
            };
            Ok(row_set)
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
        type RowSetImpl = BitmapRowSet;

        fn universe(&self) -> Self::RowSetImpl {
            self.all_rows.clone()
        }

        fn visit_leaf(&mut self, _leaf: &PredicateLeaf) -> Result<Self::RowSetImpl, Self::Error> {
            Ok(self.universe())
        }
    }

    #[test]
    fn predicate_visitors_share_traversal_logic() {
        let predicate = sample_predicate();
        let rows = sample_rows();

        let mut comparison = ComparisonVisitor::new(rows.clone());
        let comparison_result = predicate
            .accept(&mut comparison)
            .expect("comparison visitor succeeds");
        assert_eq!(collect_row_ids(&comparison_result), vec![0, 3]);

        let mut all_rows = AllRowsVisitor::new(universe_from_rows(&rows));
        let all_rows_result = predicate
            .accept(&mut all_rows)
            .expect("all rows visitor succeeds");
        assert!(all_rows_result.is_empty());
    }
}
