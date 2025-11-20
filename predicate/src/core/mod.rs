#![deny(missing_docs)]
//! Core predicate structures shared across Tonbo adapters.

mod builder;
mod row_set;

use std::{cmp::Ordering, fmt, sync::Arc};

pub use builder::PredicateBuilder;
pub use row_set::{BitmapRowSet, DynRowSet, RowId, RowIdIter, RowSet};

/// Literal values accepted by predicate operands.
#[derive(Clone, Debug, PartialEq)]
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

    /// Compares this scalar with another, returning the ordering when both sides are comparable.
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().compare(other.as_ref())
    }

    /// Returns a borrowed view over this scalar value.
    #[must_use]
    pub fn as_ref(&self) -> ScalarValueRef<'_> {
        match self {
            ScalarValue::Null => ScalarValueRef::Null,
            ScalarValue::Boolean(value) => ScalarValueRef::Boolean(*value),
            ScalarValue::Int64(value) => ScalarValueRef::Int64(*value),
            ScalarValue::UInt64(value) => ScalarValueRef::UInt64(*value),
            ScalarValue::Float64(value) => ScalarValueRef::Float64(*value),
            ScalarValue::Utf8(value) => ScalarValueRef::Utf8(value.as_str()),
            ScalarValue::Binary(value) => ScalarValueRef::Binary(value.as_slice()),
        }
    }
}

/// Borrowed view over a scalar value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScalarValueRef<'a> {
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
    /// UTF-8 string slice.
    Utf8(&'a str),
    /// Binary slice.
    Binary(&'a [u8]),
}

impl<'a> ScalarValueRef<'a> {
    /// Returns true when the literal is the `Null` variant.
    #[must_use]
    pub fn is_null(self) -> bool {
        matches!(self, ScalarValueRef::Null)
    }

    /// Compares this scalar with another, returning the ordering when both sides are comparable.
    pub fn compare(self, other: ScalarValueRef<'_>) -> Option<Ordering> {
        use ScalarValueRef::*;
        match (self, other) {
            (Null, _) | (_, Null) => None,
            (Boolean(lhs), Boolean(rhs)) => Some(lhs.cmp(&rhs)),
            (Int64(lhs), Int64(rhs)) => Some(lhs.cmp(&rhs)),
            (UInt64(lhs), UInt64(rhs)) => Some(lhs.cmp(&rhs)),
            (Float64(lhs), Float64(rhs)) => lhs.partial_cmp(&rhs),
            (Utf8(lhs), Utf8(rhs)) => Some(lhs.cmp(rhs)),
            (Binary(lhs), Binary(rhs)) => Some(lhs.cmp(rhs)),
            _ => None,
        }
    }
}

impl<'a> From<&'a ScalarValue> for ScalarValueRef<'a> {
    fn from(value: &'a ScalarValue) -> Self {
        value.as_ref()
    }
}

/// Reference identifying a column used inside predicates.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

    /// Returns the logical negation of this operator.
    #[must_use]
    pub fn negated(self) -> Self {
        match self {
            ComparisonOp::Equal => ComparisonOp::NotEqual,
            ComparisonOp::NotEqual => ComparisonOp::Equal,
            ComparisonOp::LessThan => ComparisonOp::GreaterThanOrEqual,
            ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThan,
            ComparisonOp::GreaterThan => ComparisonOp::LessThanOrEqual,
            ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThan,
        }
    }

    /// Evaluates the operator against a comparison ordering.
    #[must_use]
    pub fn test_ordering(self, ordering: Ordering) -> bool {
        match self {
            ComparisonOp::Equal => ordering == Ordering::Equal,
            ComparisonOp::NotEqual => ordering != Ordering::Equal,
            ComparisonOp::LessThan => ordering == Ordering::Less,
            ComparisonOp::LessThanOrEqual => ordering != Ordering::Greater,
            ComparisonOp::GreaterThan => ordering == Ordering::Greater,
            ComparisonOp::GreaterThanOrEqual => ordering != Ordering::Less,
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

    /// Returns the logical negation of this predicate.
    #[must_use]
    pub fn negate(self) -> Self {
        let negated = match self.kind {
            PredicateNode::Leaf(leaf) => Predicate::negate_leaf(leaf),
            PredicateNode::Inner(PredicateInner::Not(inner)) => *inner,
            PredicateNode::Inner(PredicateInner::And(children)) => {
                let negated_children: Vec<_> =
                    children.into_iter().map(Predicate::negate).collect();
                Predicate::make_or(negated_children)
            }
            PredicateNode::Inner(PredicateInner::Or(children)) => {
                let negated_children: Vec<_> =
                    children.into_iter().map(Predicate::negate).collect();
                Predicate::make_and(negated_children)
            }
        };
        negated.simplify()
    }

    /// Builds a conjunction from the supplied predicates, if any are provided.
    #[must_use]
    pub fn conjunction(predicates: Vec<Predicate>) -> Option<Predicate> {
        match predicates.len() {
            0 => None,
            1 => predicates.into_iter().next(),
            _ => Some(Predicate::make_and(predicates).simplify()),
        }
    }

    /// Builds a disjunction from the supplied predicates, if any.
    #[must_use]
    pub fn disjunction(predicates: Vec<Predicate>) -> Option<Predicate> {
        match predicates.len() {
            0 => None,
            1 => predicates.into_iter().next(),
            _ => Some(Predicate::make_or(predicates).simplify()),
        }
    }

    /// Builds a predicate directly from a leaf node.
    #[must_use]
    pub fn from_leaf(leaf: PredicateLeaf) -> Self {
        Self::from_kind(PredicateNode::Leaf(leaf))
    }

    /// Accepts a visitor that walks the predicate tree bottom-up.
    pub fn accept<V>(&self, visitor: &mut V) -> Result<VisitOutcome<V::Value>, V::Error>
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

    fn negate_leaf(leaf: PredicateLeaf) -> Predicate {
        let negated = match leaf {
            PredicateLeaf::Compare { left, op, right } => PredicateLeaf::Compare {
                left,
                op: op.negated(),
                right,
            },
            PredicateLeaf::InList {
                expr,
                list,
                negated,
            } => PredicateLeaf::InList {
                expr,
                list,
                negated: !negated,
            },
            PredicateLeaf::IsNull { expr, negated } => PredicateLeaf::IsNull {
                expr,
                negated: !negated,
            },
        };
        Predicate::from_kind(PredicateNode::Leaf(negated))
    }
}

/// Categorises a predicate node as leaf or branch.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateNode {
    /// Leaf predicates without child expressions.
    Leaf(PredicateLeaf),
    /// Branch predicates with one or more child predicates.
    Inner(PredicateInner),
}

/// Leaf predicates encode terminal expressions with no child nodes.
#[derive(Clone, Debug, PartialEq)]
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
pub enum PredicateInner {
    /// Logical negation.
    Not(Box<Predicate>),
    /// Conjunction over multiple predicates.
    And(Vec<Predicate>),
    /// Disjunction over multiple predicates.
    Or(Vec<Predicate>),
}

/// Result produced while evaluating parts of a predicate tree.
#[derive(Clone, Debug, Default)]
pub struct VisitOutcome<T> {
    /// Computed value for the evaluated portion, when available.
    pub value: Option<T>,
    /// Residual predicate that still needs evaluation elsewhere.
    pub residual: Option<Predicate>,
}

impl<T> VisitOutcome<T> {
    /// Outcome containing only a computed value.
    pub fn value(value: T) -> Self {
        Self {
            value: Some(value),
            residual: None,
        }
    }

    /// Outcome containing only a residual predicate.
    pub fn residual(residual: Predicate) -> Self {
        Self {
            value: None,
            residual: Some(residual),
        }
    }

    /// Outcome without value or residual.
    pub fn empty() -> Self {
        Self {
            value: None,
            residual: None,
        }
    }
}

/// Visitor that walks predicate trees and emits custom results plus residual predicates.
pub trait PredicateVisitor {
    /// Error type used when evaluation fails.
    type Error;
    /// Concrete value type produced while walking the predicate.
    type Value;

    /// Evaluates a leaf predicate and returns its result.
    fn visit_leaf(
        &mut self,
        leaf: &PredicateLeaf,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error>;

    /// Combines the result of a negated child predicate.
    fn combine_not(
        &mut self,
        original: &Predicate,
        child: VisitOutcome<Self::Value>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error>;

    /// Combines an `AND` clause from the supplied child results.
    fn combine_and(
        &mut self,
        original: &Predicate,
        children: Vec<VisitOutcome<Self::Value>>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error>;

    /// Combines an `OR` clause from the supplied child results.
    fn combine_or(
        &mut self,
        original: &Predicate,
        children: Vec<VisitOutcome<Self::Value>>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error>;

    /// Visits the supplied predicate by walking the expression tree.
    fn visit_predicate(
        &mut self,
        predicate: &Predicate,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        self.visit_node(predicate.kind(), predicate)
    }

    /// Internal helper that evaluates a predicate node recursively.
    fn visit_node(
        &mut self,
        node: &PredicateNode,
        original: &Predicate,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        match node {
            PredicateNode::Leaf(leaf) => self.visit_leaf(leaf),
            PredicateNode::Inner(PredicateInner::Not(inner)) => {
                let child = self.visit_predicate(inner)?;
                self.combine_not(original, child)
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
                self.combine_and(original, children)
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
                self.combine_or(original, children)
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
        PredicateBuilder::and()
            .compare(
                ColumnRef::new("a", None),
                ComparisonOp::GreaterThan,
                ScalarValue::Int64(1),
            )
            .or_group(|builder| {
                builder
                    .equals(ColumnRef::new("b", None), ScalarValue::Int64(2))
                    .equals(ColumnRef::new("b", None), ScalarValue::Int64(3))
            })
            .not_group(|builder| builder.is_null(ColumnRef::new("a", None)))
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
            leaf: &PredicateLeaf,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
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
            _leaf: &PredicateLeaf,
        ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
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
