//! Builder for composing predicate trees.

#[cfg(test)]
use super::ColumnRef;
use super::{
    ComparisonOp, Operand, Predicate, PredicateInner, PredicateLeaf, PredicateNode, ScalarValue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuilderCombine {
    Leaf,
    Conjunction,
    Disjunction,
}

/// Builder for composing predicates incrementally.
#[derive(Debug)]
pub struct PredicateBuilder {
    combine: BuilderCombine,
    clauses: Vec<Predicate>,
}

impl PredicateBuilder {
    const fn new(combine: BuilderCombine) -> Self {
        Self {
            combine,
            clauses: Vec::new(),
        }
    }

    /// Creates a builder that expects a single clause.
    #[must_use]
    pub fn leaf() -> Self {
        Self::new(BuilderCombine::Leaf)
    }

    /// Creates a builder that emits an `AND` of all clauses.
    #[must_use]
    pub fn and() -> Self {
        Self::new(BuilderCombine::Conjunction)
    }

    /// Creates a builder that emits an `OR` of all clauses.
    #[must_use]
    pub fn or() -> Self {
        Self::new(BuilderCombine::Disjunction)
    }

    /// Adds an existing predicate to the builder.
    #[must_use]
    pub fn predicate(mut self, predicate: Predicate) -> Self {
        self.clauses.push(predicate);
        self
    }

    /// Adds a comparison predicate.
    #[must_use]
    pub fn compare<L, R>(mut self, left: L, op: ComparisonOp, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op,
            right: right.into(),
        });
        self
    }

    /// Adds an equality predicate.
    #[must_use]
    pub fn equals<L, R>(mut self, left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op: ComparisonOp::Equal,
            right: right.into(),
        });
        self
    }

    /// Adds an inequality predicate.
    #[must_use]
    pub fn not_equals<L, R>(mut self, left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op: ComparisonOp::NotEqual,
            right: right.into(),
        });
        self
    }

    /// Adds a `<` comparison predicate.
    #[must_use]
    pub fn less_than<L, R>(mut self, left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op: ComparisonOp::LessThan,
            right: right.into(),
        });
        self
    }

    /// Adds a `<=` comparison predicate.
    #[must_use]
    pub fn less_than_or_equal<L, R>(mut self, left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op: ComparisonOp::LessThanOrEqual,
            right: right.into(),
        });
        self
    }

    /// Adds a `>` comparison predicate.
    #[must_use]
    pub fn greater_than<L, R>(mut self, left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op: ComparisonOp::GreaterThan,
            right: right.into(),
        });
        self
    }

    /// Adds a `>=` comparison predicate.
    #[must_use]
    pub fn greater_than_or_equal<L, R>(mut self, left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::Compare {
            left: left.into(),
            op: ComparisonOp::GreaterThanOrEqual,
            right: right.into(),
        });
        self
    }

    /// Adds an `IN` predicate.
    #[must_use]
    pub fn in_list<O, I>(mut self, expr: O, list: I) -> Self
    where
        O: Into<Operand>,
        I: IntoIterator<Item = ScalarValue>,
    {
        self.push_leaf(PredicateLeaf::InList {
            expr: expr.into(),
            list: list.into_iter().collect(),
            negated: false,
        });
        self
    }

    /// Adds a `NOT IN` predicate.
    #[must_use]
    pub fn not_in_list<O, I>(mut self, expr: O, list: I) -> Self
    where
        O: Into<Operand>,
        I: IntoIterator<Item = ScalarValue>,
    {
        self.push_leaf(PredicateLeaf::InList {
            expr: expr.into(),
            list: list.into_iter().collect(),
            negated: true,
        });
        self
    }

    /// Adds an `IS NULL` predicate.
    #[must_use]
    pub fn is_null<O>(mut self, expr: O) -> Self
    where
        O: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::IsNull {
            expr: expr.into(),
            negated: false,
        });
        self
    }

    /// Adds an `IS NOT NULL` predicate.
    #[must_use]
    pub fn is_not_null<O>(mut self, expr: O) -> Self
    where
        O: Into<Operand>,
    {
        self.push_leaf(PredicateLeaf::IsNull {
            expr: expr.into(),
            negated: true,
        });
        self
    }

    fn branch<F>(mut self, combine: BuilderCombine, build: F) -> Self
    where
        F: FnOnce(PredicateBuilder) -> PredicateBuilder,
    {
        let predicate = build(PredicateBuilder::new(combine)).build();
        self.clauses.push(predicate);
        self
    }

    /// Adds a nested conjunction built by the supplied closure.
    #[must_use]
    pub fn and_group<F>(self, build: F) -> Self
    where
        F: FnOnce(PredicateBuilder) -> PredicateBuilder,
    {
        self.branch(BuilderCombine::Conjunction, build)
    }

    /// Adds a nested disjunction built by the supplied closure.
    #[must_use]
    pub fn or_group<F>(self, build: F) -> Self
    where
        F: FnOnce(PredicateBuilder) -> PredicateBuilder,
    {
        self.branch(BuilderCombine::Disjunction, build)
    }

    /// Adds a negated predicate built by the supplied closure.
    #[must_use]
    pub fn not_group<F>(mut self, build: F) -> Self
    where
        F: FnOnce(PredicateBuilder) -> PredicateBuilder,
    {
        let predicate = build(PredicateBuilder::and()).build();
        let negated = Predicate::from_kind(PredicateNode::Inner(PredicateInner::Not(Box::new(
            predicate,
        ))))
        .simplify();
        self.clauses.push(negated);
        self
    }

    fn push_leaf(&mut self, leaf: PredicateLeaf) {
        self.clauses
            .push(Predicate::from_kind(PredicateNode::Leaf(leaf)));
    }

    /// Consumes the builder and returns the composed predicate.
    #[must_use]
    pub fn build(self) -> Predicate {
        assert!(
            !self.clauses.is_empty(),
            "PredicateBuilder requires at least one clause"
        );
        match self.combine {
            BuilderCombine::Leaf => {
                assert!(
                    self.clauses.len() == 1,
                    "PredicateBuilder::leaf must contain exactly one clause"
                );
                self.clauses
                    .into_iter()
                    .next()
                    .expect("length checked for leaf builder")
            }
            BuilderCombine::Conjunction => Predicate::make_and(self.clauses),
            BuilderCombine::Disjunction => Predicate::make_or(self.clauses),
        }
    }
}

impl Default for PredicateBuilder {
    fn default() -> Self {
        Self::leaf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "PredicateBuilder requires at least one clause")]
    fn leaf_builder_requires_clause() {
        let _ = PredicateBuilder::leaf().build();
    }

    #[test]
    #[should_panic(expected = "PredicateBuilder::leaf must contain exactly one clause")]
    fn leaf_builder_rejects_multiple_clauses() {
        let column = ColumnRef::new("col", None);
        let _ = PredicateBuilder::leaf()
            .equals(column.clone(), ScalarValue::Int64(1))
            .equals(column, ScalarValue::Int64(2))
            .build();
    }

    #[test]
    #[should_panic(expected = "PredicateBuilder requires at least one clause")]
    fn and_builder_requires_clause() {
        let _ = PredicateBuilder::and().build();
    }

    #[test]
    #[should_panic(expected = "PredicateBuilder requires at least one clause")]
    fn or_builder_requires_clause() {
        let _ = PredicateBuilder::or().build();
    }
}
