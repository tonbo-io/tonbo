//! Small helpers for building predicates.
use super::{ComparisonOp, Operand, Predicate, PredicateNode, ScalarValue};

/// Convenience constructors mirroring DataFusion-style expression helpers.
impl Predicate {
    /// Returns a predicate that always evaluates to true (matches all rows).
    #[must_use]
    pub fn always() -> Self {
        Predicate::from_kind(PredicateNode::True)
    }

    /// Create a comparison predicate.
    #[must_use]
    pub fn compare<L, R>(left: L, op: ComparisonOp, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Predicate::from_kind(PredicateNode::Compare {
            left: left.into(),
            op,
            right: right.into(),
        })
    }

    /// Equality predicate.
    #[must_use]
    pub fn eq<L, R>(left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Self::compare(left, ComparisonOp::Equal, right)
    }

    /// Inequality predicate.
    #[must_use]
    pub fn neq<L, R>(left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Self::compare(left, ComparisonOp::NotEqual, right)
    }

    /// Less-than predicate.
    #[must_use]
    pub fn lt<L, R>(left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Self::compare(left, ComparisonOp::LessThan, right)
    }

    /// Less-than-or-equal predicate.
    #[must_use]
    pub fn lte<L, R>(left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Self::compare(left, ComparisonOp::LessThanOrEqual, right)
    }

    /// Greater-than predicate.
    #[must_use]
    pub fn gt<L, R>(left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Self::compare(left, ComparisonOp::GreaterThan, right)
    }

    /// Greater-than-or-equal predicate.
    #[must_use]
    pub fn gte<L, R>(left: L, right: R) -> Self
    where
        L: Into<Operand>,
        R: Into<Operand>,
    {
        Self::compare(left, ComparisonOp::GreaterThanOrEqual, right)
    }

    /// `IN` list predicate.
    #[must_use]
    pub fn in_list<O, I>(expr: O, list: I) -> Self
    where
        O: Into<Operand>,
        I: IntoIterator<Item = ScalarValue>,
    {
        Predicate::from_kind(PredicateNode::InList {
            expr: expr.into(),
            list: list.into_iter().collect(),
            negated: false,
        })
    }

    /// `NOT IN` list predicate.
    #[must_use]
    pub fn not_in_list<O, I>(expr: O, list: I) -> Self
    where
        O: Into<Operand>,
        I: IntoIterator<Item = ScalarValue>,
    {
        Predicate::from_kind(PredicateNode::InList {
            expr: expr.into(),
            list: list.into_iter().collect(),
            negated: true,
        })
    }

    /// `IS NULL` predicate.
    #[must_use]
    pub fn is_null<O>(expr: O) -> Self
    where
        O: Into<Operand>,
    {
        Predicate::from_kind(PredicateNode::IsNull {
            expr: expr.into(),
            negated: false,
        })
    }

    /// `IS NOT NULL` predicate.
    #[must_use]
    pub fn is_not_null<O>(expr: O) -> Self
    where
        O: Into<Operand>,
    {
        Predicate::from_kind(PredicateNode::IsNull {
            expr: expr.into(),
            negated: true,
        })
    }

    /// Logical negation.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Predicate::from_kind(PredicateNode::Not(Box::new(self)))
    }
}
