use std::fmt;

use super::{Operand, PredicateVisitor, ScalarValue, VisitOutcome};

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
    fn negated(self) -> Self {
        match self {
            ComparisonOp::Equal => ComparisonOp::NotEqual,
            ComparisonOp::NotEqual => ComparisonOp::Equal,
            ComparisonOp::LessThan => ComparisonOp::GreaterThanOrEqual,
            ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThan,
            ComparisonOp::GreaterThan => ComparisonOp::LessThanOrEqual,
            ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThan,
        }
    }
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::LessThan => "<",
            ComparisonOp::LessThanOrEqual => "<=",
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::GreaterThanOrEqual => ">=",
        })
    }
}

/// Recursive predicate node; leaf and branch variants coexist.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateNode {
    /// Always-true literal; matches all rows.
    True,
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
    /// Logical negation.
    Not(Box<Predicate>),
    /// Conjunction over multiple predicates.
    And(Vec<Predicate>),
    /// Disjunction over multiple predicates.
    Or(Vec<Predicate>),
}

impl PredicateNode {
    /// Returns true when the node has no child predicates.
    #[must_use]
    pub(crate) fn is_leaf(&self) -> bool {
        matches!(
            self,
            PredicateNode::True
                | PredicateNode::Compare { .. }
                | PredicateNode::InList { .. }
                | PredicateNode::IsNull { .. }
        )
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
    pub fn and<I>(clauses: I) -> Self
    where
        I: IntoIterator<Item = Predicate>,
    {
        let mut acc = Vec::new();
        for clause in clauses {
            match clause.into_kind() {
                PredicateNode::And(mut nested) => acc.append(&mut nested),
                other => acc.push(Predicate::from_kind(other)),
            }
        }

        assert!(
            !acc.is_empty(),
            "Predicate::and requires at least one clause"
        );

        if acc.len() == 1 {
            acc.pop().expect("length checked")
        } else {
            Self::from_kind(PredicateNode::And(acc))
        }
    }

    /// Builds a disjunction from the supplied clauses.
    ///
    /// # Panics
    ///
    /// Panics if no clauses are provided.
    #[must_use]
    pub fn or<I>(clauses: I) -> Self
    where
        I: IntoIterator<Item = Predicate>,
    {
        let mut acc = Vec::new();
        for clause in clauses {
            match clause.into_kind() {
                PredicateNode::Or(mut nested) => acc.append(&mut nested),
                other => acc.push(Predicate::from_kind(other)),
            }
        }

        assert!(
            !acc.is_empty(),
            "Predicate::or requires at least one clause"
        );

        if acc.len() == 1 {
            acc.pop().expect("length checked")
        } else {
            Self::from_kind(PredicateNode::Or(acc))
        }
    }

    /// Applies simple simplification rules to reduce nesting.
    #[must_use]
    pub fn simplify(self) -> Self {
        match self.kind {
            PredicateNode::True
            | PredicateNode::Compare { .. }
            | PredicateNode::InList { .. }
            | PredicateNode::IsNull { .. } => self,
            PredicateNode::Not(inner) => {
                let simplified_child = inner.simplify();
                match simplified_child.into_kind() {
                    PredicateNode::Not(grandchild) => *grandchild,
                    other => Self::from_kind(PredicateNode::Not(Box::new(Self::from_kind(other)))),
                }
            }
            PredicateNode::And(clauses) => {
                Predicate::and(clauses.into_iter().map(Predicate::simplify))
            }
            PredicateNode::Or(clauses) => {
                Predicate::or(clauses.into_iter().map(Predicate::simplify))
            }
        }
    }

    /// Returns the logical negation of this predicate.
    #[must_use]
    pub fn negate(self) -> Self {
        let negated = match self.kind {
            PredicateNode::True
            | PredicateNode::Compare { .. }
            | PredicateNode::InList { .. }
            | PredicateNode::IsNull { .. } => Predicate::negate_leaf(self.into_kind()),
            PredicateNode::Not(inner) => *inner,
            PredicateNode::And(children) => {
                let negated_children: Vec<_> =
                    children.into_iter().map(Predicate::negate).collect();
                Predicate::or(negated_children)
            }
            PredicateNode::Or(children) => {
                let negated_children: Vec<_> =
                    children.into_iter().map(Predicate::negate).collect();
                Predicate::and(negated_children)
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
            _ => Some(Predicate::and(predicates).simplify()),
        }
    }

    /// Builds a disjunction from the supplied predicates, if any.
    #[must_use]
    pub fn disjunction(predicates: Vec<Predicate>) -> Option<Predicate> {
        match predicates.len() {
            0 => None,
            1 => predicates.into_iter().next(),
            _ => Some(Predicate::or(predicates).simplify()),
        }
    }

    /// Builds a predicate directly from a single node.
    #[must_use]
    pub fn from_node(node: PredicateNode) -> Self {
        Self::from_kind(node)
    }

    /// Accepts a visitor that walks the predicate tree bottom-up.
    pub fn accept<V>(&self, visitor: &mut V) -> Result<VisitOutcome<V::Value>, V::Error>
    where
        V: PredicateVisitor + ?Sized,
    {
        visitor.visit_predicate(self)
    }

    pub(crate) fn from_kind(kind: PredicateNode) -> Self {
        Self { kind }
    }

    fn into_kind(self) -> PredicateNode {
        self.kind
    }

    fn negate_leaf(leaf: PredicateNode) -> Predicate {
        let negated = match leaf {
            PredicateNode::True => {
                // NOT TRUE is represented as a wrapped negation
                return Predicate::from_kind(PredicateNode::Not(Box::new(Predicate::from_kind(
                    PredicateNode::True,
                ))));
            }
            PredicateNode::Compare { left, op, right } => PredicateNode::Compare {
                left,
                op: op.negated(),
                right,
            },
            PredicateNode::InList {
                expr,
                list,
                negated,
            } => PredicateNode::InList {
                expr,
                list,
                negated: !negated,
            },
            PredicateNode::IsNull { expr, negated } => PredicateNode::IsNull {
                expr,
                negated: !negated,
            },
            PredicateNode::Not(_) | PredicateNode::And(_) | PredicateNode::Or(_) => {
                unreachable!("negate_leaf only handles leaf variants")
            }
        };
        Predicate::from_kind(negated)
    }
}
