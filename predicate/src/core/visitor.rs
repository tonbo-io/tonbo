use super::{Predicate, PredicateNode};

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
        leaf: &PredicateNode,
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
            PredicateNode::Not(inner) => {
                let child = self.visit_predicate(inner)?;
                self.combine_not(original, child)
            }
            PredicateNode::And(clauses) => {
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
            PredicateNode::Or(clauses) => {
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
            leaf => {
                debug_assert!(leaf.is_leaf(), "non-leaf nodes handled earlier");
                self.visit_leaf(leaf)
            }
        }
    }
}
