#![deny(missing_docs)]
//! Tonbo predicate facade crate.

mod core;
mod datafusion;

pub use core::{
    BitmapRowSet, ColumnRef, ComparisonOp, DynRowSet, Operand, Predicate, PredicateBuilder,
    PredicateInner, PredicateLeaf, PredicateNode, PredicateVisitor, RowId, RowIdIter, RowSet,
    ScalarValue, ScalarValueRef, VisitOutcome,
};
