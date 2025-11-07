#![deny(missing_docs)]
//! Tonbo predicate facade crate.

mod core;

pub use core::{
    BitmapRowSet, ColumnRef, ComparisonOp, Operand, Predicate, PredicateBuilder, PredicateInner,
    PredicateLeaf, PredicateNode, PredicateVisitor, RowId, RowIdIter, RowSet, ScalarValue,
};
