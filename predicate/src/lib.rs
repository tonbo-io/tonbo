#![deny(missing_docs)]
//! Tonbo predicate facade crate.
//!
//! This crate is Arrow-first: predicate operands and literals are expressed
//! directly in terms of `typed-arrow-dyn` cells, and evaluation assumes Arrow
//! semantics (including NULL handling and mixed-width numeric coercions). There
//! is no alternate storage or layout backend — keep the surface tight and Arrow
//! native.

mod core;

pub use core::{
    BitmapRowSet, ColumnRef, ComparisonOp, Operand, Predicate, PredicateNode, PredicateVisitor,
    RowId, RowIdIter, RowSet, ScalarValue, ScalarValueRef, VisitOutcome,
};
