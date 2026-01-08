#![allow(dead_code)]
//! Predicate and scan-planning helpers for Tonbo’s read path.
//!
//! This module bridges user-facing Aisle-style expressions into the internal scan
//! planner and stream executor. It defines a lightweight predicate IR aligned with
//! Aisle's pruning grammar and adds conversions for key types used in scan planning.

pub(crate) mod scan;
pub(crate) mod stream;

use std::convert::TryFrom;

pub use datafusion_common::ScalarValue;

use crate::key::KeyOwned;

/// Comparison operators for predicate expressions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CmpOp {
    /// Equal (`=`).
    Eq,
    /// Not equal (`!=`).
    NotEq,
    /// Less than (`<`).
    Lt,
    /// Less than or equal (`<=`).
    LtEq,
    /// Greater than (`>`).
    Gt,
    /// Greater than or equal (`>=`).
    GtEq,
}

/// Aisle-style predicate expression for scan planning and residual filtering.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Constant true.
    True,
    /// Constant false.
    False,
    /// Compare a column with a scalar value.
    Cmp {
        /// Column name.
        column: String,
        /// Comparison operator.
        op: CmpOp,
        /// Scalar literal.
        value: ScalarValue,
    },
    /// Inclusive or exclusive range check on a column.
    Between {
        /// Column name.
        column: String,
        /// Lower bound.
        low: ScalarValue,
        /// Upper bound.
        high: ScalarValue,
        /// Whether bounds are inclusive.
        inclusive: bool,
    },
    /// Column membership check against a list of scalar values.
    InList {
        /// Column name.
        column: String,
        /// Candidate values.
        values: Vec<ScalarValue>,
    },
    /// Bloom-filter-assisted equality check.
    BloomFilterEq {
        /// Column name.
        column: String,
        /// Scalar literal.
        value: ScalarValue,
    },
    /// Bloom-filter-assisted membership check.
    BloomFilterInList {
        /// Column name.
        column: String,
        /// Candidate values.
        values: Vec<ScalarValue>,
    },
    /// String prefix match.
    StartsWith {
        /// Column name.
        column: String,
        /// Prefix literal.
        prefix: String,
    },
    /// Null check (negated = IS NOT NULL).
    IsNull {
        /// Column name.
        column: String,
        /// Whether the check is negated.
        negated: bool,
    },
    /// Logical AND across child predicates.
    And(Vec<Expr>),
    /// Logical OR across child predicates.
    Or(Vec<Expr>),
    /// Logical NOT of a child predicate.
    Not(Box<Expr>),
}

impl Expr {
    /// Build `column = value`.
    pub fn eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::Eq, value)
    }

    /// Build `column != value`.
    pub fn not_eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::NotEq, value)
    }

    /// Build `column < value`.
    pub fn lt(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::Lt, value)
    }

    /// Build `column <= value`.
    pub fn lt_eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::LtEq, value)
    }

    /// Build `column > value`.
    pub fn gt(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::Gt, value)
    }

    /// Build `column >= value`.
    pub fn gt_eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::GtEq, value)
    }

    /// Build a range predicate for `column`.
    pub fn between(
        column: impl Into<String>,
        low: ScalarValue,
        high: ScalarValue,
        inclusive: bool,
    ) -> Self {
        Self::Between {
            column: column.into(),
            low,
            high,
            inclusive,
        }
    }

    /// Build an IN-list predicate for `column`.
    pub fn in_list(column: impl Into<String>, values: Vec<ScalarValue>) -> Self {
        Self::InList {
            column: column.into(),
            values,
        }
    }

    /// Build a prefix predicate for `column`.
    pub fn starts_with(column: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::StartsWith {
            column: column.into(),
            prefix: prefix.into(),
        }
    }

    /// Build `column IS NULL`.
    pub fn is_null(column: impl Into<String>) -> Self {
        Self::IsNull {
            column: column.into(),
            negated: false,
        }
    }

    /// Build `column IS NOT NULL`.
    pub fn is_not_null(column: impl Into<String>) -> Self {
        Self::IsNull {
            column: column.into(),
            negated: true,
        }
    }

    /// Build a conjunction of child predicates.
    pub fn and(children: Vec<Expr>) -> Self {
        Self::And(children)
    }

    /// Build a disjunction of child predicates.
    pub fn or(children: Vec<Expr>) -> Self {
        Self::Or(children)
    }

    /// Build a negated predicate.
    pub fn negate(expr: Expr) -> Self {
        Self::Not(Box::new(expr))
    }

    fn cmp(column: impl Into<String>, op: CmpOp, value: ScalarValue) -> Self {
        Self::Cmp {
            column: column.into(),
            op,
            value,
        }
    }
}

/// Trait describing key types that can be derived from predicate scalar literals.
pub trait KeyPredicateValue: Ord + Clone {
    /// Convert a predicate scalar literal into the key type.
    fn from_scalar(value: &ScalarValue) -> Option<Self>;
}

impl KeyPredicateValue for i32 {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Int8(Some(v)) => Some(i32::from(*v)),
            ScalarValue::Int16(Some(v)) => Some(i32::from(*v)),
            ScalarValue::Int32(Some(v)) => Some(*v),
            ScalarValue::Int64(Some(v)) => i32::try_from(*v).ok(),
            ScalarValue::UInt8(Some(v)) => Some(i32::from(*v)),
            ScalarValue::UInt16(Some(v)) => Some(i32::from(*v)),
            ScalarValue::UInt32(Some(v)) => i32::try_from(*v).ok(),
            ScalarValue::UInt64(Some(v)) => i32::try_from(*v).ok(),
            _ => None,
        }
    }
}

impl KeyPredicateValue for i64 {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
            ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
            ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::UInt8(Some(v)) => Some(i64::from(*v)),
            ScalarValue::UInt16(Some(v)) => Some(i64::from(*v)),
            ScalarValue::UInt32(Some(v)) => Some(i64::from(*v)),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
            _ => None,
        }
    }
}

impl KeyPredicateValue for KeyOwned {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Boolean(Some(v)) => Some(KeyOwned::from(*v)),
            ScalarValue::Int8(Some(v)) => Some(KeyOwned::from(i64::from(*v))),
            ScalarValue::Int16(Some(v)) => Some(KeyOwned::from(i64::from(*v))),
            ScalarValue::Int32(Some(v)) => Some(KeyOwned::from(i64::from(*v))),
            ScalarValue::Int64(Some(v)) => Some(KeyOwned::from(*v)),
            ScalarValue::UInt8(Some(v)) => Some(KeyOwned::from(u64::from(*v))),
            ScalarValue::UInt16(Some(v)) => Some(KeyOwned::from(u64::from(*v))),
            ScalarValue::UInt32(Some(v)) => Some(KeyOwned::from(u64::from(*v))),
            ScalarValue::UInt64(Some(v)) => Some(KeyOwned::from(*v)),
            ScalarValue::Float32(Some(v)) => Some(KeyOwned::from(f64::from(*v))),
            ScalarValue::Float64(Some(v)) => Some(KeyOwned::from(*v)),
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => Some(KeyOwned::from(v.as_str())),
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::BinaryView(Some(v)) => Some(KeyOwned::from(v.as_slice())),
            ScalarValue::FixedSizeBinary(_, Some(v)) => Some(KeyOwned::from(v.as_slice())),
            _ => None,
        }
    }
}
