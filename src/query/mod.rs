#![allow(dead_code)]
//! Predicate and scan-planning helpers for Tonbo’s read path.
//!
//! This module bridges user-facing Aisle expressions into the internal scan planner
//! and stream executor. It re-exports Aisle's predicate surface and adds conversions
//! for key types used in scan planning.

pub(crate) mod scan;
pub(crate) mod stream;

use std::convert::TryFrom;

pub use aisle::Expr;
pub use datafusion_common::ScalarValue;

use crate::key::KeyOwned;

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
