#![allow(dead_code)]
//! Predicate and scan-planning helpers for Tonbo’s read path.
//!
//! This module bridges user-facing predicates into the internal scan planner
//! and stream executor. It re-exports the `predicate` crate’s surface and adds
//! conversions for key types used in scan planning.

pub(crate) mod scan;
pub(crate) mod stream;

use std::convert::TryFrom;

pub use predicate::{ColumnRef, ComparisonOp, Operand, Predicate, PredicateNode, ScalarValue};

use crate::key::KeyOwned;

/// Trait describing key types that can be derived from predicate scalar literals.
pub trait KeyPredicateValue: Ord + Clone {
    /// Convert a predicate scalar literal into the key type.
    fn from_scalar(value: &ScalarValue) -> Option<Self>;
}

impl KeyPredicateValue for i32 {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        let view = value.as_ref();
        if let Some(v) = view.as_int_i128() {
            return i32::try_from(v).ok();
        }
        if let Some(v) = view.as_uint_u128() {
            return i32::try_from(v).ok();
        }
        None
    }
}

impl KeyPredicateValue for i64 {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        let view = value.as_ref();
        if let Some(v) = view.as_int_i128() {
            return i64::try_from(v).ok();
        }
        if let Some(v) = view.as_uint_u128() {
            return i64::try_from(v).ok();
        }
        None
    }
}

impl KeyPredicateValue for KeyOwned {
    fn from_scalar(value: &ScalarValue) -> Option<Self> {
        let view = value.as_ref();
        if view.is_null() {
            return None;
        }
        if let Some(v) = view.as_bool() {
            return Some(v.into());
        }
        if let Some(v) = view.as_int_i128()
            && let Ok(val) = i64::try_from(v)
        {
            return Some(KeyOwned::from(val));
        }
        if let Some(v) = view.as_uint_u128()
            && let Ok(val) = u64::try_from(v)
        {
            return Some(KeyOwned::from(val));
        }
        if let Some(v) = view.as_f64() {
            return Some(KeyOwned::from(v));
        }
        if let Some(v) = view.as_utf8() {
            return Some(v.into());
        }
        if let Some(v) = view.as_binary() {
            return Some(v.to_vec().into());
        }
        None
    }
}
