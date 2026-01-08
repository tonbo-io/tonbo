#![allow(dead_code)]
//! Predicate and scan-planning helpers for Tonbo’s read path.
//!
//! This module bridges user-facing predicates into the internal scan planner
//! and stream executor. It re-exports the `predicate` crate’s surface and adds
//! conversions for key types used in scan planning.

pub(crate) mod scan;
pub(crate) mod stream;

use std::convert::TryFrom;

pub use tonbo_predicate::{
    ColumnRef, ComparisonOp, Operand, Predicate, PredicateNode, ScalarValue,
};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_predicate_value_i32_from_scalar() {
        let signed = ScalarValue::from(42i64);
        assert_eq!(<i32 as KeyPredicateValue>::from_scalar(&signed), Some(42));

        let unsigned = ScalarValue::from(42u64);
        assert_eq!(<i32 as KeyPredicateValue>::from_scalar(&unsigned), Some(42));

        let too_large = ScalarValue::from(i64::from(i32::MAX) + 1);
        assert_eq!(<i32 as KeyPredicateValue>::from_scalar(&too_large), None);

        let too_large_unsigned = ScalarValue::from(u64::from(i32::MAX as u32) + 1);
        assert_eq!(
            <i32 as KeyPredicateValue>::from_scalar(&too_large_unsigned),
            None
        );
    }

    #[test]
    fn key_predicate_value_i64_from_scalar() {
        let signed = ScalarValue::from(-9i64);
        assert_eq!(<i64 as KeyPredicateValue>::from_scalar(&signed), Some(-9));

        let unsigned = ScalarValue::from(9u64);
        assert_eq!(<i64 as KeyPredicateValue>::from_scalar(&unsigned), Some(9));

        let too_large = ScalarValue::from(u64::MAX);
        assert_eq!(<i64 as KeyPredicateValue>::from_scalar(&too_large), None);
    }

    #[test]
    fn key_owned_from_scalar_values() {
        let null = ScalarValue::null();
        assert_eq!(KeyOwned::from_scalar(&null), None);

        let bool_val = ScalarValue::from(true);
        assert_eq!(KeyOwned::from_scalar(&bool_val), Some(KeyOwned::from(true)));

        let signed = ScalarValue::from(7i64);
        assert_eq!(KeyOwned::from_scalar(&signed), Some(KeyOwned::from(7i64)));

        let unsigned = ScalarValue::from(7u64);
        assert_eq!(KeyOwned::from_scalar(&unsigned), Some(KeyOwned::from(7u64)));

        let float_val = ScalarValue::from(1.25f64);
        assert_eq!(
            KeyOwned::from_scalar(&float_val),
            Some(KeyOwned::from(1.25f64))
        );

        let text = ScalarValue::from("hello");
        assert_eq!(KeyOwned::from_scalar(&text), Some(KeyOwned::from("hello")));

        let bytes = ScalarValue::from(vec![1u8, 2, 3]);
        assert_eq!(
            KeyOwned::from_scalar(&bytes),
            Some(KeyOwned::from(vec![1u8, 2, 3]))
        );
    }
}
