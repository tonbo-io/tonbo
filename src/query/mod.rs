pub mod dynamic;
pub mod error;
pub mod expression;

use arrow::datatypes::FieldRef;

use crate::query::expression::Bound;
use crate::{
    query::error::ResolveError,
    record::{Schema, Value},
};

// Resolved forms (schema-aware)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSelector {
    pub index: usize,
    pub field: FieldRef,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedPredicate {
    Eq {
        selector: ResolvedSelector,
        value: Value,
    },
    Range {
        selector: ResolvedSelector,
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
    In {
        selector: ResolvedSelector,
        set: Vec<Value>,
    },
    IsNull {
        selector: ResolvedSelector,
    },
    IsNotNull {
        selector: ResolvedSelector,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedExpr {
    And(Vec<ResolvedExpr>),
    Or(Vec<ResolvedExpr>),
    Not(Box<ResolvedExpr>),
    Pred(ResolvedPredicate),
}

// Column selectors specialized to DynSchema
pub trait ColumnSelector {
    fn resolve_col<S: Schema>(&self, schema: &S) -> Result<ResolvedSelector, ResolveError>;
}

pub struct ColIndex<const I: usize>;
impl<const I: usize> ColumnSelector for ColIndex<I> {
    fn resolve_col<S: Schema>(&self, schema: &S) -> Result<ResolvedSelector, ResolveError> {
        let field: FieldRef = schema.arrow_schema().field(I).clone().into();
        Ok(ResolvedSelector { index: I, field })
    }
}

pub struct Col<'a>(pub &'a str);

impl ColumnSelector for Col<'_> {
    fn resolve_col<S: Schema>(&self, schema: &S) -> Result<ResolvedSelector, ResolveError> {
        let name = self.0;
        let arrow_schema = schema.arrow_schema();
        if let Some((index, field)) = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == name)
        {
            Ok(ResolvedSelector {
                index,
                field: field.clone(),
            })
        } else {
            Err(ResolveError::UnknownColumn(name.to_string()))
        }
    }
}

fn derive_utf8_prefix_upper(prefix: &str) -> String {
    // Exclusive upper bound: prefix + U+10FFFF (no real string equals it)
    // All strings with the prefix are strictly less than this bound.
    let mut s = String::with_capacity(prefix.len() + 4);
    s.push_str(prefix);
    s.push('\u{10FFFF}');
    s
}

fn derive_binary_prefix_upper(prefix: &[u8]) -> Vec<u8> {
    // Exclusive upper bound: prefix + 0xFF (no real binary starting with prefix exceeds it while
    // sharing prefix)
    let mut v = Vec::with_capacity(prefix.len() + 1);
    v.extend_from_slice(prefix);
    v.push(0xFF);
    v
}

fn assert_value_type(column: &ResolvedSelector, value: &Value) -> Result<(), ResolveError> {
    let actual = value.data_type();
    let expected = column.field.data_type().clone();
    if expected == actual {
        Ok(())
    } else {
        Err(ResolveError::TypeMismatch {
            column: column.field.name().to_string(),
            expected,
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::expression::*;
    use super::*;
    use crate::record::DynSchema;

    fn test_schema() -> DynSchema {
        crate::dyn_schema!(
            ("name", Utf8, false),
            ("age", UInt8, false),
            ("email", Utf8, true),
            0
        )
    }

    #[test]
    fn resolve_eq_and_type_check() {
        let schema = test_schema();
        let expr = Eq {
            col: Col("name"),
            val: "Alice",
        };
        let resolved = expr.resolve(&schema).unwrap();
        match resolved {
            ResolvedExpr::Pred(ResolvedPredicate::Eq { selector, value }) => {
                assert_eq!(selector.field.name(), "name");
                assert_eq!(selector.field.data_type(), &DataType::Utf8);
                assert_eq!(value, Value::String("Alice".to_string()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_column_error() {
        let schema = test_schema();
        let expr = Eq {
            col: Col("nope"),
            val: 1u8,
        };
        let err = expr.resolve(&schema).unwrap_err();
        match err {
            ResolveError::UnknownColumn(col) => assert_eq!(col, "nope"),
            e => panic!("unexpected error: {e:?}"),
        }
    }

    #[test]
    fn type_mismatch_error() {
        let schema = test_schema();
        let expr = Eq {
            col: Col("age"),
            val: 42i32,
        }; // age is UInt8
        let err = expr.resolve(&schema).unwrap_err();
        match err {
            ResolveError::TypeMismatch { column, .. } => assert_eq!(column, "age"),
            e => panic!("unexpected error: {e:?}"),
        }
    }

    #[test]
    fn and_compose_two_eq() {
        let schema = test_schema();
        // Arrow indices: 0=_null, 1=_ts, 2=name, 3=age, 4=email
        let expr = And {
            lhs: Eq {
                col: Col("name"),
                val: "Alice",
            },
            rhs: Eq {
                col: ColIndex::<3>,
                val: 23u8,
            },
        };
        let resolved = expr.resolve(&schema).unwrap();
        match resolved {
            ResolvedExpr::And(list) => assert_eq!(list.len(), 2),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn resolve_range_uint8() {
        let schema = test_schema();
        let expr = Range {
            col: Col("age"),
            lower: Some((10u8, true)),
            upper: Some((20u8, false)),
        };
        let resolved = expr.resolve(&schema).unwrap();
        match resolved {
            ResolvedExpr::Pred(ResolvedPredicate::Range {
                selector,
                lower,
                upper,
            }) => {
                assert_eq!(selector.field.name(), "age");
                let l = lower.unwrap();
                assert!(l.inclusive);
                assert_eq!(l.value, Value::UInt8(10));
                let u = upper.unwrap();
                assert!(!u.inclusive);
                assert_eq!(u.value, Value::UInt8(20));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn resolve_prefix_utf8() {
        let schema = test_schema();
        let expr = Prefix {
            col: Col("name"),
            prefix: "Al",
        };
        let resolved = expr.resolve(&schema).unwrap();
        match resolved {
            ResolvedExpr::Pred(ResolvedPredicate::Range {
                selector,
                lower,
                upper,
            }) => {
                assert_eq!(selector.field.data_type(), &DataType::Utf8);
                let l = lower.unwrap();
                assert_eq!(l.value, Value::String("Al".to_string()));
                let u = upper.unwrap();
                assert_eq!(u.value, Value::String("Al\u{10FFFF}".to_string()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn prefix_wrong_type_rejected() {
        let schema = test_schema();
        let expr = Prefix {
            col: Col("age"),
            prefix: 1u8,
        };
        let err = expr.resolve(&schema).unwrap_err();
        match err {
            ResolveError::UnsupportedOperator { op, column, .. } => {
                assert_eq!(op, "prefix");
                assert_eq!(column, "age");
            }
            e => panic!("unexpected error: {e:?}"),
        }
    }
}
