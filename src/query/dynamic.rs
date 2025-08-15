use arrow::datatypes::DataType;

use crate::{
    query::{
        assert_value_type, derive_binary_prefix_upper, derive_utf8_prefix_upper,
        error::ResolveError, Bound, ResolvedExpr, ResolvedPredicate, ResolvedSelector,
    },
    record::{DynSchema, Schema, Value},
};

// Dynamic, owned expression AST for FFI/SQL paths

// Enum for pointing at a column. Can use the column name or index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    Name(String),
    Index(usize),
}

// Range bound with value, specifies inclusive (>=/<=) or exclusive (>/<)
#[derive(Debug, Clone, PartialEq)]
pub struct BoundOwned {
    pub inclusive: bool,
    pub value: Value,
}

// Describes a single predicate node
#[derive(Debug, Clone, PartialEq)]
pub enum PredOwned {
    Eq {
        sel: Selector,
        val: Value,
    },
    // Describes a bound on a column. `None` for bound is [`Bound::Unbounded`]
    Range {
        sel: Selector,
        lower: Option<BoundOwned>,
        upper: Option<BoundOwned>,
    },
    In {
        sel: Selector,
        set: Vec<Value>,
    },
    IsNull {
        sel: Selector,
    },
    IsNotNull {
        sel: Selector,
    },
    Prefix {
        sel: Selector,
        prefix: Value,
    },
}

// Describes AST
#[derive(Debug, Clone, PartialEq)]
pub enum ExprOwned {
    Pred(PredOwned),
    And(Vec<ExprOwned>),
    Or(Vec<ExprOwned>),
    Not(Box<ExprOwned>),
}

impl ExprOwned {
    pub fn resolve(&self, schema: &DynSchema) -> Result<ResolvedExpr, ResolveError> {
        match self {
            ExprOwned::Pred(p) => resolve_pred(p, schema),
            ExprOwned::And(list) => {
                let mut out = Vec::with_capacity(list.len());
                for e in list {
                    out.push(e.resolve(schema)?);
                }
                Ok(ResolvedExpr::And(out))
            }
            ExprOwned::Or(list) => {
                let mut out = Vec::with_capacity(list.len());
                for e in list {
                    out.push(e.resolve(schema)?);
                }
                Ok(ResolvedExpr::Or(out))
            }
            ExprOwned::Not(e) => Ok(ResolvedExpr::Not(Box::new(e.resolve(schema)?))),
        }
    }
}

fn resolve_selector(schema: &DynSchema, sel: &Selector) -> Result<ResolvedSelector, ResolveError> {
    match sel {
        Selector::Index(i) => {
            let field = schema.arrow_schema().field(*i).clone();
            Ok(ResolvedSelector {
                index: *i,
                field: field.into(),
            })
        }
        Selector::Name(name) => {
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
                Err(ResolveError::UnknownColumn(name.clone()))
            }
        }
    }
}

fn resolve_pred(pred: &PredOwned, schema: &DynSchema) -> Result<ResolvedExpr, ResolveError> {
    match pred {
        PredOwned::Eq { sel, val } => {
            if val.is_null() {
                return Err(ResolveError::EqNull);
            }
            let selector = resolve_selector(schema, sel)?;
            assert_value_type(&selector, val)?;
            Ok(ResolvedExpr::Pred(ResolvedPredicate::Eq {
                selector,
                value: val.clone(),
            }))
        }
        PredOwned::Range { sel, lower, upper } => {
            if lower.is_none() && upper.is_none() {
                return Err(ResolveError::InvalidArity {
                    op: "range",
                    expected: "1 or 2",
                    got: 0,
                });
            }
            let selector = resolve_selector(schema, sel)?;
            let l = if let Some(b) = lower {
                if b.value.is_null() {
                    return Err(ResolveError::EqNull);
                }
                assert_value_type(&selector, &b.value)?;
                Some(Bound {
                    inclusive: b.inclusive,
                    value: b.value.clone(),
                })
            } else {
                None
            };
            let u = if let Some(b) = upper {
                if b.value.is_null() {
                    return Err(ResolveError::EqNull);
                }
                assert_value_type(&selector, &b.value)?;
                Some(Bound {
                    inclusive: b.inclusive,
                    value: b.value.clone(),
                })
            } else {
                None
            };
            Ok(ResolvedExpr::Pred(ResolvedPredicate::Range {
                selector,
                lower: l,
                upper: u,
            }))
        }
        PredOwned::In { sel, set } => {
            if set.is_empty() {
                return Err(ResolveError::InvalidArity {
                    op: "in",
                    expected: ">=1",
                    got: 0,
                });
            }
            let selector = resolve_selector(schema, sel)?;
            let mut out = Vec::with_capacity(set.len());
            for v in set.iter() {
                if v.is_null() {
                    return Err(ResolveError::EqNull);
                }
                assert_value_type(&selector, v)?;
                out.push(v.clone());
            }
            Ok(ResolvedExpr::Pred(ResolvedPredicate::In {
                selector,
                set: out,
            }))
        }
        PredOwned::IsNull { sel } => {
            let selector = resolve_selector(schema, sel)?;
            if !selector.field.is_nullable() {
                return Err(ResolveError::NullabilityViolation(
                    selector.field.name().to_string(),
                ));
            }
            Ok(ResolvedExpr::Pred(ResolvedPredicate::IsNull { selector }))
        }
        PredOwned::IsNotNull { sel } => {
            let selector = resolve_selector(schema, sel)?;
            if !selector.field.is_nullable() {
                return Err(ResolveError::NullabilityViolation(
                    selector.field.name().to_string(),
                ));
            }
            Ok(ResolvedExpr::Pred(ResolvedPredicate::IsNotNull {
                selector,
            }))
        }
        PredOwned::Prefix { sel, prefix } => {
            if prefix.is_null() {
                return Err(ResolveError::EqNull);
            }
            let selector = resolve_selector(schema, sel)?;
            match (selector.field.data_type(), prefix) {
                (DataType::Utf8, Value::String(s)) => {
                    let upper = derive_utf8_prefix_upper(s);
                    Ok(ResolvedExpr::Pred(ResolvedPredicate::Range {
                        selector,
                        lower: Some(Bound {
                            inclusive: true,
                            value: Value::String(s.clone()),
                        }),
                        upper: Some(Bound {
                            inclusive: false,
                            value: Value::String(upper),
                        }),
                    }))
                }
                (DataType::Binary, Value::Binary(bytes)) => {
                    let upper = derive_binary_prefix_upper(bytes);
                    Ok(ResolvedExpr::Pred(ResolvedPredicate::Range {
                        selector,
                        lower: Some(Bound {
                            inclusive: true,
                            value: Value::Binary(bytes.clone()),
                        }),
                        upper: Some(Bound {
                            inclusive: false,
                            value: Value::Binary(upper),
                        }),
                    }))
                }
                (dt, _) => Err(ResolveError::UnsupportedOperator {
                    column: selector.field.name().to_string(),
                    data_type: dt.clone(),
                    op: "prefix",
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> DynSchema {
        crate::dyn_schema!(
            ("name", Utf8, false),
            ("age", UInt8, false),
            ("email", Utf8, true),
            0
        )
    }

    #[test]
    fn dyn_eq_and() {
        let schema = test_schema();
        let expr = ExprOwned::And(vec![
            ExprOwned::Pred(PredOwned::Eq {
                sel: Selector::Name("name".into()),
                val: Value::String("Alice".into()),
            }),
            ExprOwned::Pred(PredOwned::Eq {
                sel: Selector::Index(3),
                val: Value::UInt8(23),
            }),
        ]);
        let resolved = expr.resolve(&schema).unwrap();
        match resolved {
            ResolvedExpr::And(v) => assert_eq!(v.len(), 2),
            _ => panic!("not and"),
        }
    }

    #[test]
    fn dyn_prefix() {
        let schema = test_schema();
        let expr = ExprOwned::Pred(PredOwned::Prefix {
            sel: Selector::Name("name".into()),
            prefix: Value::String("Al".into()),
        });
        let resolved = expr.resolve(&schema).unwrap();
        match resolved {
            ResolvedExpr::Pred(ResolvedPredicate::Range { lower, upper, .. }) => {
                let l = lower.unwrap();
                let u = upper.unwrap();
                assert_eq!(l.value, Value::String("Al".into()));
                assert_eq!(u.value, Value::String("Al\u{10FFFF}".into()));
                assert!(!u.inclusive);
            }
            _ => panic!("not range"),
        }
    }
}
