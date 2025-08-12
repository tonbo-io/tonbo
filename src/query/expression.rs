use arrow::datatypes::DataType;

use crate::{
    query::{
        assert_value_type, derive_binary_prefix_upper, derive_utf8_prefix_upper,
        error::ResolveError, ColumnSelector, ResolvedExpr, ResolvedPredicate,
    },
    record::{Schema, Value},
};

// Expression trait: resolves to ResolvedExpr
pub trait Expression {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError>;
}

// Primitive predicate: Eq
pub struct Eq<C, V> {
    pub col: C,
    pub val: V,
}

impl<C: ColumnSelector, V: Into<Value> + Clone> Expression for Eq<C, V> {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        let selector = self.col.resolve_col(schema)?;
        let value: Value = self.val.clone().into();
        if value.is_null() {
            return Err(ResolveError::EqNull);
        }
        assert_value_type(&selector, &value)?;
        Ok(ResolvedExpr::Pred(ResolvedPredicate::Eq {
            selector,
            value,
        }))
    }
}

// Logical: And
pub struct And<Lhs, Rhs> {
    pub lhs: Lhs,
    pub rhs: Rhs,
}

impl<L: Expression, R: Expression> Expression for And<L, R> {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        Ok(ResolvedExpr::And(vec![
            self.lhs.resolve(schema)?,
            self.rhs.resolve(schema)?,
        ]))
    }
}

// Logical: Or
pub struct Or<Lhs, Rhs> {
    pub lhs: Lhs,
    pub rhs: Rhs,
}

impl<L: Expression, R: Expression> Expression for Or<L, R> {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        Ok(ResolvedExpr::Or(vec![
            self.lhs.resolve(schema)?,
            self.rhs.resolve(schema)?,
        ]))
    }
}

// Logical: Not
pub struct Not<E> {
    pub expr: E,
}

impl<E: Expression> Expression for Not<E> {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        Ok(ResolvedExpr::Not(Box::new(self.expr.resolve(schema)?)))
    }
}

// Range bounds
#[derive(Debug, Clone, PartialEq)]
pub struct Bound {
    pub inclusive: bool,
    pub value: Value,
}

// Predicate: Range
pub struct Range<C, L, U> {
    pub col: C,
    pub lower: Option<(L, bool)>,
    pub upper: Option<(U, bool)>,
}

impl<C, L, U> Expression for Range<C, L, U>
where
    C: ColumnSelector,
    L: Into<Value> + Clone,
    U: Into<Value> + Clone,
{
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        let selector = self.col.resolve_col(schema)?;
        if self.lower.is_none() && self.upper.is_none() {
            return Err(ResolveError::InvalidArity {
                op: "range",
                expected: "1 or 2",
                got: 0,
            });
        }
        let lower = if let Some((lv, inc)) = &self.lower {
            let v: Value = lv.clone().into();
            if v.is_null() {
                return Err(ResolveError::EqNull);
            }
            assert_value_type(&selector, &v)?;
            Some(Bound {
                inclusive: *inc,
                value: v,
            })
        } else {
            None
        };
        let upper = if let Some((uv, inc)) = &self.upper {
            let v: Value = uv.clone().into();
            if v.is_null() {
                return Err(ResolveError::EqNull);
            }
            assert_value_type(&selector, &v)?;
            Some(Bound {
                inclusive: *inc,
                value: v,
            })
        } else {
            None
        };
        Ok(ResolvedExpr::Pred(ResolvedPredicate::Range {
            selector,
            lower,
            upper,
        }))
    }
}

// Predicate: Prefix match (lowers to a range)
pub struct Prefix<C, P> {
    pub col: C,
    pub prefix: P,
}

impl<C, P> Expression for Prefix<C, P>
where
    C: ColumnSelector,
    P: Into<Value> + Clone,
{
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        let selector = self.col.resolve_col(schema)?;
        let col_type = selector.field.data_type();
        let prefix_val: Value = self.prefix.clone().into();
        if prefix_val.is_null() {
            return Err(ResolveError::EqNull);
        }
        match (col_type, &prefix_val) {
            (DataType::Utf8, Value::String(s)) => {
                let upper = derive_utf8_prefix_upper(s);
                let lower = Bound {
                    inclusive: true,
                    value: Value::String(s.clone()),
                };
                let upper = Bound {
                    inclusive: false, // exclusive upper bound
                    value: Value::String(upper),
                };
                Ok(ResolvedExpr::Pred(ResolvedPredicate::Range {
                    selector,
                    lower: Some(lower),
                    upper: Some(upper),
                }))
            }
            (DataType::Binary, Value::Binary(bytes)) => {
                let upper = derive_binary_prefix_upper(bytes);
                let lower = Bound {
                    inclusive: true,
                    value: Value::Binary(bytes.clone()),
                };
                let upper = Bound {
                    inclusive: false, // exclusive upper bound
                    value: Value::Binary(upper),
                };
                Ok(ResolvedExpr::Pred(ResolvedPredicate::Range {
                    selector,
                    lower: Some(lower),
                    upper: Some(upper),
                }))
            }
            _ => Err(ResolveError::UnsupportedOperator {
                column: selector.field.name().to_string(),
                data_type: col_type.clone(),
                op: "prefix",
            }),
        }
    }
}

// Predicate: In (small unions)
pub struct In<C, V> {
    pub col: C,
    pub set: Vec<V>,
}

impl<C, V> Expression for In<C, V>
where
    C: ColumnSelector,
    V: Into<Value> + Clone,
{
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        let selector = self.col.resolve_col(schema)?;
        if self.set.is_empty() {
            return Err(ResolveError::InvalidArity {
                op: "in",
                expected: ">=1",
                got: 0,
            });
        }
        let mut out = Vec::with_capacity(self.set.len());
        for v in &self.set {
            let val: Value = v.clone().into();
            if val.is_null() {
                return Err(ResolveError::EqNull);
            }
            assert_value_type(&selector, &val)?;
            out.push(val);
        }
        Ok(ResolvedExpr::Pred(ResolvedPredicate::In {
            selector,
            set: out,
        }))
    }
}

// Predicate: IsNull / IsNotNull
pub struct IsNull<C> {
    pub col: C,
}

impl<C: ColumnSelector> Expression for IsNull<C> {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        let selector = self.col.resolve_col(schema)?;
        if !selector.field.is_nullable() {
            return Err(ResolveError::NullabilityViolation(
                selector.field.name().to_string(),
            ));
        }
        Ok(ResolvedExpr::Pred(ResolvedPredicate::IsNull { selector }))
    }
}

pub struct IsNotNull<C> {
    pub col: C,
}

impl<C: ColumnSelector> Expression for IsNotNull<C> {
    fn resolve<S: Schema>(&self, schema: &S) -> Result<ResolvedExpr, ResolveError> {
        let selector = self.col.resolve_col(schema)?;
        if !selector.field.is_nullable() {
            return Err(ResolveError::NullabilityViolation(
                selector.field.name().to_string(),
            ));
        }
        Ok(ResolvedExpr::Pred(ResolvedPredicate::IsNotNull {
            selector,
        }))
    }
}
