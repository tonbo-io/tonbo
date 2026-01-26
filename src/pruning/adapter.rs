//! Adapter from Tonbo predicates to Aisle pruning IR.
//!
//! Mapping rules (conservative):
//! - `PredicateNode::True` -> `Expr::True`
//! - `Compare` -> `Expr::Cmp` when exactly one side is a column and the other is a literal.
//!   - If the literal is on the left, the comparison operator is flipped.
//! - `InList` -> `Expr::InList` when the tested expression is a column and all list values are
//!   supported literals. `NOT IN` becomes `Expr::Not(Expr::InList(...))`.
//! - `IsNull` -> `Expr::IsNull` when the expression is a column.
//! - `Not` -> `Expr::Not` when the child is supported.
//! - `And` -> retains supported children and drops unsupported ones. If none are supported, returns
//!   `None` (no pruning).
//! - `Or` -> all children must be supported; otherwise returns `None` (no pruning).
//!
//! Unsupported constructs return `None`, signaling the caller to skip pruning for safety.

use aisle::{CmpOp, Expr};
use datafusion_common::ScalarValue as DfScalar;
use tonbo_predicate::{ComparisonOp, Operand, Predicate, PredicateNode, ScalarValue};
use typed_arrow_dyn::DynCell;

/// Convert a Tonbo predicate into an Aisle IR expression.
///
/// Returns `None` when the predicate cannot be translated safely.
pub(crate) fn to_aisle_expr(predicate: &Predicate) -> Option<Expr> {
    match predicate.kind() {
        PredicateNode::True => Some(Expr::True),
        PredicateNode::Compare { left, op, right } => compare_to_expr(left, *op, right),
        PredicateNode::InList {
            expr,
            list,
            negated,
        } => in_list_to_expr(expr, list, *negated),
        PredicateNode::IsNull { expr, negated } => is_null_to_expr(expr, *negated),
        PredicateNode::Not(child) => {
            let child_expr = to_aisle_expr(child)?;
            Some(Expr::not(child_expr))
        }
        PredicateNode::And(children) => and_to_expr(children),
        PredicateNode::Or(children) => or_to_expr(children),
    }
}

fn compare_to_expr(left: &Operand, op: ComparisonOp, right: &Operand) -> Option<Expr> {
    match (left, right) {
        (Operand::Column(column), Operand::Literal(value)) => {
            let df_value = scalar_to_df(value)?;
            Some(Expr::cmp(column.name.as_ref(), map_cmp(op), df_value))
        }
        (Operand::Literal(value), Operand::Column(column)) => {
            let df_value = scalar_to_df(value)?;
            Some(Expr::cmp(
                column.name.as_ref(),
                map_cmp(op.flipped()),
                df_value,
            ))
        }
        _ => None,
    }
}

fn in_list_to_expr(expr: &Operand, list: &[ScalarValue], negated: bool) -> Option<Expr> {
    let Operand::Column(column) = expr else {
        return None;
    };
    if list.is_empty() {
        return None;
    }
    let mut values = Vec::with_capacity(list.len());
    for value in list {
        values.push(scalar_to_df(value)?);
    }
    let expr = Expr::in_list(column.name.as_ref(), values);
    if negated {
        Some(Expr::not(expr))
    } else {
        Some(expr)
    }
}

fn is_null_to_expr(expr: &Operand, negated: bool) -> Option<Expr> {
    let Operand::Column(column) = expr else {
        return None;
    };
    Some(Expr::IsNull {
        column: column.name.to_string(),
        negated,
    })
}

fn and_to_expr(children: &[Predicate]) -> Option<Expr> {
    let mut supported = Vec::new();
    for child in children {
        if let Some(expr) = to_aisle_expr(child) {
            supported.push(expr);
        }
    }
    match supported.len() {
        0 => None,
        1 => supported.pop(),
        _ => Some(Expr::and(supported)),
    }
}

fn or_to_expr(children: &[Predicate]) -> Option<Expr> {
    let mut supported = Vec::with_capacity(children.len());
    for child in children {
        supported.push(to_aisle_expr(child)?);
    }
    match supported.len() {
        0 => None,
        1 => supported.pop(),
        _ => Some(Expr::or(supported)),
    }
}

fn map_cmp(op: ComparisonOp) -> CmpOp {
    match op {
        ComparisonOp::Equal => CmpOp::Eq,
        ComparisonOp::NotEqual => CmpOp::NotEq,
        ComparisonOp::LessThan => CmpOp::Lt,
        ComparisonOp::LessThanOrEqual => CmpOp::LtEq,
        ComparisonOp::GreaterThan => CmpOp::Gt,
        ComparisonOp::GreaterThanOrEqual => CmpOp::GtEq,
    }
}

fn scalar_to_df(value: &ScalarValue) -> Option<DfScalar> {
    match value.as_dyn() {
        DynCell::Null => Some(DfScalar::Null),
        DynCell::Bool(v) => Some(DfScalar::Boolean(Some(*v))),
        DynCell::I8(v) => Some(DfScalar::Int8(Some(*v))),
        DynCell::I16(v) => Some(DfScalar::Int16(Some(*v))),
        DynCell::I32(v) => Some(DfScalar::Int32(Some(*v))),
        DynCell::I64(v) => Some(DfScalar::Int64(Some(*v))),
        DynCell::U8(v) => Some(DfScalar::UInt8(Some(*v))),
        DynCell::U16(v) => Some(DfScalar::UInt16(Some(*v))),
        DynCell::U32(v) => Some(DfScalar::UInt32(Some(*v))),
        DynCell::U64(v) => Some(DfScalar::UInt64(Some(*v))),
        DynCell::F32(v) => Some(DfScalar::Float32(Some(*v))),
        DynCell::F64(v) => Some(DfScalar::Float64(Some(*v))),
        DynCell::Str(v) => Some(DfScalar::Utf8(Some(v.clone()))),
        DynCell::Bin(v) => Some(DfScalar::Binary(Some(v.clone()))),
        DynCell::Struct(_)
        | DynCell::List(_)
        | DynCell::FixedSizeList(_)
        | DynCell::Map(_)
        | DynCell::Union { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use tonbo_predicate::{ColumnRef, Predicate, ScalarValue};

    use super::*;

    #[test]
    fn compares_column_literal() {
        let pred = Predicate::gt(ColumnRef::new("age"), ScalarValue::from(42_i64));
        let expr = to_aisle_expr(&pred).expect("expr");
        match expr {
            Expr::Cmp { column, op, value } => {
                assert_eq!(column, "age");
                assert_eq!(op, CmpOp::Gt);
                assert_eq!(value, DfScalar::Int64(Some(42)));
            }
            other => panic!("unexpected expr: {other:?}"),
        }
    }

    #[test]
    fn flips_literal_column() {
        let pred = Predicate::lt(ScalarValue::from(10_i64), ColumnRef::new("score"));
        let expr = to_aisle_expr(&pred).expect("expr");
        match expr {
            Expr::Cmp { column, op, value } => {
                assert_eq!(column, "score");
                assert_eq!(op, CmpOp::Gt);
                assert_eq!(value, DfScalar::Int64(Some(10)));
            }
            other => panic!("unexpected expr: {other:?}"),
        }
    }

    #[test]
    fn and_drops_unsupported_children() {
        let supported = Predicate::eq(ColumnRef::new("a"), ScalarValue::from(1_i64));
        let unsupported = Predicate::eq(ColumnRef::new("a"), ColumnRef::new("b"));
        let pred = Predicate::and(vec![supported.clone(), unsupported]);
        let expr = to_aisle_expr(&pred).expect("expr");
        assert_eq!(expr, to_aisle_expr(&supported).expect("supported"));
    }

    #[test]
    fn or_requires_all_supported() {
        let supported = Predicate::eq(ColumnRef::new("a"), ScalarValue::from(1_i64));
        let unsupported = Predicate::eq(ColumnRef::new("a"), ColumnRef::new("b"));
        let pred = Predicate::or(vec![supported, unsupported]);
        assert!(to_aisle_expr(&pred).is_none());
    }

    #[test]
    fn not_in_list_wraps_not() {
        let pred = Predicate::not_in_list(ColumnRef::new("id"), vec![ScalarValue::from(1_i64)]);
        let expr = to_aisle_expr(&pred).expect("expr");
        match expr {
            Expr::Not(inner) => match *inner {
                Expr::InList { column, values } => {
                    assert_eq!(column, "id");
                    assert_eq!(values, vec![DfScalar::Int64(Some(1))]);
                }
                other => panic!("unexpected inner expr: {other:?}"),
            },
            other => panic!("unexpected expr: {other:?}"),
        }
    }
}
