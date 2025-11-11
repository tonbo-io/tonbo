use datafusion_expr::Expr;

use crate::Predicate;

fn _translate_expr_to_predicate(_filters: &[Expr]) -> &Predicate {
    todo!("Translate datafusion's expression to in-town Predicate")
}
