use thiserror::Error;

use crate::value::Value;

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

pub trait FilterProvider {
    fn eq(&self, value: Value) -> Result<Filter, FilterError>;

    fn neq(&self, value: Value) -> Result<Filter, FilterError>;

    fn lt(&self, value: Value) -> Result<Filter, FilterError>;

    fn le(&self, value: Value) -> Result<Filter, FilterError>;

    fn gt(&self, value: Value) -> Result<Filter, FilterError>;

    fn ge(&self, value: Value) -> Result<Filter, FilterError>;
}

#[allow(dead_code)]
pub struct Filter {
    field_offset: usize,
    op: FilterOp,
    value: Value,
}

impl Filter {
    pub fn new(field_offset: usize, op: FilterOp, value: Value) -> Self {
        Self {
            field_offset,
            op,
            value,
        }
    }
}

pub enum FilterOp {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
}
