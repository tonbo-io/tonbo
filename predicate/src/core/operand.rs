use std::sync::Arc;

use super::ScalarValue;

/// Reference identifying a column used inside predicates.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Optional ordinal of the column within the projected schema.
    pub index: Option<usize>,
    /// Canonical column name.
    pub name: Arc<str>,
}

impl ColumnRef {
    /// Creates a new column reference from a name and optional index.
    #[must_use]
    pub fn new<N>(name: N, index: Option<usize>) -> Self
    where
        N: Into<Arc<str>>,
    {
        Self {
            name: name.into(),
            index,
        }
    }
}

/// Operand used by predicate comparisons and function calls.
#[derive(Clone, Debug, PartialEq)]
pub enum Operand {
    /// Reference to a column.
    Column(ColumnRef),
    /// Literal value.
    Literal(ScalarValue),
}

impl From<ColumnRef> for Operand {
    fn from(value: ColumnRef) -> Self {
        Self::Column(value)
    }
}

impl From<ScalarValue> for Operand {
    fn from(value: ScalarValue) -> Self {
        Self::Literal(value)
    }
}
