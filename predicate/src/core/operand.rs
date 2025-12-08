use std::sync::Arc;

use super::ScalarValue;

/// Reference identifying a column used inside predicates.
///
/// This is a logical column reference using only the column name.
/// Physical binding (resolving to schema indices) happens during
/// query planning, not at predicate construction time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Canonical column name.
    pub name: Arc<str>,
}

impl ColumnRef {
    /// Creates a new column reference from a name.
    #[must_use]
    pub fn new<N>(name: N) -> Self
    where
        N: Into<Arc<str>>,
    {
        Self { name: name.into() }
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
