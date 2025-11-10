use std::fmt;

/// Generic mutation container used across dynamic ingest paths.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum DynMutation<U, D = ()> {
    /// Insert or update payload materialised at commit.
    Upsert(U),
    /// Logical delete recorded at commit.
    Delete(D),
}

impl<U, D> fmt::Debug for DynMutation<U, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DynMutation::Upsert(_) => f.write_str("DynMutation::Upsert"),
            DynMutation::Delete(_) => f.write_str("DynMutation::Delete"),
        }
    }
}
