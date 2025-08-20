//! Typed, ArrowArrays-like layout for immutable memtables, built on typed-arrow.
//!
//! This keeps compile-time types for columns and only converts to an
//! Arrow `RecordBatch` when interoperability is needed.

use std::sync::Arc;

use typed_arrow::{
    arrow_array::RecordBatch,
    arrow_schema::Schema as ArrowSchema,
    schema::{BuildRows, IntoRecordBatch, RowBuilder, SchemaMeta},
};

/// Generic immutable arrays wrapper for a typed record `R`.
pub struct ImmutableArrays<R>
where
    R: SchemaMeta + BuildRows,
{
    pub(crate) arrays: <<R as BuildRows>::Builders as RowBuilder<R>>::Arrays,
    pub(crate) len: usize,
}

impl<R> ImmutableArrays<R>
where
    R: SchemaMeta + BuildRows,
{
    /// Returns the Arrow schema for this record type.
    #[allow(dead_code)]
    pub(crate) fn arrow_schema() -> Arc<ArrowSchema> {
        R::schema()
    }

    /// Convert into a `RecordBatch` for interoperability (consumes self).
    #[allow(dead_code)]
    pub(crate) fn into_record_batch(self) -> RecordBatch {
        IntoRecordBatch::into_record_batch(self.arrays)
    }

    /// Number of rows contained in these arrays.
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

/// Builder for `ImmutableArrays<R>` using typed-arrow generated builders.
pub(crate) struct ImmutableArraysBuilder<R>
where
    R: SchemaMeta + BuildRows,
    <R::Builders as RowBuilder<R>>::Arrays: IntoRecordBatch,
{
    inner: <R as BuildRows>::Builders,
    len: usize,
}

#[allow(dead_code)]
impl<R> ImmutableArraysBuilder<R>
where
    R: SchemaMeta + BuildRows,
    <R::Builders as RowBuilder<R>>::Arrays: IntoRecordBatch,
{
    /// Create builders with `capacity` hint.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: R::new_builders(capacity),
            len: 0,
        }
    }

    /// Append a non-null row.
    pub(crate) fn push_row(&mut self, row: R) {
        <R::Builders as RowBuilder<R>>::append_row(&mut self.inner, row);
        self.len += 1;
    }

    /// Append a null row (all columns null for this row).
    #[allow(dead_code)]
    pub(crate) fn push_null(&mut self) {
        <R::Builders as RowBuilder<R>>::append_null_row(&mut self.inner);
        self.len += 1;
    }

    /// Finish into immutable arrays.
    pub(crate) fn finish(self) -> ImmutableArrays<R> {
        let arrays = <R::Builders as RowBuilder<R>>::finish(self.inner);
        ImmutableArrays {
            arrays,
            len: self.len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ImmutableArrays, ImmutableArraysBuilder};

    #[derive(typed_arrow::Record, Clone)]
    struct User {
        id: u32,
        name: String,
        active: bool,
    }

    #[test]
    fn build_and_view_record_batch() {
        let mut b = ImmutableArraysBuilder::<User>::new(3);
        b.push_row(User {
            id: 1,
            name: "a".into(),
            active: true,
        });
        b.push_row(User {
            id: 2,
            name: "b".into(),
            active: false,
        });
        let arrays: ImmutableArrays<User> = b.finish();
        let batch = arrays.into_record_batch();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }
}
