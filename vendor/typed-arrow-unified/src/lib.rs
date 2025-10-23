#![deny(missing_docs)]
//! A small facade that unifies compile-time typed schemas
//! (from the `typed-arrow` crate) and runtime/dynamic schemas
//! (from the `typed-arrow-dyn` crate) behind a single, lean API.
//!
//! The goal is zero-cost construction on the typed path (via
//! generics and monomorphization) and a minimal-dispatch dynamic
//! path for cases where the schema is only known at runtime.
//!
//! Most users will interact with:
//! - `Typed<R>` when `R` is a struct deriving the `typed-arrow` traits.
//! - `DynSchema` (or `Arc<Schema>`) for runtime-driven schemas.
//! - `SchemaLike::build_batch` to assemble a `RecordBatch` from rows.

use std::{marker::PhantomData, sync::Arc};

use typed_arrow::{
    arrow_array::RecordBatch,
    arrow_schema::Schema,
    schema::{BuildRows, IntoRecordBatch, RowBuilder, SchemaMeta},
};
use typed_arrow_dyn::{DynBuilders, DynError, DynRow, DynSchema};

/// Marker type for a compile-time typed schema `R`.
pub struct Typed<R> {
    _phantom: PhantomData<R>,
}

impl<R> Default for Typed<R> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

/// Unified interface for building batches across typed and dynamic schemas.
pub trait BuildersLike {
    /// The row representation accepted by these builders.
    type Row;

    /// The error type returned by these builders.
    type Error: std::error::Error;

    /// Append a non-null row to all columns.
    ///
    /// # Errors
    /// Returns an error if the dynamic path detects an append/type/builder issue.
    fn append_row(&mut self, row: Self::Row) -> Result<(), Self::Error>;

    /// Append an optional row; `None` appends a null to all columns.
    ///
    /// # Errors
    /// Returns an error if the dynamic path detects an append/type/builder issue.
    fn append_option_row(&mut self, row: Option<Self::Row>) -> Result<(), Self::Error>;

    /// Finish building and convert accumulated arrays into a `RecordBatch`.
    fn finish_into_batch(self) -> RecordBatch;

    /// Try to finish building a `RecordBatch`, returning an error with
    /// richer diagnostics when available (e.g., dynamic nullability).
    ///
    /// # Errors
    /// Returns an error when batch assembly fails (e.g., dynamic nullability).
    fn try_finish_into_batch(self) -> Result<RecordBatch, Self::Error>
    where
        Self: Sized,
    {
        Ok(self.finish_into_batch())
    }
}

/// Unified schema abstraction: exposes Arrow schema and row/builder types.
pub trait SchemaLike {
    /// The row type produced/consumed for this schema.
    type Row;

    /// Concrete builders used to accumulate rows into columns.
    type Builders: BuildersLike<Row = Self::Row>;

    /// Return a shared reference to the underlying Arrow `Schema`.
    fn schema_ref(&self) -> Arc<Schema>;

    /// Create new column builders with a given capacity hint.
    fn new_builders(&self, capacity: usize) -> Self::Builders;

    /// Build a `RecordBatch` from an iterator of rows.
    ///
    /// Capacity is inferred from the iterator's size hint (upper bound if
    /// present, otherwise the lower bound). For `ExactSizeIterator`s like
    /// `Vec` and slices this yields exact preallocation.
    /// # Errors
    /// Returns an error if row appends or batch finishing fails on the dynamic path.
    fn build_batch<I>(
        &self,
        rows: I,
    ) -> Result<RecordBatch, <Self::Builders as BuildersLike>::Error>
    where
        I: IntoIterator<Item = Self::Row>,
    {
        let iter = rows.into_iter();
        let (lb, ub) = iter.size_hint();
        let capacity = ub.unwrap_or(lb);
        let mut b = self.new_builders(capacity);
        for r in iter {
            b.append_row(r)?;
        }
        b.try_finish_into_batch()
    }
}

/// Adapter over typed builders that implements `BuildersLike`.
pub struct TypedBuilders<R: BuildRows> {
    inner: R::Builders,
}

impl<R: BuildRows> TypedBuilders<R> {
    fn new(inner: R::Builders) -> Self {
        Self { inner }
    }
}

#[derive(Debug)]
/// Error type for `TypedBuilders`.
pub struct NoError;

impl std::fmt::Display for NoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoError")
    }
}

impl std::error::Error for NoError {}

impl<R> BuildersLike for TypedBuilders<R>
where
    R: BuildRows,
{
    type Row = R;

    type Error = NoError;

    fn append_row(&mut self, row: Self::Row) -> Result<(), NoError> {
        <R::Builders as RowBuilder<R>>::append_row(&mut self.inner, row);
        Ok(())
    }

    fn append_option_row(&mut self, row: Option<Self::Row>) -> Result<(), NoError> {
        <R::Builders as RowBuilder<R>>::append_option_row(&mut self.inner, row);
        Ok(())
    }

    fn finish_into_batch(self) -> RecordBatch {
        <R::Builders as RowBuilder<R>>::finish(self.inner).into_record_batch()
    }
}

/// Typed schema: compile-time path.
impl<R> SchemaLike for Typed<R>
where
    R: SchemaMeta + BuildRows,
{
    type Row = R;

    type Builders = TypedBuilders<R>;

    fn schema_ref(&self) -> Arc<Schema> {
        R::schema()
    }

    fn new_builders(&self, capacity: usize) -> Self::Builders {
        TypedBuilders::new(R::new_builders(capacity))
    }
}

/// Dynamic schema: runtime path.
impl SchemaLike for DynSchema {
    type Row = DynRow;

    type Builders = DynBuilders;

    fn schema_ref(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn new_builders(&self, capacity: usize) -> Self::Builders {
        DynBuilders::new(self.schema.clone(), capacity)
    }
}

/// Convenience: treat an `Arc<Schema>` (aka `SchemaRef`) as a dynamic schema.
impl SchemaLike for Arc<Schema> {
    type Row = DynRow;

    type Builders = DynBuilders;

    fn schema_ref(&self) -> Arc<Schema> {
        self.clone()
    }

    fn new_builders(&self, capacity: usize) -> Self::Builders {
        DynBuilders::new(self.clone(), capacity)
    }
}

/// Implement unified builders for dynamic builders.
impl BuildersLike for DynBuilders {
    type Row = DynRow;

    type Error = DynError;

    fn append_row(&mut self, row: Self::Row) -> Result<(), DynError> {
        typed_arrow_dyn::DynBuilders::append_option_row(self, Some(row))
    }

    fn append_option_row(&mut self, row: Option<Self::Row>) -> Result<(), DynError> {
        typed_arrow_dyn::DynBuilders::append_option_row(self, row)
    }

    fn finish_into_batch(self) -> RecordBatch {
        typed_arrow_dyn::DynBuilders::finish_into_batch(self)
    }

    fn try_finish_into_batch(self) -> Result<RecordBatch, DynError> {
        typed_arrow_dyn::DynBuilders::try_finish_into_batch(self)
    }
}
