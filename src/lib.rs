pub mod executor;
pub mod filter;
mod memtable;
pub mod schema;
pub mod value;
mod version;

use std::{marker::PhantomData, sync::Arc};

use smallvec::SmallVec;
use thiserror::Error;

use crate::{
    executor::{tokio::TokioExecutor, Executor},
    filter::{Filter, FilterProvider},
    schema::{dynamic::DynamicSchema, Schema},
    value::Value,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Field not found: {0}")]
    FieldNotFound(String),
    #[error("Filter error: {0}")]
    FilterError(#[from] crate::filter::FilterError),
}

pub struct Tonbo<S: Schema, E: Executor = TokioExecutor> {
    _marker: PhantomData<S>,
    #[allow(dead_code)]
    executor: E,
}

/// Type alias for a database with dynamic schema
pub type DynamicDb<E = TokioExecutor> = Tonbo<DynamicSchema, E>;

/// Simplified database type alias
pub type Db<S, E = TokioExecutor> = Tonbo<S, E>;

/// A field reference for dynamic queries
pub struct FieldRef {
    pub(crate) name: String,
    pub(crate) schema: Arc<DynamicSchema>,
}

impl FieldRef {
    pub fn equal(&self, value: impl Into<Value>) -> Result<Filter, Error> {
        let value = value.into();
        if let Some(field) = self.schema.field(&self.name) {
            Ok(field.eq(value)?)
        } else {
            Err(Error::FieldNotFound(self.name.clone()))
        }
    }

    pub fn less_than(&self, value: impl Into<Value>) -> Result<Filter, Error> {
        let value = value.into();
        if let Some(field) = self.schema.field(&self.name) {
            Ok(field.lt(value)?)
        } else {
            Err(Error::FieldNotFound(self.name.clone()))
        }
    }

    pub fn greater_than(&self, value: impl Into<Value>) -> Result<Filter, Error> {
        let value = value.into();
        if let Some(field) = self.schema.field(&self.name) {
            Ok(field.gt(value)?)
        } else {
            Err(Error::FieldNotFound(self.name.clone()))
        }
    }
}

impl<S: Schema> Tonbo<S, TokioExecutor> {
    /// Create a new database instance with the given schema using the default TokioExecutor
    pub fn new(_schema: S) -> Self {
        Self {
            _marker: PhantomData,
            executor: TokioExecutor::default(),
        }
    }
}

impl<S: Schema, E: Executor> Tonbo<S, E> {
    /// Create a new database instance with a custom executor
    pub fn with_executor(_schema: S, executor: E) -> Self {
        Self {
            _marker: PhantomData,
            executor,
        }
    }

    pub fn txn(&self) -> Transaction<'_, S> {
        Transaction::new()
    }
}

impl<E: Executor> Tonbo<DynamicSchema, E> {
    /// Get a field reference for dynamic queries
    pub fn key(&self, field_name: impl Into<String>) -> FieldRef {
        FieldRef {
            name: field_name.into(),
            // This is a placeholder - in a real implementation, we'd store the schema in Tonbo
            schema: Arc::new(DynamicSchema::new("placeholder")),
        }
    }
}

pub struct Transaction<'txn, S: Schema> {
    _marker: PhantomData<&'txn S>,
}

impl<'txn, S: Schema> Transaction<'txn, S> {
    fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    pub fn scan(&self) -> ScanBuilder<'_, S> {
        ScanBuilder::new(self)
    }
}

impl<'txn> Transaction<'txn, DynamicSchema> {
    /// Create a scan builder for dynamic queries
    pub fn scan_dynamic(&self) -> DynamicScanBuilder<'_> {
        DynamicScanBuilder::new(self)
    }
}

pub struct ScanBuilder<'txn, S: Schema> {
    #[allow(dead_code)]
    txn: &'txn Transaction<'txn, S>,
    filters: SmallVec<Filter, 8>,
    projection: SmallVec<usize, 8>,
    limit: Option<usize>,
}

impl<'txn, S: Schema> ScanBuilder<'txn, S> {
    fn new(txn: &'txn Transaction<'txn, S>) -> Self {
        Self {
            txn,
            filters: SmallVec::new(),
            projection: SmallVec::new(),
            limit: None,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn projection(mut self, projection: impl IntoIterator<Item = usize>) -> Self {
        self.projection.extend(projection);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn execute(self) -> Result<(), Error> {
        // Implementation of scan execution goes here
        Ok(())
    }
}

/// Scan builder for dynamic schemas with string-based projections
pub struct DynamicScanBuilder<'txn> {
    #[allow(dead_code)]
    txn: &'txn Transaction<'txn, DynamicSchema>,
    filters: SmallVec<Filter, 8>,
    projection: Vec<String>,
    limit: Option<usize>,
}

impl<'txn> DynamicScanBuilder<'txn> {
    fn new(txn: &'txn Transaction<'txn, DynamicSchema>) -> Self {
        Self {
            txn,
            filters: SmallVec::new(),
            projection: Vec::new(),
            limit: None,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn projection(mut self, fields: Vec<&str>) -> Self {
        self.projection = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub async fn execute(self) -> Result<(), Error> {
        // Implementation of dynamic scan execution goes here
        Ok(())
    }
}

#[cfg(test)]
mod tests;
