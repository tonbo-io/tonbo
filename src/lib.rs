#![deny(missing_docs)]
//! Arrow-based, in-memory building blocks for Tonbo.
//!
//! The current focus is the dynamic runtime-schema path: callers work with
//! Arrow `RecordBatch` values, and the engine derives logical keys at runtime.
//! The `Mode` trait and surrounding structure keep the door open for
//! re-introducing compile-time typed dispatch in the future.

mod inmem;
pub mod mode;
pub mod record;

// Re-export the unified DB so users can do `tonbo::DB`.
pub use crate::db::DB;

#[cfg(test)]
mod test_util;

/// Generic DB that dispatches between typed and dynamic modes via generic types.
pub mod db;

/// File system for Tonbo
pub mod fs;

/// Shared scan utilities (key ranges, range sets).
pub mod scan;

/// Minimal key-focused expression tree and helpers.
pub mod query;

/// Write-ahead log framework (async, fusio-backed).
pub mod wal;

/// Manifest integration atop `fusio-manifest`.
pub mod manifest;

/// MVCC primitives shared across modules.
pub mod mvcc;

/// On-disk persistence scaffolding (SSTable skeletons).
pub mod ondisk;

/// Simple compaction orchestrators.
pub mod compaction;
