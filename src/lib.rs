#![deny(missing_docs)]
#![deny(clippy::unwrap_used, dead_code)]
//! Tonbo is an Arrow-native embedded database for serverless and edge analytics, built to keep data
//! in Arrow/Parquet on object storage.
//!
//! This crate hosts the core runtime: Arrow-first ingestion, in-memory segments, WAL + manifest
//! plumbing, and Fusio-backed async I/O. The engine is currently single-mode and dynamic: callers
//! pass Arrow `RecordBatch` values and logical keys are derived at runtime; the former `Mode` split
//! was flattened, making this dynamic layout the canonical storage path. Use `DbBuilder` to
//! construct the database and `DB` for reads/writes; other modules expose WAL, manifest, MVCC, and
//! compaction scaffolding.

pub(crate) mod extractor;
/// File and object identifiers.
pub(crate) mod id;
mod inmem;
/// Zero-copy key projection scaffolding and owned key wrapper.
pub(crate) mod key;
pub(crate) mod mode;
pub(crate) mod mutation;
pub mod schema;

// Re-export the unified DB so users can do `tonbo::DB`.
// Re-export in-memory sealing policy helpers for embedders that tweak durability.
pub use crate::{
    db::{
        ColumnRef, ComparisonOp, DB, Operand, Predicate, PredicateNode, ScalarValue, WalSyncPolicy,
    },
    inmem::policy::{BatchesThreshold, NeverSeal, SealPolicy},
    transaction::CommitAckMode,
};

#[cfg(test)]
mod test;

/// Generic DB that dispatches between typed and dynamic modes via generic types.
pub mod db;

pub(crate) mod query;

/// Write-ahead log framework (async, fusio-backed).
pub(crate) mod wal;

/// Manifest integration atop `fusio-manifest`.
pub(crate) mod manifest;

/// MVCC primitives shared across modules.
pub(crate) mod mvcc;

/// Optimistic transaction scaffolding (write path focus for now).
pub(crate) mod transaction;

/// On-disk persistence scaffolding (SSTable skeletons).
pub(crate) mod ondisk;

/// Simple compaction orchestrators.
pub(crate) mod compaction;
