#![deny(missing_docs)]
//! Typed-arrow based, in-memory building blocks for Tonbo.
//!
//! This crate experiments with a minimal design:
//! - A `Record` trait that defines the logical key and how to derive keys directly from typed-arrow
//!   arrays (string/binary keys are zero-copy via buffer-backed key types).
//! - An in-memory columnar mutable memtable with a last-writer index.
//! - An `ImmutableMemTable` that indexes with the same key type.

mod inmem;
pub mod record;
pub use record::Record;

// Re-export the unified DB so users can do `tonbo::DB`.
pub use crate::tonbo::Tonbo;

/// Generic DB that dispatches between typed and dynamic modes via generic types.
pub mod tonbo;

/// Shared scan utilities (key ranges, range sets).
pub mod scan;

/// Minimal key-focused expression tree and helpers.
pub mod query;
