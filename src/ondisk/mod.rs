//! On-disk storage scaffolding.
//!
//! This wip module captures the entry points for durable table structures such
//! as SSTables. The concrete encoding and IO plumbing will land in follow-up
//! changes; for now we outline the core types so downstream components can
//! start wiring mode-agnostic APIs.

/// Sorted-string-table primitives and planning helpers.
pub mod sstable;
