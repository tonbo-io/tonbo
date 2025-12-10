//! On-disk SST scaffolding (writers/readers, merge, scan skeletons).
//!
//! This wip module captures the entry points for durable table structures such
//! as SSTables. The concrete encoding and IO plumbing will land in follow-up
//! changes; for now we outline the core types so downstream components can
//! start wiring mode-agnostic APIs.

/// Sorted-string-table primitives and planning helpers.
pub mod sstable;

/// Merge pipeline scaffolding used by major compaction.
pub mod merge;

/// Scan stream for a sstable
pub mod scan;
