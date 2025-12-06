//! Mutable memtable(s).
//!
//! This module implements the columnar-style mutable memtable used by Tonbo's
//! dynamic layout. The earlier trait-based abstraction has been flattened: the
//! engine now only builds the dynamic `DynMem` implementation.

pub(crate) mod memtable;
mod metrics;

pub(crate) use memtable::DynMem;
pub(crate) use metrics::MutableMemTableMetrics;
