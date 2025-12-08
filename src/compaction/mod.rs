//! Lightweight compaction orchestrators and planners.
//!
//! These helpers sit on top of the in-memory staging surfaces and decide when
//! to drain immutable runs into on-disk SSTables.

/// Compaction driver for orchestrating compaction operations.
mod driver;
/// Compaction executor interfaces.
pub(crate) mod executor;
/// Unified handle for background compaction workers.
mod handle;
/// Naïve minor-compaction driver for flushing immutable memtables.
mod minor;
/// Pure orchestration functions for version/outcome manipulation.
pub(crate) mod orchestrator;
/// Leveled compaction planning helpers.
pub mod planner;
/// Scheduler scaffolding for background/remote compaction (native builds only for now).
mod scheduler;

pub(crate) use driver::CompactionDriver;
pub(crate) use handle::CompactionHandle;
pub(crate) use minor::MinorCompactor;
