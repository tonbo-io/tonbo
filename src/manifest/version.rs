use super::domain::{SstEntry, WalSegmentRef};
use crate::ondisk::sstable::SsTableId;

/// Manifest edits applied sequentially to produce a new version.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum VersionEdit {
    /// Add SST entries to the specified level.
    AddSsts {
        /// Compaction level receiving the new SSTs.
        level: u32,
        /// SST descriptors to attach to the level.
        entries: Vec<SstEntry>,
    },
    /// Remove SST entries (by id) from the specified level.
    RemoveSsts {
        /// Level whose SST set should be pruned.
        level: u32,
        /// Identifiers of SSTs that must be removed.
        sst_ids: Vec<SsTableId>,
    },
    /// Replace the WAL segments referenced by the version.
    SetWalSegment {
        /// Complete set of WAL fragments backing the version.
        segment: WalSegmentRef,
    },
    /// Update the tombstone watermark.
    SetTombstoneWatermark {
        /// Upper MVCC bound for tombstones visible in the version.
        watermark: u64,
    },
}
