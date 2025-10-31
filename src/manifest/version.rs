use std::collections::HashSet;

use super::{
    ManifestError, ManifestResult,
    domain::{SstEntry, WalSegmentRef},
};
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

pub(super) fn apply_add_ssts(
    levels: &mut Vec<Vec<SstEntry>>,
    level: u32,
    entries: &[SstEntry],
) -> ManifestResult<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let bucket = ensure_level(levels, level);
    for entry in entries {
        bucket.retain(|existing| existing.sst_id() != entry.sst_id());
        bucket.push(entry.clone());
    }
    Ok(())
}

pub(super) fn apply_remove_ssts(
    levels: &mut Vec<Vec<SstEntry>>,
    level: u32,
    sst_ids: &[SsTableId],
) -> ManifestResult<()> {
    if sst_ids.is_empty() {
        return Ok(());
    }
    let index = level as usize;
    if index >= levels.len() {
        return Err(ManifestError::Invariant(
            "attempted to remove SSTs from a missing level",
        ));
    }
    let bucket = &mut levels[index];
    let targets: HashSet<SsTableId> = sst_ids.iter().cloned().collect();
    bucket.retain(|entry| !targets.contains(entry.sst_id()));
    Ok(())
}

fn ensure_level(levels: &mut Vec<Vec<SstEntry>>, level: u32) -> &mut Vec<SstEntry> {
    let index = level as usize;
    if levels.len() <= index {
        levels.resize_with(index + 1, Vec::new);
    }
    &mut levels[index]
}

pub(super) fn trim_trailing_empty_levels(levels: &mut Vec<Vec<SstEntry>>) {
    while levels.last().is_some_and(|entries| entries.is_empty()) {
        levels.pop();
    }
}
