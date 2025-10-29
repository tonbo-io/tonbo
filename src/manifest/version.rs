use std::collections::HashSet;

use super::{ManifestError, ManifestResult, SstEntry, VersionState, WalSegmentRef};
use crate::ondisk::sstable::SsTableId;

/// Manifest edits applied sequentially to produce a new version.
#[derive(Debug, Clone)]
pub enum VersionEdit {
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
    SetWalSegments {
        /// Complete set of WAL fragments backing the version.
        segments: Vec<WalSegmentRef>,
    },
    /// Update the tombstone watermark.
    SetTombstoneWatermark {
        /// Upper MVCC bound for tombstones visible in the version.
        watermark: u64,
    },
}

pub(super) fn apply_edits(state: &mut VersionState, edits: &[VersionEdit]) -> ManifestResult<()> {
    for edit in edits {
        match edit {
            VersionEdit::AddSsts { level, entries } => {
                apply_add_ssts(&mut state.ssts, *level, entries)?
            }
            VersionEdit::RemoveSsts { level, sst_ids } => {
                apply_remove_ssts(&mut state.ssts, *level, sst_ids)?
            }
            VersionEdit::SetWalSegments { segments } => {
                state.wal_segments = segments.clone();
            }
            VersionEdit::SetTombstoneWatermark { watermark } => {
                state.tombstone_watermark = Some(*watermark);
            }
        }
    }

    for bucket in &mut state.ssts {
        bucket.sort_by_key(|entry| entry.sst_id.raw());
    }
    trim_trailing_empty_levels(&mut state.ssts);

    Ok(())
}

fn apply_add_ssts(
    levels: &mut Vec<Vec<SstEntry>>,
    level: u32,
    entries: &[SstEntry],
) -> ManifestResult<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let bucket = ensure_level(levels, level);
    for entry in entries {
        bucket.retain(|existing| existing.sst_id != entry.sst_id);
        bucket.push(entry.clone());
    }
    Ok(())
}

fn apply_remove_ssts(
    levels: &mut [Vec<SstEntry>],
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
    bucket.retain(|entry| !targets.contains(&entry.sst_id));
    Ok(())
}

fn ensure_level(levels: &mut Vec<Vec<SstEntry>>, level: u32) -> &mut Vec<SstEntry> {
    let index = level as usize;
    if levels.len() <= index {
        levels.resize_with(index + 1, Vec::new);
    }
    &mut levels[index]
}

fn trim_trailing_empty_levels(levels: &mut Vec<Vec<SstEntry>>) {
    while levels.last().is_some_and(|entries| entries.is_empty()) {
        levels.pop();
    }
}
