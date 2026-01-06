//! Pure orchestration functions for compaction.
//!
//! These functions operate on version state and compaction outcomes without requiring
//! access to the database or manifest. They are stateless and independently testable.

use std::collections::HashSet;

use crate::{
    compaction::{
        executor::{CompactionError, CompactionOutcome},
        planner::{CompactionInput, CompactionPlanner, CompactionSnapshot, CompactionTask},
    },
    id::FileId,
    manifest::{GcPlanState, GcSstRef, ManifestError, VersionState, WalSegmentRef},
    ondisk::sstable::{SsTableDescriptor, SsTableId},
};

/// Plan compaction from a version snapshot.
///
/// Returns `None` if the snapshot is empty or the planner finds no work to do.
pub(crate) fn plan_from_version<P>(planner: &P, version: &VersionState) -> Option<CompactionTask>
where
    P: CompactionPlanner,
{
    let layout: CompactionSnapshot = version.into();
    if layout.is_empty() {
        return None;
    }
    planner.plan(&layout)
}

/// Plan compaction from a version snapshot, starting from a minimum source level.
///
/// Returns `None` if the snapshot is empty or the planner finds no work to do.
pub(crate) fn plan_from_version_with_min_level<P>(
    planner: &P,
    version: &VersionState,
    min_level: usize,
) -> Option<CompactionTask>
where
    P: CompactionPlanner,
{
    let layout: CompactionSnapshot = version.into();
    if layout.is_empty() {
        return None;
    }
    planner.plan_with_min_level(&layout, min_level)
}

/// Resolve SST descriptors for a planned compaction task.
///
/// Looks up each input SST in the version state and builds full descriptors
/// with stats, WAL IDs, and storage paths.
pub(crate) fn resolve_inputs(
    version: &VersionState,
    task: &CompactionTask,
) -> Result<Vec<SsTableDescriptor>, CompactionError> {
    let mut descriptors = Vec::with_capacity(task.input.len());
    for CompactionInput { level, sst_id } in &task.input {
        let Some(bucket) = version.ssts().get(*level) else {
            return Err(CompactionError::Manifest(ManifestError::Invariant(
                "planner selected level missing in manifest",
            )));
        };

        let Some(entry) = bucket.iter().find(|entry| entry.sst_id() == sst_id) else {
            return Err(CompactionError::Manifest(ManifestError::Invariant(
                "planner selected SST missing in manifest",
            )));
        };
        let mut descriptor = SsTableDescriptor::new(entry.sst_id().clone(), *level);
        if let Some(stats) = entry.stats().cloned() {
            descriptor = descriptor.with_stats(stats);
        }
        descriptor = descriptor.with_wal_ids(entry.wal_segments().map(|ids| ids.to_vec()));
        descriptor =
            descriptor.with_storage_paths(entry.data_path().clone(), entry.delete_path().cloned());
        descriptors.push(descriptor);
    }
    Ok(descriptors)
}

/// Collect WAL file IDs referenced by remaining SSTs after compaction.
///
/// Returns `None` if any SST lacks WAL metadata, signaling that the existing
/// manifest WAL set should be preserved.
pub(crate) fn wal_ids_for_remaining_ssts(
    version: &VersionState,
    removed: &HashSet<SsTableId>,
    added: &[SsTableDescriptor],
) -> Option<HashSet<FileId>> {
    let mut wal_ids = HashSet::new();
    for bucket in version.ssts() {
        for entry in bucket {
            if removed.contains(entry.sst_id()) {
                continue;
            }
            let Some(ids) = entry.wal_segments() else {
                // If any remaining SST lacks WAL metadata, keep the existing manifest set.
                return None;
            };
            wal_ids.extend(ids.iter().cloned());
        }
    }
    for desc in added {
        let Some(ids) = desc.wal_ids() else {
            // Missing WAL metadata on new outputs: preserve manifest set.
            return None;
        };
        wal_ids.extend(ids.iter().cloned());
    }
    Some(wal_ids)
}

/// Compute the WAL segment list after compaction.
///
/// Filters the existing WAL segments to only those still referenced by
/// remaining SSTs. Handles sequence gaps conservatively.
pub(crate) fn wal_segments_after_compaction(
    version: &VersionState,
    removed_ssts: &[SsTableDescriptor],
    added_ssts: &[SsTableDescriptor],
) -> Option<Vec<WalSegmentRef>> {
    let removed_ids: HashSet<SsTableId> = removed_ssts.iter().map(|d| d.id().clone()).collect();
    let wal_ids = wal_ids_for_remaining_ssts(version, &removed_ids, added_ssts)?;
    let existing = version.wal_segments();
    if existing.is_empty() {
        return None;
    }
    let mut filtered: Vec<WalSegmentRef> = existing
        .iter()
        .filter(|seg| wal_ids.contains(seg.file_id()))
        .cloned()
        .collect();
    if filtered == existing {
        return None;
    }
    if filtered.is_empty() {
        return Some(Vec::new());
    }
    let has_seq_gaps = existing
        .windows(2)
        .any(|pair| pair[1].seq() > pair[0].seq().saturating_add(1));
    if has_seq_gaps {
        if let Some(first_retained) = filtered.first()
            && let Some(first_idx) = existing
                .iter()
                .position(|seg| seg.file_id() == first_retained.file_id())
            && first_idx > 0
        {
            let mut with_gap = existing[..first_idx].to_vec();
            with_gap.append(&mut filtered);
            Some(with_gap)
        } else {
            Some(existing.to_vec())
        }
    } else {
        Some(filtered)
    }
}

/// Reconcile WAL segments in the compaction outcome.
///
/// Updates the outcome with the final WAL segment list, floor, and obsolete segments.
pub(crate) fn reconcile_wal_segments(
    version: &VersionState,
    outcome: &mut CompactionOutcome,
    existing_wal_segments: &[WalSegmentRef],
    wal_floor: Option<WalSegmentRef>,
) {
    let wal_from_helper =
        wal_segments_after_compaction(version, &outcome.remove_ssts, &outcome.outputs);
    let (final_wal_segments, obsolete_wal_segments) = match wal_from_helper {
        Some(filtered) => {
            outcome.wal_segments = Some(filtered.clone());
            let obsolete = existing_wal_segments
                .iter()
                .filter(|seg| !filtered.iter().any(|s| s == *seg))
                .cloned()
                .collect();
            (filtered, obsolete)
        }
        None => {
            outcome.wal_segments = Some(existing_wal_segments.to_vec());
            (existing_wal_segments.to_vec(), Vec::new())
        }
    };
    outcome.wal_floor = if final_wal_segments.is_empty() {
        wal_floor
    } else {
        wal_floor.or_else(|| final_wal_segments.first().cloned())
    };
    if outcome.wal_segments.is_none() && !final_wal_segments.is_empty() {
        outcome.wal_segments = Some(final_wal_segments.clone());
    }
    outcome.obsolete_wal_segments = obsolete_wal_segments;
}

/// Build a GC plan from a compaction outcome.
///
/// Returns `None` if there are no obsolete SSTs or WAL segments to clean up.
pub(crate) fn gc_plan_from_outcome(
    outcome: &CompactionOutcome,
) -> Result<Option<GcPlanState>, CompactionError> {
    if outcome.remove_ssts.is_empty() && outcome.obsolete_wal_segments.is_empty() {
        return Ok(None);
    }
    let mut obsolete_ssts = Vec::new();
    for desc in &outcome.remove_ssts {
        let data_path = desc
            .data_path()
            .cloned()
            .ok_or(CompactionError::MissingPath("data"))?;
        obsolete_ssts.push(GcSstRef {
            id: desc.id().clone(),
            level: desc.level() as u32,
            data_path,
            delete_path: desc.delete_path().cloned(),
        });
    }
    Ok(Some(GcPlanState {
        obsolete_ssts,
        obsolete_wal_segments: outcome.obsolete_wal_segments.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compaction::planner::CompactionSnapshot, id::FileIdGenerator, manifest::TableId};

    struct AlwaysCompactPlanner;

    impl CompactionPlanner for AlwaysCompactPlanner {
        fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
            let level0 = snapshot.level(0)?;
            if level0.is_empty() {
                return None;
            }
            let input = level0
                .files()
                .iter()
                .map(|f| CompactionInput {
                    level: 0,
                    sst_id: f.sst_id.clone(),
                })
                .collect();
            Some(CompactionTask {
                source_level: 0,
                target_level: 1,
                input,
                key_range: None,
            })
        }
    }

    #[test]
    fn plan_from_version_empty_returns_none() {
        let generator = FileIdGenerator::default();
        let table_id = TableId::new(&generator);
        let version = VersionState::empty(table_id);
        let planner = AlwaysCompactPlanner;
        assert!(plan_from_version(&planner, &version).is_none());
    }

    #[test]
    fn gc_plan_from_empty_outcome_returns_none() {
        let outcome = CompactionOutcome {
            add_ssts: Vec::new(),
            remove_ssts: Vec::new(),
            target_level: 0,
            wal_segments: None,
            tombstone_watermark: None,
            outputs: Vec::new(),
            obsolete_sst_ids: Vec::new(),
            wal_floor: None,
            obsolete_wal_segments: Vec::new(),
        };
        assert!(gc_plan_from_outcome(&outcome).unwrap().is_none());
    }
}
