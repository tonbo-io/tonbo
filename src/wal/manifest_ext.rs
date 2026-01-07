//! Helpers that bridge WAL metadata into manifest structures.

use std::sync::Arc;

use crate::{
    logging::{LogContext, tonbo_log},
    manifest::WalSegmentRef,
    wal::{
        WalConfig, WalError,
        storage::{SegmentDescriptor, SegmentFrameBounds, WalStorage},
        wal_segment_file_id,
    },
};

const WAL_LOG_CTX: LogContext = LogContext::new("component=wal");

/// Collect WAL segment references using the configuration supplied to the writer.
pub(crate) async fn collect_wal_segment_refs(
    cfg: &WalConfig,
    manifest_floor: Option<&WalSegmentRef>,
    live_frame_floor: Option<u64>,
) -> Result<Vec<WalSegmentRef>, WalError> {
    let storage = WalStorage::new(Arc::clone(&cfg.segment_backend), cfg.dir.clone());
    let wal_state_hint = storage
        .load_state_handle(cfg.state_store.as_ref())
        .await?
        .and_then(|handle| handle.state().last_segment_seq);

    let Some(tail) = storage.tail_metadata_with_hint(wal_state_hint).await? else {
        return Ok(Vec::new());
    };

    let mut refs = Vec::with_capacity(2);
    let manifest_cutoff = manifest_floor.map(|f| f.seq());
    for descriptor in tail
        .completed
        .iter()
        .filter(|descriptor| descriptor.bytes > 0)
    {
        let bounds = storage
            .segment_frame_bounds(&descriptor.path)
            .await?
            .ok_or({
                WalError::Corrupt("wal segment contained no frames despite non-zero length")
            })?;
        if segment_required(descriptor.seq, &bounds, manifest_cutoff, live_frame_floor) {
            refs.push(wal_segment_ref_from_descriptor(descriptor, bounds));
        }
    }

    if tail.active.bytes > 0 {
        let bounds = storage
            .segment_frame_bounds(&tail.active.path)
            .await?
            .ok_or({
                WalError::Corrupt("active wal segment contained no frames despite non-zero length")
            })?;
        if segment_required(tail.active.seq, &bounds, manifest_cutoff, live_frame_floor) {
            refs.push(wal_segment_ref_from_descriptor(&tail.active, bounds));
        }
    }

    // Note: We intentionally don't re-add the old floor when refs is empty.
    // If no segments are required (all data persisted), the floor should be cleared.
    // This ensures WAL replay doesn't replay already-persisted data.

    refs.sort_by_key(|segment| segment.seq());
    refs.dedup_by_key(|segment| segment.seq());

    Ok(refs)
}

fn wal_segment_ref_from_descriptor(
    descriptor: &SegmentDescriptor,
    bounds: SegmentFrameBounds,
) -> WalSegmentRef {
    let file_id = wal_segment_file_id(descriptor.seq);
    WalSegmentRef::new(descriptor.seq, file_id, bounds.first_seq, bounds.last_seq)
}

fn segment_required(
    seq: u64,
    bounds: &SegmentFrameBounds,
    manifest_cutoff: Option<u64>,
    live_frame_floor: Option<u64>,
) -> bool {
    if let Some(live_floor) = live_frame_floor
        && bounds.last_seq >= live_floor
    {
        return true;
    }

    match manifest_cutoff {
        Some(cutoff) => seq > cutoff,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_floor_filters_when_unpinned() {
        let bounds = SegmentFrameBounds {
            first_seq: 10,
            last_seq: 20,
        };
        assert!(!segment_required(5, &bounds, Some(5), None));
        assert!(segment_required(6, &bounds, Some(5), None));
    }

    #[test]
    fn live_floor_keeps_older_segments() {
        let bounds = SegmentFrameBounds {
            first_seq: 30,
            last_seq: 40,
        };
        assert!(segment_required(3, &bounds, Some(5), Some(35)));
        assert!(!segment_required(3, &bounds, Some(5), Some(45)));
    }
}

/// Prune WAL segments whose sequence is strictly below the manifest floor.
pub(crate) async fn prune_wal_segments(
    cfg: &WalConfig,
    floor: &WalSegmentRef,
) -> Result<usize, WalError> {
    let storage = WalStorage::new(Arc::clone(&cfg.segment_backend), cfg.dir.clone());
    if cfg.prune_dry_run {
        let segments = storage.list_segments_with_hint(None).await?;
        let removable = segments
            .into_iter()
            .filter(|descriptor| descriptor.seq < floor.seq())
            .count();
        tonbo_log!(
            log::Level::Info,
            ctx: WAL_LOG_CTX,
            "wal_prune_dry_run",
            "floor_seq={} removable_segments={}",
            floor.seq(),
            removable,
        );
        Ok(removable)
    } else {
        let removed = storage.prune_below(floor.seq()).await?;
        tonbo_log!(
            log::Level::Info,
            ctx: WAL_LOG_CTX,
            "wal_prune_completed",
            "floor_seq={} removed_segments={}",
            floor.seq(),
            removed,
        );
        Ok(removed)
    }
}
