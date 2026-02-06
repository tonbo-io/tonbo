//! Read-only immutable memtables.
//!
//! Currently only the dynamic runtime-schema layout is wired through.

pub(crate) mod memtable;

/// Immutable segment emitted by sealing the dynamic mutable memtable.
pub(crate) type ImmutableSegment = memtable::ImmutableMemTable;

/// Lightweight pruning helper; currently returns all segment indexes.
pub(crate) fn prune_segments(
    segments: &[&ImmutableSegment],
    key_bounds: Option<&crate::query::scan::KeyBounds>,
    read_ts: crate::mvcc::Timestamp,
) -> Vec<usize> {
    segments
        .iter()
        .enumerate()
        .filter_map(|(idx, segment)| {
            if segment.entry_count() == 0 {
                return None;
            }
            if let Some(bounds) = key_bounds {
                let (Some(min_key), Some(max_key)) = (segment.min_key(), segment.max_key()) else {
                    return Some(idx);
                };
                if !bounds.overlaps(&min_key, &max_key) {
                    return None;
                }
            }
            if let Some((min_commit_ts, _)) = segment.commit_ts_bounds()
                && min_commit_ts > read_ts
            {
                return None;
            }
            Some(idx)
        })
        .collect()
}
